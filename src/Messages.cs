namespace DurableExecutionMachine;

public class Messages
{
    private readonly States _states;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly UtcNow _utcNow;
    private readonly SortedDictionary<int, object> _inbox = new();
    private readonly Dictionary<ExecutionScopeId, Subscription> _subscriptions = new();

    private readonly ExecutionScopeId _rootExecutionScope = new("-1");
    private readonly ExecutionScopeId _messagesExecutionScope;
    private readonly ExecutionScopeId _idempotencyScope;
    private readonly ExecutionScopeId _nextMessageIdScope;
    
    private readonly Dictionary<string, long> _idempotencyKeys = new();
    private int _nextMessageId;
    private readonly Lock _lock = new();
    
    public Messages(States states, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
        _utcNow = utcNow;
        
        _messagesExecutionScope = _rootExecutionScope.CreateChild(0);
        _idempotencyScope = _rootExecutionScope.CreateChild(1);
        _nextMessageIdScope = _rootExecutionScope.CreateChild(2);
        
        var nextMessageId = _states
            .Deserialize(_nextMessageIdScope, out _)
            as int?;
        _nextMessageId = nextMessageId ?? 0;
    }

    public void Initialize()
    {
        lock (_lock)
        {
            // messages still in the messages scope were not delivered to a subscription before the previous run ended
            foreach (var messageId in _states.GetDescendants(_messagesExecutionScope))
            {
                var message = _states.Deserialize(messageId, out _)!;
                _inbox[(int) messageId.ChildId] = message;
            }
            
            foreach (var idempotencyValueId in _states.GetDescendants(_idempotencyScope).Where(id => id.ChildId == 0))
            {
                var idempotencyKey = (string) _states.Deserialize(idempotencyValueId, out _)!;
                var createdAtId = idempotencyValueId.Parent.CreateChild(1);
                var createdAt = (long) _states.Deserialize(createdAtId, out _)!;
                _idempotencyKeys[idempotencyKey] = createdAt;
            }
        }
    }
    
    public async Task<object?> Subscribe(
        Func<object, bool> filter, 
        ExecutionScopeId messageId, 
        ExecutionScopeId timeoutId, 
        DateTime? timeout)
    {
        var message = _states.Deserialize(messageId, out var messageExists);
        if (messageExists)
            return message;
        
        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
        
        var existingTimeout = (long?) _states.Deserialize(timeoutId, out var timeoutExists);
        DateTime? timeoutAt = timeoutExists
            ? new DateTime(existingTimeout!.Value, DateTimeKind.Utc)
            : timeout?.ToUniversalTime();
        
        // the subscription, the delivery attempt and the timeout registration are atomic,
        // so a concurrent delivery cannot complete the subscription before its timeout is registered
        lock (_lock)
        {
            _subscriptions[messageId] = new Subscription(filter, tcs, messageId, timeoutId);
            
            // deliver before counting as suspended so the subscription's writes to states happen before a sync can observe all subflows suspended
            TryToDeliver();
            
            if (!tcs.Task.IsCompleted && timeoutAt is not null)
            {
                if (!timeoutExists)
                    _states.TryAddState(timeoutId, timeoutAt.Value.Ticks, typeof(long), removeDescendants: false);

                _timeoutsManager.RegisterTimeout(timeoutId, timeoutAt.Value, () =>
                {
                    lock (_lock)
                    {
                        // the subscription is removed when a message is delivered to it
                        if (!_subscriptions.Remove(messageId))
                            return;

                        _states.SetNull(messageId, removeDescendants: false);
                        tcs.SetResult(null);
                    }
                });
            }
        }
        
        _states.RunningAndWaiting.SubflowSuspended();
        var result = await tcs.Task;
        await _states.RunningAndWaiting.ResumeSubflow();
        return result;
    }

    private void TryToDeliver()
    {
        lock (_lock)
        {
            foreach (var (number, message) in _inbox.ToList())
            foreach (var subscription in _subscriptions.Values.ToList())
            {
                // a throwing filter does not match the message, so the message stays in the inbox for other subscriptions
                if (!Safe.Try(() => subscription.Filter(message), onError: _ => false))
                    continue;

                // moving the message from the inbox to the subscription is atomic with respect to flushes.
                // the timeout cannot have resolved the subscription as it removes the subscription under the same lock
                _states.Lock(doWhileLocked: () =>
                {
                    _states.SetState(subscription.MessageId, message, message.GetType(), removeDescendants: false);
                    _states.Remove(_messagesExecutionScope.CreateChild(number));
                });

                _timeoutsManager.CancelTimeout(subscription.TimeoutId);
                _subscriptions.Remove(subscription.MessageId);
                _inbox.Remove(number);
                subscription.CompletionSource.SetResult(message);
                break;
            }
        }
    }

    public async Task Deliver(object message, string? idempotencyKey, TimeSpan? timeout)
    {
        // the delivery waits for a sync in progress, so nothing changes in states until the subflows are resumed,
        // and it counts as running while it writes, so a sync waits for it
        await _states.RunningAndWaiting.StartMessageDelivery();
        Task flushed;
        try
        {
            flushed = DeliverNow(message, idempotencyKey, timeout);
        }
        finally
        {
            _states.RunningAndWaiting.MessageDeliveryCompleted();
        }

        await flushed;
    }

    private Task DeliverNow(object message, string? idempotencyKey, TimeSpan? timeout)
    {
        Task nextFlush = Task.CompletedTask;
        lock (_lock)
            _states.Lock(doWhileLocked: () =>
            {
                nextFlush = _states.WaitForNextFlush();

                // a duplicate waits for the next flush, as the original delivery might not have been flushed yet
                if (idempotencyKey != null && _idempotencyKeys.ContainsKey(idempotencyKey))
                    return;

                var number = _nextMessageId++;
                _states.SetState(_nextMessageIdScope, _nextMessageId, typeof(int), removeDescendants: false);

                if (idempotencyKey is not null)
                {
                    var createdAt = _utcNow().Ticks;
                    _idempotencyKeys[idempotencyKey] = createdAt;
                    SaveIdempotencyKey(number, idempotencyKey, createdAt);
                }

                var messageId = _messagesExecutionScope.CreateChild(number);
                _states.SetState(messageId, message, message.GetType(), removeDescendants: false);
                _inbox[number] = message;
            });

        TryToDeliver();

        if (timeout is not null)
            _ = FlushIfNotFlushedWithin(timeout.Value, nextFlush);

        return nextFlush;
    }

    private async Task FlushIfNotFlushedWithin(TimeSpan timeout, Task flushed)
    {
        using var cts = new CancellationTokenSource();
        if (await Task.WhenAny(flushed, Task.Delay(timeout, cts.Token)) == flushed)
        {
            await cts.CancelAsync();
            return;
        }

        try
        {
            await _states.Flush();
        }
        catch
        {
            // the failure reaches the caller through the flushed task
        }
    }

    private void SaveIdempotencyKey(int number, string idempotencyKey, long createdAt)
    {
        // each key is stored under <idempotency scope>.<message number> with the key at .0 and its creation ticks at .1
        var idempotencyKeyId = _idempotencyScope.CreateChild(number);
        _states.SetState(idempotencyKeyId.CreateChild(0), instance: idempotencyKey, typeof(string), removeDescendants: false);
        _states.SetState(idempotencyKeyId.CreateChild(1), instance: createdAt, typeof(long), removeDescendants: false);
    }
    
    private record Subscription(
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId TimeoutId
    );
}