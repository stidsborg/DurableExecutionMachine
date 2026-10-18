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
    
    private readonly Dictionary<string, long> _idempotencyKeys = new();
    private int _nextMessageId;
    private readonly Lock _lock = new();
    
    public Messages(States states, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
        _utcNow = utcNow;
        
        var nextMessageId = _states
            .Deserialize(_rootExecutionScope, out _)
            as int?;
        _nextMessageId = nextMessageId ?? 0;
        
        _messagesExecutionScope = _rootExecutionScope.CreateChild(0);
        _idempotencyScope = _rootExecutionScope.CreateChild(1);
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
   
        lock (_lock)
            _subscriptions[messageId] = new Subscription(filter, tcs, messageId, timeoutId);
        
        var existingTimeout = (long?) _states.Deserialize(timeoutId, out var timeoutExists);
        DateTime? timeoutAt = timeoutExists
            ? new DateTime(existingTimeout!.Value, DateTimeKind.Utc)
            : timeout?.ToUniversalTime();
        
        if (timeoutAt is not null)
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

        // deliver before counting as suspended so the subscription's writes to states happen before a sync can observe all subflows suspended
        TryToDeliver();
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

    public Task Deliver(object message, string? idempotencyKey, TimeSpan? timeout)
    {
        Task nextSyncTask = Task.CompletedTask;
        lock (_lock)
        {
            if (idempotencyKey != null && _idempotencyKeys.ContainsKey(idempotencyKey))
                return Task.CompletedTask;
            
            _states.Lock(doWhileLocked: () =>
            {
                nextSyncTask = _states.NextFlush();
                
                var number = _nextMessageId++;
                _states.SetState(_rootExecutionScope, _nextMessageId, typeof(int), removeDescendants: false);
                
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
        }
        
        TryToDeliver();

        if (timeout is not null)
            _ = FlushIfNotFlushedWithin(timeout.Value, nextSyncTask);

        return nextSyncTask;
    }

    private async Task FlushIfNotFlushedWithin(TimeSpan timeout, Task flushed)
    {
        using var cts = new CancellationTokenSource();
        if (await Task.WhenAny(flushed, Task.Delay(timeout, cts.Token)) == flushed)
        {
            cts.Cancel();
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