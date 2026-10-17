namespace DurableExecutionMachine;

public class Messages
{
    private readonly States _states;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly SortedDictionary<int, object> _inbox = new();
    private readonly Dictionary<ExecutionScopeId, Subscription> _subscriptions = new();

    private readonly ExecutionScopeId _rootExecutionScope = new("-1");
    private readonly ExecutionScopeId _messagesExecutionScope;
    private readonly ExecutionScopeId _idempotencyScope;
    
    private readonly Dictionary<string, long> _idempotencyKeys = new();
    private int _nextMessageId;
    private readonly Lock _lock = new();
    
    public Messages(States states, TimeoutsManager timeoutsManager)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
        
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
        }
    }
    
    public Task<object?> Subscribe(
        Func<object, bool> filter, 
        ExecutionScopeId messageId, 
        ExecutionScopeId timeoutId, 
        DateTime? timeout)
    {
        var message = _states.Deserialize(messageId, out var messageExists);
        if (messageExists)
            return Task.FromResult(message);
        
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
                _states.TryAddState(timeoutId, timeoutAt.Value.Ticks, typeof(long), removeChildren: false);

            _timeoutsManager.RegisterTimeout(timeoutId, timeoutAt.Value, () =>
            {
                lock (_lock)
                {
                    // the subscription is removed when a message is delivered to it
                    if (!_subscriptions.Remove(messageId))
                        return;

                    _states.SetNull(messageId, removeChildren: false);
                    tcs.SetResult(null);
                }
            });
        }

        _states.RunningAndWaiting.SubflowSuspended();
        TryToDeliver();
        return tcs.Task;
    }

    private void RegisterTimeout()
    {
        
    }

    private void TrySetTimeout()
    {
        
    }

    private void TryToDeliver()
    {
        lock (_lock)
        {
            foreach (var (number, message) in _inbox.ToList())
            foreach (var subscription in _subscriptions.Values.ToList())
            {
                if (!subscription.Filter(message))
                    continue;

                // moving the message from the inbox to the subscription is atomic with respect to flushes.
                // the timeout cannot have resolved the subscription as it removes the subscription under the same lock
                _states.Lock(doWhileLocked: () =>
                {
                    _states.SetState(subscription.MessageId, message, message.GetType(), removeChildren: false);
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
                _states.SetState(_rootExecutionScope, _nextMessageId, typeof(int), removeChildren: false);
                
                if (idempotencyKey is not null)
                {
                    _idempotencyKeys[idempotencyKey] = number;
                    
                    var idempotencyValueId = _idempotencyScope.CreateChild(number).CreateChild(0);
                    var idempotencyTimestampId = _idempotencyScope.CreateChild(number).CreateChild(1);
                    var created = DateTime.UtcNow.Ticks;
                    _states.SetState(idempotencyValueId, instance: idempotencyKey, typeof(string), removeChildren: false);
                    _states.SetState(idempotencyTimestampId, instance: created, typeof(long), removeChildren: false);
                }
                
                var messageId = _messagesExecutionScope.CreateChild(number);
                _states.SetState(messageId, message, message.GetType(), removeChildren: false);
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
    
    private record Subscription(
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId TimeoutId
    );
}