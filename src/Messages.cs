namespace DurableExecutionMachine;

public class Messages
{
    private readonly States _states;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly Dictionary<ExecutionScopeId, object> _inbox = new();
    private readonly Dictionary<ExecutionScopeId, Subscription> _subscriptions = new();

    private readonly ExecutionScopeId _messagesExecutionScope;
    private readonly ExecutionScopeId _idempotencyScope;
    
    private readonly Dictionary<string, long> _idempotencyKeys = new();
    
    private readonly Lock _lock = new();
    
    public Messages(States states, TimeoutsManager timeoutsManager)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
        
        ExecutionScopeId rootExecutionScope = new("-1");
        _messagesExecutionScope = rootExecutionScope.CreateChild(0);
        _idempotencyScope = rootExecutionScope.CreateChild(1);
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
                if (_states.TryAddNull(messageId, removeChildren: false))
                {
                    lock (_lock)
                        _subscriptions.Remove(messageId);
                    
                    tcs.TrySetResult(null);
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
            
        }
    }

    //difficult but task needs to reflect what the deliver did
    public Task Deliver(object message, string? idempotencyKey) //todo after added to states let the caller know when that state has been persisted
    {
        lock (_lock)
        {
            if (idempotencyKey != null && _idempotencyKeys.ContainsKey(idempotencyKey))
                return Task.CompletedTask;
    
            _states.Lock(doWhileLocked: () =>
            {
                var randomId = Random.Shared.NextInt64();
                //consider seriializing before
                if (idempotencyKey != null)
                {
                    _idempotencyKeys[idempotencyKey] = randomId;
                    
                    var idempotencyValueId = _idempotencyScope.CreateChild(randomId).CreateChild(0);
                    var idempotencyTimestampId = _idempotencyScope.CreateChild(randomId).CreateChild(1);
                    var created = DateTime.UtcNow.Ticks;
                    _states.SetState(idempotencyValueId, instance: idempotencyKey, typeof(string), removeChildren: false);
                    _states.SetState(idempotencyTimestampId, instance: created, typeof(long), removeChildren: false);
                }
                
                var messageId = _messagesExecutionScope.CreateChild(randomId);
                _states.SetState(messageId, message, message.GetType(), removeChildren: false);
                _inbox[messageId] = message;
            });
        }
        
        TryToDeliver();
        //wait for sync to complete
        return Task.CompletedTask;
    }
    
    private record Subscription(
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId TimeoutId
    );
}