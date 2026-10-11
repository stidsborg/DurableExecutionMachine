namespace DurableExecutionMachine;

public class Messages
{
    private readonly States _states;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly List<object> _inbox = new();
    private readonly Dictionary<ExecutionScopeId, Subscription> _subscriptions = new();
    private int _nextSubscriptionId = 0;
    
    private readonly Lock _lock = new();

    //todo add idempotency keys - private readonly 
    
    //deliver...
    
    public Messages(States states, TimeoutsManager timeoutsManager)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
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

    public async Task Deliver(object message, int messageId, Func<int, Task>? deliveredCallback = null)
    {
        
    }
    
    private record Subscription(
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId TimeoutId
    );
}