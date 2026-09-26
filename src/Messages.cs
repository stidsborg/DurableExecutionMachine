namespace DurableExecutionMachine;

public class Messages
{
    private readonly List<object> _inbox = new();
    private readonly Dictionary<int, Subscription> _subscriptions = new();
    private int _nextSubscriptionId = 0;
    
    private readonly Lock _lock = new();
    private readonly AsyncGate _gate;

    //todo add idempotency keys - private readonly 
    
    //deliver...

    public Messages(AsyncGate gate, States states)
    {
        _gate = gate;
    }

    public Task<object?> Subscribe(
        Func<object, bool> filter, 
        ExecutionScopeId messageId, 
        ExecutionScopeId messageTypeId, 
        ExecutionScopeId timeoutId, 
        DateTime? timeout)
    {
        var tcs = new TaskCompletionSource<object?>();
        
        lock (_lock)
        {
            var subscriptionId = _nextSubscriptionId++;
            var subscription = new Subscription(subscriptionId, filter, tcs, messageId, messageTypeId, timeoutId, timeout);
            _subscriptions[subscriptionId] = subscription;
        }
        
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
        int SubscriptionId,
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId MessageTypeId,
        ExecutionScopeId TimeoutId,
        DateTime? Timeout
    );
}