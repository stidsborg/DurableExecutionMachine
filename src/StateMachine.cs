namespace DurableExecutionMachine;

public class StateMachine<TFlow, TParam, TResult>(
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    States states,
    TimeoutsManager timeoutsManager,
    SyncBehavior syncBehavior = SyncBehavior.Immediate
) 
{
    private Dictionary<int, byte[]> _state;
    private TFlow _flow;
    private AsyncSignal _syncSignal = new AsyncSignal();
    private AsyncSignal _notifySignal = new AsyncSignal();
    private Task<TResult> _flowTask;
    
    internal Context Context { get; } = CreateContext(states);

    private static Context CreateContext(States states)
    {
        var gate = new AsyncGate();
        var messages = new Messages(gate, states);
        return new Context(new ExecutionScope(), gate, states, messages);
    }
    
    //can this work with multiple threads?
    public bool IsCompleted => _flowTask.IsCompleted;
    public TResult GetResult() => _flowTask.Result;
    
    public void Start(TFlow flow, TParam param)
    {
        _flowTask = startFlow(flow, param, Context);
    }

    public async Task Sync()
    {
        await Task.WhenAny(_syncSignal.Wait(), _flowTask);
    }
    
    public async Task DeliverMessage()
    {
        //can we allow delivery of message while still running?
    }
    
    public SerializedStateMachine Serialize()
    {
        throw new NotImplementedException();
    }

    public void SetState(string id, byte[]  bytes)
    {
        
    }
    
    public void RemoveState(string id)
    {
    }
}