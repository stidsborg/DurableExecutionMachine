namespace DurableExecutionMachine;

public class StateMachine<TFlow, TParam, TResult>(Func<TFlow, TParam, Context, Task<TResult>> startFlow) 
{
    private Dictionary<int, byte[]> _state;
    private TFlow _flow;
    private AsyncSignal _syncSignal = new AsyncSignal();
    private AsyncSignal _notifySignal = new AsyncSignal();
    private Task<TResult> _flowTask;

    private int _subflows;
    private int _waitingSubflows;
    
    //can this work with multiple threads?
    public bool IsCompleted => _flowTask.IsCompleted;
    public TResult GetResult() => _flowTask.Result;
    
    public void Start(TFlow flow, Context ctx, TParam param)
    {
        _flowTask = startFlow(flow, param, ctx);
    }

    public async Task Sync()
    {
        await Task.WhenAny(_syncSignal.Wait(), _flowTask);
    }
    
    public async Task DeliverMessage()
    {
        //can we allow delivery of message while still running?
    }

    public async Task RegisterTimeout(string id)
    {
        
    }

    public async Task CancelTimeout(string id)
    {
        
    }

    public byte[] Serialize()
    {
        throw new NotImplementedException();
    }
    
    public void Deserialize(byte[] bytes)
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