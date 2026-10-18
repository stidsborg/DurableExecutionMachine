namespace DurableExecutionMachine;

public class StateMachine<TFlow, TParam, TResult>(
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    SyncCallback syncCallback,
    States states,
    RunningAndWaiting runningAndWaiting,
    TimeoutsManager timeoutsManager,
    UtcNow utcNow
)
{
    private Dictionary<int, byte[]> _state;
    private TFlow _flow;
    private Task<TResult> _flowTask;

    internal Context Context { get; } = CreateContext(states, runningAndWaiting, timeoutsManager, utcNow);

    private static Context CreateContext(States states, RunningAndWaiting runningAndWaiting, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        var messages = new Messages(states, timeoutsManager, utcNow);
        messages.Initialize();
        return new Context(new ExecutionScope(), states, messages, runningAndWaiting, timeoutsManager);
    }
    
    //can this work with multiple threads?
    public bool IsCompleted => _flowTask.IsCompleted;
    public TResult GetResult() => _flowTask.Result;
    
    public void Start(TFlow flow, TParam param)
    {
        runningAndWaiting.SubflowStarted();
        _flowTask = RunFlow();

        async Task<TResult> RunFlow()
        {
            Context.Scope.SetRoot();
            try
            {
                return await startFlow(flow, param, Context);
            }
            finally
            {
                runningAndWaiting.SubflowCompleted();
            }
        }
    }

    public async Task Sync()
    {
        runningAndWaiting.Suspend();
        try
        {
            await runningAndWaiting.WaitForAllSuspended();
            var (bytes, minimumTimeout) = states.Serialize();
            await syncCallback(bytes, minimumTimeout);
        }
        finally
        {
            runningAndWaiting.Resume();
        }
    }

    public async Task Suspend()
    {
        
    }

    public void Resume()
    {
        
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