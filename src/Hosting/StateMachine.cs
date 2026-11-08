using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Hosting;

public class StateMachine<TFlow, TParam, TResult> where TParam : notnull
{
    private readonly Func<TFlow, TParam, Context, Task<TResult>> _startFlow;
    private readonly SyncCallback _syncCallback;
    private readonly States _states;
    private readonly RunningAndWaiting _runningAndWaiting;

    private Dictionary<int, byte[]> _state;
    private TFlow _flow;
    private Task<TResult> _flowTask;
    private readonly ExecutionScopeId _paramId = ExecutionScopeId.Root.CreateChild(-2);

    internal Context Context { get; }

    internal StateMachine(
        Func<TFlow, TParam, Context, Task<TResult>> startFlow,
        SyncCallback syncCallback,
        States states,
        RunningAndWaiting runningAndWaiting,
        TimeoutsManager timeoutsManager,
        UtcNow utcNow)
    {
        _startFlow = startFlow;
        _syncCallback = syncCallback;
        _states = states;
        _runningAndWaiting = runningAndWaiting;
        Context = CreateContext(states, runningAndWaiting, timeoutsManager, utcNow);
    }

    private static Context CreateContext(States states, RunningAndWaiting runningAndWaiting, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        var messages = new Messages(states, timeoutsManager, utcNow);
        messages.Initialize();
        return new Context(new ExecutionScope(), states, messages, runningAndWaiting, timeoutsManager, utcNow);
    }
    
    //can this work with multiple threads?
    public bool IsCompleted => _flowTask.IsCompleted;
    public TResult GetResult() => _flowTask.Result;
    
    public void Start(TFlow flow, TParam param)
    {
        ArgumentNullException.ThrowIfNull(param);

        _states.SetState(_paramId, param, typeof(TParam), removeDescendants: false);
        _runningAndWaiting.SubflowStarted();
        _flowTask = RunFlow(flow, param);
    }

    public async Task Sync()
    {
        _runningAndWaiting.Suspend();
        try
        {
            await _runningAndWaiting.WaitForAllSuspended();
            var (bytes, minimumTimeout) = _states.Serialize();
            await _syncCallback(bytes, minimumTimeout);
        }
        finally
        {
            _runningAndWaiting.Resume();
        }
    }

    public async Task Suspend()
    {
        
    }

    public void Resume(TFlow flow)
    {
        if (!_states.TryGet(_paramId, out TParam param))
            throw new InvalidOperationException("No stored parameter found - did you mean to call Start(flow, param) to begin a new execution?");

        _runningAndWaiting.SubflowStarted();
        _flowTask = RunFlow(flow, param);
    }
    
    public Task DeliverMessage(object message, string? idempotencyKey = null, TimeSpan? timeout = null)
        => Context.Messages.Deliver(message, idempotencyKey, timeout);
    
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
    
    private async Task<TResult> RunFlow(TFlow flow, TParam param)
    {
        Context.Scope.SetRoot();
        try
        {
            return await _startFlow(flow, param, Context);
        }
        finally
        {
            _runningAndWaiting.SubflowCompleted();
        }
    }
}