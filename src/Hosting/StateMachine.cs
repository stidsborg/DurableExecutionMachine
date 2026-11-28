using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Hosting;

public class StateMachine<TFlow, TParam, TResult> where TParam : notnull
{
    private readonly Func<TFlow, TParam, Context, Task<TResult>> _startFlow;
    private readonly States _states;
    private readonly SuspensionBarrier _suspensionBarrier;
    
    private TFlow _flow;
    private Task<TResult> _flowTask;
    private readonly ExecutionScopeId _paramId = ExecutionScopeId.Root.CreateChild(-2);

    internal Context Context { get; }

    internal StateMachine(
        Func<TFlow, TParam, Context, Task<TResult>> startFlow,
        States states,
        SuspensionBarrier suspensionBarrier,
        TimeoutsManager timeoutsManager,
        UtcNow utcNow)
    {
        _startFlow = startFlow;
        _states = states;
        _suspensionBarrier = suspensionBarrier;
        Context = CreateContext(states, suspensionBarrier, timeoutsManager, utcNow);
    }

    private static Context CreateContext(States states, SuspensionBarrier suspensionBarrier, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        var messages = new Messages(states, timeoutsManager, utcNow);
        messages.Initialize();
        return new Context(new ExecutionScope(), states, messages, suspensionBarrier, utcNow);
    }
    
    //can this work with multiple threads?
    public bool IsCompleted => _flowTask.IsCompleted;
    public TResult Completion() => _flowTask.Result;

    /// <summary>
    /// <see cref="StateMachineStatus.Completed"/> once the flow has returned or thrown, otherwise <see cref="StateMachineStatus.Quiescent"/>
    /// when no subflow is running (see <see cref="Quiescence"/>), otherwise <see cref="StateMachineStatus.Running"/>.
    /// </summary>
    public StateMachineStatus Status
    {
        get
        {
            if (_flowTask is { IsCompleted: true })
                return StateMachineStatus.Completed;

            return _suspensionBarrier.AllSuspended ? StateMachineStatus.Quiescent : StateMachineStatus.Running;
        }
    }
    
    public void Start(TFlow flow, TParam param)
    {
        ArgumentNullException.ThrowIfNull(param);

        _states.SetState(_paramId, param, typeof(TParam), removeDescendants: false);
        _flowTask = RunFlow(flow, param);
    }

    public async Task Suspend()
    {
        _suspensionBarrier.RequestSuspension();
        await _suspensionBarrier.WaitForAllSuspended();
    }

    /// <summary>
    /// Completes when no subflow is running, i.e. every subflow is waiting or done. Unlike <see cref="Suspend"/>, nothing is held:
    /// a subflow that is woken afterwards runs again.
    /// </summary>
    public Task Quiescence() => _suspensionBarrier.WaitForAllSuspended();

    /// <summary>
    /// Completes when no subflow is running (see <see cref="Quiescence"/>) or a sync is requested that the last started sync doesn't cover.
    /// Completes at once if either already holds.
    /// The host calls it repeatedly: on a sync request it syncs (<see cref="StartSync"/>, persist, <see cref="CompleteSync"/>),
    /// and it stops once the status is not <see cref="StateMachineStatus.Running"/> and no sync is requested.
    /// A subflow waiting for a sync counts as running, so it holds back quiescence until the host syncs.
    /// The result holds <see cref="Status"/> and <see cref="IsSyncRequested"/> as read when it completes, so a subflow woken in the meantime
    /// may already have made the status <see cref="StateMachineStatus.Running"/>.
    /// </summary>
    public async Task<StatusAndSync> NextQuiescenceOrSync()
    {
        await Task.WhenAny(Quiescence(), _states.FlushRequested());
        return new StatusAndSync(Status, IsSyncRequested);
    }

    /// <summary>
    /// True while a sync is requested that the last started sync doesn't cover, i.e. when the sync half of <see cref="NextQuiescenceOrSync"/> holds.
    /// </summary>
    public bool IsSyncRequested => _states.IsFlushRequested;

    /// <summary>
    /// Starts a sync: returns the snapshot for the host to persist, which covers every sync requested so far.
    /// A flow that requested a sync (a <c>Capture</c> with <c>flush: true</c>) continues once the host calls <see cref="CompleteSync"/>.
    /// The host calls <see cref="StartSync"/> and <see cref="CompleteSync"/> alternately and never concurrently.
    /// </summary>
    public SerializedStateMachine StartSync() => _states.StartSync();

    /// <summary>
    /// The snapshot returned by the last <see cref="StartSync"/> is persisted: the subflows and deliveries waiting for it continue.
    /// </summary>
    public void CompleteSync() => _states.CompleteSync();

    public void ResumeAfterSuspend()
    {
        _suspensionBarrier.Release();
    }

    public void Resume(TFlow flow)
    {
        if (!_states.TryGet<TParam>(_paramId, out var param))
            throw new InvalidOperationException("No stored parameter found - did you mean to call Start(flow, param) to begin a new execution?");

        _flowTask = RunFlow(flow, param);
    }
    
    /// <summary>
    /// When it returns, the message is part of the machine's state, so every later <see cref="StartSync"/> and <see cref="Serialize"/> contains it,
    /// either in the inbox or at the wait it was handed to.
    /// The returned task completes when a sync that started after this call completes, so the caller may then release what the message
    /// depends on (e.g. ack it). The delivery does not request a sync: the caller may ignore the task and persist the state itself.
    /// It also delivers to a suspended machine: a waiting flow receives the message, but only continues when the machine is resumed.
    /// A message with an idempotency key that was already delivered is ignored, but its task still waits for the next sync,
    /// as the original delivery might not have been synced yet.
    /// </summary>
    public Task DeliverMessage(object message, string? idempotencyKey = null)
    {
        ArgumentNullException.ThrowIfNull(message);
        return Context.Messages.Deliver(message, idempotencyKey);
    }

    public SerializedStateMachine Serialize() => _states.Serialize();

    public void SetState(string id, byte[]  bytes)
    {
        
    }
    
    public void RemoveState(string id)
    {
    }
    
    private async Task<TResult> RunFlow(TFlow flow, TParam param)
    {
        _suspensionBarrier.Enter();
        Context.Scope.SetRoot();
        try
        {
            return await _startFlow(flow, param, Context);
        }
        finally
        {
            _suspensionBarrier.Leave();
        }
    }
}