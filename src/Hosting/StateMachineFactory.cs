using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Hosting;

public class StateMachineFactory<TFlow, TParam, TResult>(
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    SyncCallback syncCallback) where TParam : notnull
{
    public StateMachine<TFlow, TParam, TResult> New()
    {
        var suspensionBarrier = new SuspensionBarrier();
        var timeoutsManager = new TimeoutsManager();
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, syncCallback, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }
    
    public StateMachine<TFlow, TParam, TResult> New(Func<TFlow, TParam, Context, Task<TResult>> startFlow, SyncCallback syncCallback)
    {
        var suspensionBarrier = new SuspensionBarrier();
        var timeoutsManager = new TimeoutsManager();
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, syncCallback, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var suspensionBarrier = new SuspensionBarrier();
        var timeoutsManager = new TimeoutsManager();
        var states = States.Deserialize(bytes, suspensionBarrier, syncCallback, timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, states, suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow
        );
    }
}