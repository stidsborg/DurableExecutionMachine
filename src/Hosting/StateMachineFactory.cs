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
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, syncCallback, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }
    
    public StateMachine<TFlow, TParam, TResult> New(Func<TFlow, TParam, Context, Task<TResult>> startFlow, SyncCallback syncCallback)
    {
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, syncCallback, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        var states = States.Deserialize(bytes, suspensionBarrier, syncCallback, timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, states, suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow
        );
    }
}