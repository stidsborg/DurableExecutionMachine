using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Hosting;

public class StateMachineFactory<TFlow, TParam, TResult>(
    Func<TFlow, TParam, Context, Task<TResult>> startFlow) where TParam : notnull
{
    public StateMachine<TFlow, TParam, TResult> New()
    {
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }
    
    public StateMachine<TFlow, TParam, TResult> New(Func<TFlow, TParam, Context, Task<TResult>> startFlow)
    {
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(suspensionBarrier, timeoutsManager), suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var timeoutsManager = new TimeoutsManager();
        var suspensionBarrier = new SuspensionBarrier(timeoutsManager);
        var states = States.Deserialize(bytes, suspensionBarrier, timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, states, suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow
        );
    }
}