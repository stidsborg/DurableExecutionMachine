namespace DurableExecutionMachine;

public class StateMachineFactory<TFlow, TParam, TResult>(
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    SyncCallback syncCallback)
{
    public StateMachine<TFlow, TParam, TResult> New()
    {
        var runningAndWaiting = new RunningAndWaiting();
        var timeoutsManager = new TimeoutsManager();
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, syncCallback, new States(runningAndWaiting, syncCallback, timeoutsManager), runningAndWaiting, timeoutsManager, utcNow: () => DateTime.UtcNow);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var runningAndWaiting = new RunningAndWaiting();
        var timeoutsManager = new TimeoutsManager();
        var states = States.Deserialize(bytes, runningAndWaiting, syncCallback, timeoutsManager);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, syncCallback, states, runningAndWaiting, timeoutsManager, utcNow: () => DateTime.UtcNow
        );
    }
}