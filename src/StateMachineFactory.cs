namespace DurableExecutionMachine;

public class StateMachineFactory<TFlow, TParam, TResult>( 
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    Func<Task> syncCallback,
    SyncBehavior syncBehavior = SyncBehavior.Immediate)
{
    public StateMachine<TFlow, TParam, TResult> New()
    {
        var runningAndWaiting = new RunningAndWaiting();
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, syncCallback, new States(runningAndWaiting), runningAndWaiting, new TimeoutsManager(), syncBehavior);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var runningAndWaiting = new RunningAndWaiting();
        var states = States.Deserialize(bytes, runningAndWaiting);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, syncCallback, states, runningAndWaiting, new TimeoutsManager(), syncBehavior
        );
    }
}