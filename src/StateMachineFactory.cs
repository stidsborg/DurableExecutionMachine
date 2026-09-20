namespace DurableExecutionMachine;

public class StateMachineFactory<TFlow, TParam, TResult>( 
    Func<TFlow, TParam, Context, Task<TResult>> startFlow,
    SyncBehavior syncBehavior = SyncBehavior.Immediate)
{
    public StateMachine<TFlow, TParam, TResult> New()
    {
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, new States(), new TimeoutsManager(), syncBehavior);
    }

    public StateMachine<TFlow, TParam, TResult> Deserialize(byte[] bytes)
    {
        var states = States.Deserialize(bytes);
        return new StateMachine<TFlow, TParam, TResult>(
            startFlow, states,new TimeoutsManager(), syncBehavior
        );
    }
}