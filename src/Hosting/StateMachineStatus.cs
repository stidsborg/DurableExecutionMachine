namespace DurableExecutionMachine.Hosting;

// AwaitingSync and Settled change only when a message is delivered, a timer expires or the host releases the machine
public enum StateMachineStatus
{
    NotStarted,
    // a branch is executing flow code, or was woken and will once the machine is not suspended
    Running,
    // no branch is running; at least one is parked on a flushed capture until the host has persisted
    AwaitingSync,
    // no branch is running; every branch waits on a message, a timer or a join
    Settled,
    Completed,
    Failed,
}
