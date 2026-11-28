namespace DurableExecutionMachine.Hosting;

public enum StateMachineStatus
{
    // at least one subflow is running
    Running,
    // no subflow is running: every subflow is waiting, e.g. for a message, a delay or a suspension to be released
    Quiescent,
    // the flow has returned or thrown
    Completed
}
