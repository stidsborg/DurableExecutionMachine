namespace DurableExecutionMachine.Hosting;

public record StatusAndSync(StateMachineStatus Status, bool IsSyncRequested);
