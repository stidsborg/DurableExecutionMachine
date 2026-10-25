namespace DurableExecutionMachine.Hosting;

public record SerializedStateMachine(byte[] States, DateTime? MinimumTimeout);