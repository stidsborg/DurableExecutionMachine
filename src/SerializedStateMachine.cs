namespace DurableExecutionMachine;

public record SerializedStateMachine(byte[] States, DateTime? MinimumTimeout);