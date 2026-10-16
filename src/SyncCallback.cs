namespace DurableExecutionMachine;

public delegate Task SyncCallback(byte[] state, DateTime? minTimeout);