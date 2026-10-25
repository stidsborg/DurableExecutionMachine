namespace DurableExecutionMachine.Hosting;

public delegate Task SyncCallback(byte[] state, DateTime? minTimeout);