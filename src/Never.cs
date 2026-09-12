namespace DurableExecutionMachine;

/// <summary>
/// A task that never completes. Awaiting it suspends the caller indefinitely.
/// </summary>
public static class Never
{
    public static Task Task { get; } = new TaskCompletionSource().Task;
}
