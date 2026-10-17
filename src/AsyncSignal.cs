namespace DurableExecutionMachine;

public sealed class AsyncSignal
{
    private TaskCompletionSource _waitingTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lock _lock = new();

    public Task Wait()
    {
        lock (_lock)
            return _waitingTcs.Task;
    }

    public TaskCompletionSource MoveNext()
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            tcs = _waitingTcs;
            _waitingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        return tcs;
    }
    
    public void Notify()
    {
        TaskCompletionSource tcs;
        lock (_lock)
        {
            tcs = _waitingTcs;
            _waitingTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        
        tcs.TrySetResult();
    }
}