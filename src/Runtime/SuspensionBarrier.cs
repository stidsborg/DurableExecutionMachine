namespace DurableExecutionMachine.Runtime;

// counts the subflows and message deliveries that are running. a suspension is complete when none are running
internal class SuspensionBarrier
{
    private readonly Lock _lock = new();

    // non-null while a suspension is requested, and completed when it is released
    private TaskCompletionSource? _released;
    private TaskCompletionSource? _allSuspended;

    public int Running { get; private set; }

    // the caller is already running, so a suspension cannot complete before the new subflow is counted
    public void Enter()
    {
        lock (_lock)
            Running++;
    }

    public async Task EnterWhenReleased()
    {
        while (true)
        {
            Task released;
            lock (_lock)
            {
                if (_released == null)
                {
                    Running++;
                    return;
                }

                released = _released.Task;
            }

            await released;
        }
    }

    public void Leave()
    {
        lock (_lock)
        {
            Running--;
            if (Running == 0)
                _allSuspended?.TrySetResult();
        }
    }

    public async Task SuspendIfNeeded()
    {
        lock (_lock)
            if (_released == null)
                return;

        await WaitSuspended(Task.CompletedTask);
    }

    // the caller counts as suspended while it waits, so a suspension can complete in the meantime
    public async Task WaitSuspended(Task task)
    {
        Leave();
        try
        {
            await task;
        }
        finally
        {
            await EnterWhenReleased();
        }
    }

    public async Task<T> WaitSuspended<T>(Task<T> task)
    {
        Leave();
        try
        {
            return await task;
        }
        finally
        {
            await EnterWhenReleased();
        }
    }

    public void RequestSuspension()
    {
        lock (_lock)
            _released ??= NewTcs();
    }

    public void Release()
    {
        TaskCompletionSource? released;
        lock (_lock)
        {
            if (_released == null)
                return;

            released = _released;
            _released = null;
            _allSuspended = null;
        }

        released.SetResult();
    }

    public Task WaitForAllSuspended()
    {
        lock (_lock)
        {
            if (Running == 0)
                return Task.CompletedTask;

            _allSuspended ??= NewTcs();
            return _allSuspended.Task;
        }
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
