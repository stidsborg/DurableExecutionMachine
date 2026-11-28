namespace DurableExecutionMachine.Runtime;

// counts the subflows that are running. a suspension is complete when none are running
internal class SuspensionBarrier(TimeoutsManager timeoutsManager)
{
    private readonly Lock _lock = new();

    // non-null while a suspension is requested, and completed when it is released
    private TaskCompletionSource? _released;
    private TaskCompletionSource? _allSuspended;

    public int Running { get; private set; }

    // true when no subflow is running, i.e. when WaitForAllSuspended would complete at once
    public bool AllSuspended
    {
        get
        {
            lock (_lock)
                return Running == 0;
        }
    }

    // the caller is already running, so a suspension cannot complete before the new subflow is counted
    public void Enter()
    {
        lock (_lock)
            Running++;
    }

    public async Task EnterWhenReleased(ExecutionScopeId? timeoutId = null)
    {
        while (true)
        {
            Task released;
            lock (_lock)
            {
                if (_released == null)
                {
                    Running++;
                    // removed together with counting the subflow as running, so a fired timeout counts until then
                    if (timeoutId != null)
                        timeoutsManager.CancelTimeout(timeoutId);
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
            LeaveWhileLocked();
    }

    private void LeaveWhileLocked()
    {
        Running--;
        if (Running == 0)
        {
            // reset, so a later wait waits for the next time no subflow is running
            _allSuspended?.TrySetResult();
            _allSuspended = null;
        }
    }

    public async Task SuspendIfNeeded()
    {
        lock (_lock)
            if (_released == null)
                return;

        Leave();
        await EnterWhenReleased();
    }

    // the caller counts as suspended while it waits, so a suspension can complete in the meantime.
    // a task that has already completed is no wait: the caller stays running, unless a suspension is requested
    public async Task SuspendedWhile(Task task)
    {
        if (task.IsCompleted)
        {
            await SuspendIfNeeded();
            await task; // rethrows if the task failed or was cancelled
            return;
        }

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

    public async Task<T> SuspendedWhile<T>(Task<T> task)
    {
        if (task.IsCompleted)
        {
            await SuspendIfNeeded();
            return await task;
        }

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

    // as SuspendedWhile, but also completes when the timeout fires. returns true if the timeout fired before the task completed.
    // the timeout is registered when the caller counts as suspended and removed when it is running again.
    // as for SuspendedWhile, a task that has already completed is no wait, and its timeout is never registered
    public async Task<bool> SuspendedUntil(Task task, ExecutionScopeId timeoutId, DateTime timeout)
    {
        if (task.IsCompleted)
        {
            await SuspendIfNeeded();
            await task;
            return false;
        }

        var timedOut = NewTcs();
        lock (_lock)
        {
            timeoutsManager.RegisterTimeout(timeoutId, timeout, () => timedOut.TrySetResult());
            LeaveWhileLocked();
        }

        try
        {
            // the task wins if both have completed
            var first = await Task.WhenAny(task, timedOut.Task);
            if (first == task)
                await task; // rethrows if the task failed or was cancelled

            return first != task;
        }
        finally
        {
            await EnterWhenReleased(timeoutId);
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
