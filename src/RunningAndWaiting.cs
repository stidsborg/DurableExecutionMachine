namespace DurableExecutionMachine;

public class RunningAndWaiting
{
    private readonly Lock _lock = new();

    private bool _suspending;
    private TaskCompletionSource _resumed = NewTcs();
    private TaskCompletionSource? _allSuspended;

    public int Total { get; private set; }
    public int Suspended { get; private set; }

    public bool IsAllSuspended()
    {
        lock (_lock)
            return Suspended == Total;
    }

    public Task SuspendIfNeeded()
    {
        lock (_lock)
        {
            if (!_suspending)
                return Task.CompletedTask;

            Suspended++;
            SignalIfAllSuspended();
            return _resumed.Task;
        }
    }

    public void SubflowStarted()
    {
        lock (_lock)
            Total++;
    }

    public void SubflowCompleted()
    {
        lock (_lock)
        {
            Total--;
            SignalIfAllSuspended();
        }
    }

    public void Suspend()
    {
        lock (_lock)
            _suspending = true;
    }

    public void Resume()
    {
        TaskCompletionSource resumed;
        lock (_lock)
        {
            _suspending = false;
            Suspended = 0;
            _allSuspended = null;
            resumed = _resumed;
            _resumed = NewTcs();
        }

        resumed.SetResult();
    }

    public Task WaitForAllSuspended()
    {
        lock (_lock)
        {
            if (Suspended == Total)
                return Task.CompletedTask;

            _allSuspended ??= NewTcs();
            return _allSuspended.Task;
        }
    }

    private void SignalIfAllSuspended()
    {
        if (_allSuspended is null || Suspended != Total)
            return;

        _allSuspended.SetResult();
        _allSuspended = null;
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
