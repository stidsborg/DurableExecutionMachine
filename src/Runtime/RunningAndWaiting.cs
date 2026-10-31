namespace DurableExecutionMachine.Runtime;

internal class RunningAndWaiting
{
    private readonly Lock _lock = new();
    
    private TaskCompletionSource? _shouldSuspend;
    private TaskCompletionSource? _allSuspended;

    public int Total { get; private set; }
    public int Suspended { get; private set; }

    public bool IsAllSuspended()
    {
        lock (_lock)
            return Suspended == Total;
    }

    public async Task SuspendIfNeeded()
    {
        lock (_lock)
        {
            if (_shouldSuspend == null)
                return;

            Suspended++;
            SignalIfAllSuspended();
        }

        await ResumeSubflow();
    }
    
    public async Task ResumeSubflow()
    {
        while (true)
        {
            TaskCompletionSource shouldSuspend;
            lock (_lock)
            {
                if (_shouldSuspend == null)
                {
                    Suspended--;
                    return;
                }
                
                shouldSuspend = _shouldSuspend;         
            }
            
            await shouldSuspend.Task;
        }
    }
    
    public void SubflowSuspended()
    {
        lock (_lock)
        {
            Suspended++;
            SignalIfAllSuspended();
        }
    }
    
    public async Task StartMessageDelivery()
    {
        while (true)
        {
            Task shouldSuspend;
            lock (_lock)
            {
                if (_shouldSuspend == null)
                {
                    Total++;
                    return;
                }

                shouldSuspend = _shouldSuspend.Task;
            }

            await shouldSuspend;
        }
    }

    public void MessageDeliveryCompleted() => SubflowOrMessageDeliveryCompleted();

    public void SubflowStarted()
    {
        lock (_lock)
            Total++;
    }

    public void SubflowCompleted() => SubflowOrMessageDeliveryCompleted();

    public void Suspend()
    {
        lock (_lock)
            _shouldSuspend ??= NewTcs();
    }

    public void Resume()
    {
        TaskCompletionSource? shouldSuspend;
        lock (_lock)
        {
            if (_shouldSuspend == null)
                return;
            
            shouldSuspend = _shouldSuspend;
            _shouldSuspend = null;
            _allSuspended = null;
        }

        shouldSuspend.SetResult();
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

        _allSuspended.TrySetResult();
    }

    private void SubflowOrMessageDeliveryCompleted()
    {
        lock (_lock)
        {
            Total--;
            SignalIfAllSuspended();
        }
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
