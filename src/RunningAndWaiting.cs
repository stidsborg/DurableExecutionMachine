namespace DurableExecutionMachine;

public class RunningAndWaiting
{
    private readonly Lock _lock = new();
    
    private TaskCompletionSource? _shouldSuspend;
    private TaskCompletionSource? _allSuspended;

    public int Total { get; private set; }
    public int Suspended { get; private set; }
    
    // deliveries writing to states; all subflows only count as suspended once no delivery is in progress
    private int _deliveries;

    public bool IsAllSuspended()
    {
        lock (_lock)
            return AllSuspended;
    }

    private bool AllSuspended => Suspended == Total && _deliveries == 0;

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
    
    public Task ResumeSubflow() => WhenResumed(() => Suspended--);
    
    public void SubflowSuspended()
    {
        lock (_lock)
        {
            Suspended++;
            SignalIfAllSuspended();
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

    // a delivery waits while subflows are suspended so states does not change until they are resumed
    public Task DeliveryStarted() => WhenResumed(() => _deliveries++);

    public void DeliveryCompleted()
    {
        lock (_lock)
        {
            _deliveries--;
            SignalIfAllSuspended();
        }
    }

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
            if (AllSuspended)
                return Task.CompletedTask;

            _allSuspended ??= NewTcs();
            return _allSuspended.Task;
        }
    }

    // waits until subflows are not suspended and then applies the change under the same lock, so a suspend cannot come in between
    private async Task WhenResumed(Action change)
    {
        while (true)
        {
            TaskCompletionSource shouldSuspend;
            lock (_lock)
            {
                if (_shouldSuspend == null)
                {
                    change();
                    return;
                }
                
                shouldSuspend = _shouldSuspend;
            }
            
            await shouldSuspend.Task;
        }
    }

    private void SignalIfAllSuspended()
    {
        if (_allSuspended is null || !AllSuspended)
            return;

        _allSuspended.TrySetResult();
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
