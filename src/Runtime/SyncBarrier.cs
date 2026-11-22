using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

// lets a subflow wait until its current state has been persisted.
// the host calls SyncStarted before it snapshots the state and SyncCompleted once the snapshot is persisted.
// the host must call them alternately and never concurrently
internal sealed class SyncBarrier
{
    private readonly Lock _lock = new();
    private readonly AsyncSignal _nextSync = new();
    private TaskCompletionSource? _syncing;

    // completed while a sync is requested that no started sync covers
    private TaskCompletionSource _requested = NewTcs();

    // a sync already in progress may have snapshotted the state before this call, so wait for the next one
    public Task Sync()
    {
        lock (_lock)
        {
            _requested.TrySetResult();
            return _nextSync.Wait();
        }
    }

    // completes when a sync is requested, or at once if one already is
    public Task SyncRequested()
    {
        lock (_lock)
            return _requested.Task;
    }

    public void SyncStarted()
    {
        lock (_lock)
        {
            _syncing = _nextSync.MoveNext();
            // the starting sync covers every request made so far
            if (_requested.Task.IsCompleted)
                _requested = NewTcs();
        }
    }

    public void SyncCompleted()
    {
        _syncing?.SetResult();
        _syncing = null;
    }

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
