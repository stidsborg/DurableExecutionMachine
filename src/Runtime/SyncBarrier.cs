using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

// lets a subflow wait until its current state has been persisted.
// the host calls SyncStarted before it snapshots the state and SyncCompleted once the snapshot is persisted.
// the host must call them alternately and never concurrently
internal sealed class SyncBarrier
{
    private readonly AsyncSignal _nextSync = new();
    private TaskCompletionSource? _syncing;

    // a sync already in progress may have snapshotted the state before this call, so wait for the next one
    public Task Sync() => _nextSync.Wait();

    public void SyncStarted() => _syncing = _nextSync.MoveNext();

    public void SyncCompleted()
    {
        _syncing?.SetResult();
        _syncing = null;
    }
}
