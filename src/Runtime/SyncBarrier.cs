using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

// lets a subflow wait until its current state has been persisted.
// the host calls SyncStarted before it snapshots the state and SyncCompleted once the snapshot is persisted.
// the host must call them alternately and never concurrently, and Dispose neither concurrently with them
internal sealed class SyncBarrier
{
    private readonly Lock _lock = new();
    private readonly AsyncSignal _nextSync = new();
    private TaskCompletionSource? _syncing;

    // completed while a sync is requested that no started sync covers
    private TaskCompletionSource _requested = NewTcs();

    // true exactly while _requested is completed. written under the lock, read without it
    private volatile bool _isRequested;
    public bool IsSyncRequested => _isRequested;

    // set once the host gives up on the machine. written and read under the lock
    private bool _disposed;

    // requests a sync and waits for it, see NextSync
    public Task Sync()
    {
        lock (_lock)
        {
            if (_disposed)
                return Disposed();

            _requested.TrySetResult();
            _isRequested = true;
            return _nextSync.Wait();
        }
    }

    // waits for the next sync without requesting one.
    // a sync already in progress may have snapshotted the state before this call, so wait for the next one
    public Task NextSync()
    {
        lock (_lock)
            return _disposed ? Disposed() : _nextSync.Wait();
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
            ObjectDisposedException.ThrowIf(_disposed, this);
            _syncing = _nextSync.MoveNext();
            // the starting sync covers every request made so far
            if (_requested.Task.IsCompleted)
                _requested = NewTcs();
            _isRequested = false;
        }
    }

    public void SyncCompleted()
    {
        _syncing?.SetResult();
        _syncing = null;
    }

    // fails the sync in progress, if any, and the next one, so everyone waiting for a sync is told at once. every later wait fails too
    public void Dispose()
    {
        TaskCompletionSource? syncing;
        TaskCompletionSource next;
        lock (_lock)
        {
            if (_disposed)
                return;

            _disposed = true;
            // no sync can start any more, so none is requested
            if (_requested.Task.IsCompleted)
                _requested = NewTcs();
            _isRequested = false;
            syncing = _syncing;
            _syncing = null;
            next = _nextSync.MoveNext();
        }

        var disposed = new ObjectDisposedException(nameof(SyncBarrier));
        syncing?.SetException(disposed);
        next.SetException(disposed);
    }

    private Task Disposed() => Task.FromException(new ObjectDisposedException(nameof(SyncBarrier)));

    private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
}
