using DurableExecutionMachine.Runtime;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class SyncBarrierTests
{
    [TestMethod]
    public void Sync_CompletesWhenNextSyncCompletes()
    {
        var barrier = new SyncBarrier();

        var sync = barrier.Sync();
        Assert.IsFalse(sync.IsCompleted);

        barrier.SyncStarted();
        Assert.IsFalse(sync.IsCompleted);

        barrier.SyncCompleted();
        Assert.IsTrue(sync.Wait(TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public void Sync_DuringInProgressSync_WaitsForTheFollowingSync()
    {
        var barrier = new SyncBarrier();

        barrier.SyncStarted();
        var sync = barrier.Sync();
        barrier.SyncCompleted();
        Assert.IsFalse(sync.Wait(TimeSpan.FromMilliseconds(100)));

        barrier.SyncStarted();
        barrier.SyncCompleted();
        Assert.IsTrue(sync.Wait(TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public void SyncCompleted_WithoutSyncStarted_DoesNothing()
    {
        var barrier = new SyncBarrier();

        var sync = barrier.Sync();
        barrier.SyncCompleted();

        Assert.IsFalse(sync.Wait(TimeSpan.FromMilliseconds(100)));
    }

    [TestMethod]
    public void SyncRequested_CompletesWhenSyncIsCalled_AndStaysCompletedUntilASyncStarts()
    {
        var barrier = new SyncBarrier();
        Assert.IsFalse(barrier.SyncRequested().IsCompleted);
        Assert.IsFalse(barrier.IsSyncRequested);

        _ = barrier.Sync();
        Assert.IsTrue(barrier.SyncRequested().IsCompleted);
        Assert.IsTrue(barrier.IsSyncRequested);

        barrier.SyncStarted();
        Assert.IsFalse(barrier.SyncRequested().IsCompleted);
        Assert.IsFalse(barrier.IsSyncRequested);
    }

    [TestMethod]
    public void SyncRequested_DuringInProgressSync_StaysRequested()
    {
        var barrier = new SyncBarrier();

        barrier.SyncStarted();
        _ = barrier.Sync();
        barrier.SyncCompleted();

        Assert.IsTrue(barrier.SyncRequested().IsCompleted, "the in-progress sync does not cover the request");
        Assert.IsTrue(barrier.IsSyncRequested, "the in-progress sync does not cover the request");
    }

    [TestMethod]
    public void NextSync_DoesNotRequestASync_AndCompletesWhenNextSyncCompletes()
    {
        var barrier = new SyncBarrier();

        var sync = barrier.NextSync();
        Assert.IsFalse(barrier.SyncRequested().IsCompleted);

        barrier.SyncStarted();
        barrier.SyncCompleted();
        Assert.IsTrue(sync.Wait(TimeSpan.FromSeconds(5)));
    }
}
