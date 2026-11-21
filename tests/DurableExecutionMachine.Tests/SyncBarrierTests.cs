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
}
