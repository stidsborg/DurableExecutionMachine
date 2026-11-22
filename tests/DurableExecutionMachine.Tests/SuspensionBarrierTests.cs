using DurableExecutionMachine.Runtime;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class SuspensionBarrierTests
{
    [TestMethod]
    public async Task SuspendedUntil_FiredTimeout_IsPartOfMinimumTimeout_UntilTheSubflowRunsAgain()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var timeout = DateTime.UtcNow;

        barrier.RequestSuspension();
        var wait = barrier.SuspendedUntil(new TaskCompletionSource().Task, new ExecutionScopeId("0"), timeout);
        timeoutsManager.NotifyExpiredTimeouts();

        Assert.IsFalse(wait.IsCompleted, "the timeout fired, but the subflow is held by the suspension");
        Assert.AreEqual(timeout, timeoutsManager.MinimumTimeout);

        barrier.Release();
        Assert.IsTrue(await wait.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.AreEqual(1, barrier.Running);
    }

    [TestMethod]
    public async Task SuspendedUntil_ReturnsFalse_AndRemovesTheTimeout_WhenTheTaskCompletesFirst()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var completed = new TaskCompletionSource();

        var wait = barrier.SuspendedUntil(completed.Task, new ExecutionScopeId("0"), DateTime.UtcNow.AddHours(1));
        completed.SetResult();

        Assert.IsFalse(await wait.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.AreEqual(1, barrier.Running);
    }

    [TestMethod]
    public async Task SuspendedUntil_WithoutTimeout_IsNotPartOfMinimumTimeout_AndNeverFires()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var completed = new TaskCompletionSource();

        var wait = barrier.SuspendedUntil(completed.Task, new ExecutionScopeId("0"), timeout: null);
        timeoutsManager.NotifyExpiredTimeouts();

        Assert.IsFalse(wait.IsCompleted);
        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.AreEqual(0, barrier.Running);

        completed.SetResult();
        Assert.IsFalse(await wait.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreEqual(1, barrier.Running);
    }
}
