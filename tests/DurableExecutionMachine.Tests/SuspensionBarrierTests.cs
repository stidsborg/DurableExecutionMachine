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
    public async Task SuspendedWhile_CompletedTask_KeepsTheCallerRunning()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var allSuspended = barrier.WaitForAllSuspended();

        Assert.AreEqual(5, await barrier.SuspendedWhile(Task.FromResult(5)));
        await barrier.SuspendedWhile(Task.CompletedTask);

        Assert.IsFalse(allSuspended.IsCompleted, "the caller never waited, so it never counted as suspended");
        Assert.AreEqual(1, barrier.Running);
    }

    [TestMethod]
    public async Task SuspendedWhile_CompletedTask_IsHeldByARequestedSuspension()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();

        barrier.RequestSuspension();
        var wait = barrier.SuspendedWhile(Task.FromResult(5));

        await barrier.WaitForAllSuspended().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsFalse(wait.IsCompleted, "the caller is held by the suspension");

        barrier.Release();
        Assert.AreEqual(5, await wait.WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.AreEqual(1, barrier.Running);
    }

    [TestMethod]
    public async Task SuspendedUntil_CompletedTask_ReturnsFalse_WithoutRegisteringTheTimeout()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var allSuspended = barrier.WaitForAllSuspended();

        Assert.IsFalse(await barrier.SuspendedUntil(Task.CompletedTask, new ExecutionScopeId("0"), DateTime.UtcNow.AddHours(-1)));

        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.IsFalse(allSuspended.IsCompleted, "the caller never waited, so it never counted as suspended");
        Assert.AreEqual(1, barrier.Running);
    }
}
