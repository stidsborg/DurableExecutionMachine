using DurableExecutionMachine.Runtime;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class SuspensionBarrierTests
{
    [TestMethod]
    public async Task Timeout_IsPartOfMinimumTimeout_UntilTheSubflowRunsAgain()
    {
        using var timeoutsManager = new TimeoutsManager();
        var barrier = new SuspensionBarrier(timeoutsManager);
        barrier.Enter();
        var timeoutId = new ExecutionScopeId("0");
        var timeout = DateTime.UtcNow.AddMinutes(1);
        var woken = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        timeoutsManager.RegisterTimeout(timeoutId, timeout, () => woken.TrySetResult());

        var wait = barrier.SuspendedWhile(woken.Task, timeoutId);
        Assert.AreEqual(timeout, timeoutsManager.MinimumTimeout);

        barrier.RequestSuspension();
        woken.SetResult();
        Assert.AreEqual(timeout, timeoutsManager.MinimumTimeout, "the subflow is woken, but held by the suspension");

        barrier.Release();
        await wait.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.AreEqual(1, barrier.Running);
    }
}
