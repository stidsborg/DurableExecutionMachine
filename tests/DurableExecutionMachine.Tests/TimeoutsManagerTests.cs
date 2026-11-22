using DurableExecutionMachine.Runtime;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class TimeoutsManagerTests
{
    [TestMethod]
    public async Task FiredTimeout_IsPartOfMinimumTimeout_UntilCancelled()
    {
        using var timeoutsManager = new TimeoutsManager();
        var id = new ExecutionScopeId("0");
        var timeout = DateTime.UtcNow;
        var fired = new TaskCompletionSource();

        timeoutsManager.RegisterTimeout(id, timeout, () => fired.SetResult());
        await fired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual(timeout, timeoutsManager.MinimumTimeout, "the waiter has not run again yet");

        timeoutsManager.CancelTimeout(id);
        Assert.IsNull(timeoutsManager.MinimumTimeout);
    }

    [TestMethod]
    public async Task LaterTimeout_Fires_WhileAFiredTimeoutIsStillRegistered()
    {
        using var timeoutsManager = new TimeoutsManager();
        var firstFired = new TaskCompletionSource();
        timeoutsManager.RegisterTimeout(new ExecutionScopeId("0"), DateTime.UtcNow, () => firstFired.SetResult());
        await firstFired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var secondFired = new TaskCompletionSource();
        timeoutsManager.RegisterTimeout(new ExecutionScopeId("1"), DateTime.UtcNow.AddMilliseconds(50), () => secondFired.SetResult());

        await secondFired.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
