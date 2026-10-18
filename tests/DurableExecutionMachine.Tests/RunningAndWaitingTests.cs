namespace DurableExecutionMachine.Tests;

[TestClass]
public class RunningAndWaitingTests
{
    [TestMethod]
    public async Task WaitForAllSuspended_WaitsForDeliveryInProgress()
    {
        var runningAndWaiting = new RunningAndWaiting();
        await runningAndWaiting.DeliveryStarted();

        runningAndWaiting.Suspend();
        var allSuspended = runningAndWaiting.WaitForAllSuspended();
        Assert.IsFalse(allSuspended.IsCompleted);

        runningAndWaiting.DeliveryCompleted();
        await allSuspended.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
