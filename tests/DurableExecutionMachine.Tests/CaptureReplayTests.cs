namespace DurableExecutionMachine.Tests;

[TestClass]
public class CaptureReplayTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public async Task Capture_RethrowsStoredException_OnReplay_WithoutRunningWork()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates);
        int Fail() => throw new InvalidOperationException("boom");
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => firstRun.Capture(Fail)
        );

        var replayStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates);
        var workRan = false;
        var exception = await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => replay.Capture(() => { workRan = true; return 42; })
        );

        Assert.IsFalse(workRan);
        Assert.AreEqual("boom", exception.FlowErrorMessage);
    }

    private static Context CreateContext(States states)
    {
        var timeoutsManager = new TimeoutsManager();
        var ctx = new Context(new ExecutionScope(), states, messages: null!, new RunningAndWaiting(), timeoutsManager, utcNow: () => DateTime.UtcNow);
        ctx.Scope.SetRoot();
        return ctx;
    }
}
