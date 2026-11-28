using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;
using ExecutionScope = DurableExecutionMachine.Runtime.ExecutionScope;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class CaptureReplayTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public async Task Capture_RethrowsStoredException_OnReplay_WithoutRunningWork()
    {
        var firstStates = new States(new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates);
        int Fail() => throw new InvalidOperationException("boom");
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => firstRun.Capture(Fail)
        );

        var replayStates = States.Deserialize(firstStates.GetBytes(), new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates);
        var workRan = false;
        var exception = await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => replay.Capture(() => { workRan = true; return 42; })
        );

        Assert.IsFalse(workRan);
        Assert.AreEqual("boom", exception.FlowErrorMessage);
    }

    [TestMethod]
    public async Task Capture_StoresCancellation_AndThrowsItAsOperationCanceledException_OnReplay()
    {
        var firstStates = new States(new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates);
        int Cancelled() => throw new OperationCanceledException("cancelled");
        await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => firstRun.Capture(Cancelled)
        );

        var replayStates = States.Deserialize(firstStates.GetBytes(), new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates);
        var workRan = false;
        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => replay.Capture(() => { workRan = true; return 42; })
        );

        Assert.IsFalse(workRan);
        Assert.AreEqual("cancelled", exception.Message);
    }

    [TestMethod]
    public async Task Capture_ThrowsSubtypeOfOperationCanceledException_AsFatalWorkflowException_InEveryRun()
    {
        var firstStates = new States(new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates);
        int Cancelled() => throw new TaskCanceledException("cancelled");
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<TaskCanceledException>>(
            () => firstRun.Capture(Cancelled)
        );

        var replayStates = States.Deserialize(firstStates.GetBytes(), new SuspensionBarrier(new TimeoutsManager()), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates);
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<TaskCanceledException>>(
            () => replay.Capture(() => 42)
        );
    }

    private static Context CreateContext(States states)
    {
        var ctx = new Context(new ExecutionScope(), states, messages: null!, new SuspensionBarrier(new TimeoutsManager()), utcNow: () => DateTime.UtcNow);
        ctx.Scope.SetRoot();
        return ctx;
    }
}
