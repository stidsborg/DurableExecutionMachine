using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;
using ExecutionScope = DurableExecutionMachine.Runtime.ExecutionScope;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class ParallelReplayTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public async Task Parallel_ReturnsStoredResult_OnReplay_WithoutRunningSubTask()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRunningAndWaiting = new RunningAndWaiting();
        var firstRun = CreateContext(firstStates, firstRunningAndWaiting);
        var sideEffects = 0;
        var firstResult = await firstRun.Parallel(async () =>
        {
            var inner = await firstRun.Capture(() => ++sideEffects);
            return inner * 10;
        });

        Assert.AreEqual(10, firstResult);
        Assert.AreEqual(1, sideEffects);
        Assert.AreEqual(0, firstRunningAndWaiting.Total);

        var replayStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var replayRunningAndWaiting = new RunningAndWaiting();
        var replay = CreateContext(replayStates, replayRunningAndWaiting);
        var subTaskRan = false;
        var replayResult = await replay.Parallel(async () =>
        {
            subTaskRan = true;
            return await replay.Capture(() => ++sideEffects) * 10;
        });

        Assert.AreEqual(10, replayResult);
        Assert.IsFalse(subTaskRan);
        Assert.AreEqual(1, sideEffects);
        Assert.AreEqual(0, replayRunningAndWaiting.Total);
    }

    [TestMethod]
    public async Task Parallel_StoresNullResult_AndReturnsItOnReplay()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates, new RunningAndWaiting());
        var firstResult = await firstRun.Parallel(() => Task.FromResult<string?>(null));
        Assert.IsNull(firstResult);

        var replayStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates, new RunningAndWaiting());
        var subTaskRan = false;
        var replayResult = await replay.Parallel(() =>
        {
            subTaskRan = true;
            return Task.FromResult<string?>("not stored");
        });

        Assert.IsNull(replayResult);
        Assert.IsFalse(subTaskRan);
    }

    [TestMethod]
    public async Task Parallel_RethrowsStoredException_OnReplay_WithoutRunningSubTask()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRunningAndWaiting = new RunningAndWaiting();
        var firstRun = CreateContext(firstStates, firstRunningAndWaiting);
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => firstRun.Parallel<int>(() => throw new InvalidOperationException("boom"))
        );
        Assert.AreEqual(0, firstRunningAndWaiting.Total);

        var replayStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates, new RunningAndWaiting());
        var subTaskRan = false;
        var exception = await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => replay.Parallel(() => { subTaskRan = true; return Task.FromResult(42); })
        );

        Assert.IsFalse(subTaskRan);
        Assert.AreEqual("boom", exception.FlowErrorMessage);
    }

    private static Context CreateContext(States states, RunningAndWaiting runningAndWaiting)
    {
        var timeoutsManager = new TimeoutsManager();
        var ctx = new Context(new ExecutionScope(), states, messages: null!, runningAndWaiting, timeoutsManager, utcNow: () => DateTime.UtcNow);
        ctx.Scope.SetRoot();
        return ctx;
    }
}
