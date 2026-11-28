using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class NextQuiescenceOrSyncTests
{
    [TestMethod]
    public async Task NextQuiescenceOrSync_CompletesWhenFlowWaitsForMessage()
    {
        var gates = new Gates();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var next = stm.NextQuiescenceOrSync();
        Assert.IsFalse(next.IsCompleted, "the capture is still running");

        gates.ReleaseCapture.SetResult();
        var result = await next.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(new StatusAndSync(StateMachineStatus.Quiescent, IsSyncRequested: false), result);
    }

    [TestMethod]
    public async Task NextQuiescenceOrSync_IsNotCompletedByDelivery_WhileCaptureRuns()
    {
        var gates = new Gates();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var next = stm.NextQuiescenceOrSync();

        // a delivery waits for the next flush without requesting one
        _ = stm.DeliverMessage("hello");
        Assert.IsFalse(stm.IsSyncRequested, "a delivery does not request a sync");
        Assert.IsFalse(next.Wait(TimeSpan.FromMilliseconds(100)), "the capture is still running and no sync is requested");

        gates.ReleaseCapture.SetResult();
        var result = await next.WaitAsync(TimeSpan.FromSeconds(5));
        // the message is already delivered, so the message wait doesn't leave the barrier: the wait completes when the flow returns,
        // which is read as Completed, or as Quiescent just before the flow's task completes
        Assert.AreNotEqual(StateMachineStatus.Running, result.Status);
        Assert.IsFalse(result.IsSyncRequested);
    }

    [TestMethod]
    public async Task NextQuiescenceOrSync_ReportsSyncRequest_AndTheCaptureWaitsUntilTheSyncCompletes()
    {
        var stm = NewFlushingStateMachine();
        stm.Start(new object(), "param");

        var requested = await stm.NextQuiescenceOrSync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(new StatusAndSync(StateMachineStatus.Running, IsSyncRequested: true), requested);

        var snapshot = stm.StartSync();
        Assert.AreEqual(42, States.DeserializeToDictionary(snapshot.States)[new ExecutionScopeId("0")]);
        Assert.IsFalse(stm.IsSyncRequested, "the started sync covers the request");

        var next = stm.NextQuiescenceOrSync();
        Assert.IsFalse(next.Wait(TimeSpan.FromMilliseconds(100)), "the capture waits for the sync to complete");

        stm.CompleteSync();
        var result = await next.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(new StatusAndSync(StateMachineStatus.Quiescent, IsSyncRequested: false), result);
    }

    private static StateMachine<object, string, string> NewFlushingStateMachine()
        => new StateMachineFactory<object, string, string>(
            startFlow: async (_, _, ctx) =>
            {
                await ctx.Capture(() => 42);
                return await ctx.Message<string>();
            }
        ).New();

    private sealed class Gates
    {
        public TaskCompletionSource InCapture { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseCapture { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static StateMachine<Gates, string, string> NewStateMachine()
        => new StateMachineFactory<Gates, string, string>(
            startFlow: async (gates, _, ctx) =>
            {
                await ctx.Capture(async () =>
                {
                    gates.InCapture.SetResult();
                    await gates.ReleaseCapture.Task;
                    return 1;
                }, flush: false);

                return await ctx.Message<string>();
            }
        ).New();
}
