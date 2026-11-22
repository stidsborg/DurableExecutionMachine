using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class NextQuiescenceOrSyncTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

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
        await next.WaitAsync(TimeSpan.FromSeconds(5));
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
        Assert.IsFalse(next.Wait(TimeSpan.FromMilliseconds(100)), "the capture is still running and no sync is requested");

        gates.ReleaseCapture.SetResult();
        await next.WaitAsync(TimeSpan.FromSeconds(5));
    }

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
            },
            syncCallback: NoSync
        ).New();
}
