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
    public async Task NextQuiescenceOrSync_CompletesWhenSyncIsRequested_WhileCaptureRuns()
    {
        var gates = new Gates();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var next = stm.NextQuiescenceOrSync();
        Assert.IsFalse(next.IsCompleted, "the capture is still running");

        // waits for the next flush, i.e. requests a sync
        _ = stm.DeliverMessage("hello");
        await next.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsFalse(gates.ReleaseCapture.Task.IsCompleted);
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
