using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class QuiescenceTests
{
    [TestMethod]
    public async Task Quiescence_WaitsForRunningCapture_AndCompletesWhenFlowWaitsForMessage()
    {
        var gates = new Gates();
        var stm = NewStateMachine(waitForMessageFirst: false);
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var quiescence = stm.Quiescence();
        Assert.IsFalse(quiescence.IsCompleted, "the capture is still running");

        gates.ReleaseCapture.SetResult();
        await quiescence.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsFalse(stm.IsCompleted, "the flow should be waiting for the message");
    }

    [TestMethod]
    public async Task Quiescence_CanBeAwaitedAgain_AfterTheFlowRunsAgain()
    {
        var gates = new Gates();
        var stm = NewStateMachine(waitForMessageFirst: true);
        stm.Start(gates, "param");
        await stm.Quiescence().WaitAsync(TimeSpan.FromSeconds(5));

        _ = stm.DeliverMessage("hello");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var quiescence = stm.Quiescence();
        Assert.IsFalse(quiescence.IsCompleted, "the capture is still running");

        gates.ReleaseCapture.SetResult();
        await quiescence.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsFalse(stm.IsCompleted, "the flow should be waiting for the second message");
    }

    private sealed class Gates
    {
        public TaskCompletionSource InCapture { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource ReleaseCapture { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static StateMachine<Gates, string, string> NewStateMachine(bool waitForMessageFirst)
        => new StateMachineFactory<Gates, string, string>(
            startFlow: async (gates, _, ctx) =>
            {
                if (waitForMessageFirst)
                    await ctx.Message<string>();

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
