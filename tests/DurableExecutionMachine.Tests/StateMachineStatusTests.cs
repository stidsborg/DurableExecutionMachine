using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class StateMachineStatusTests
{
    [TestMethod]
    public async Task Status_IsRunning_WhileCaptureRuns()
    {
        var gates = new Gates();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual(StateMachineStatus.Running, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsQuiescent_WhileFlowWaitsForMessage()
    {
        var gates = new Gates();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await gates.InCapture.Task.WaitAsync(TimeSpan.FromSeconds(5));

        gates.ReleaseCapture.SetResult();
        await stm.Quiescence().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual(StateMachineStatus.Quiescent, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsCompleted_WhenFlowReturns()
    {
        var gates = new Gates();
        gates.ReleaseCapture.SetResult();
        var stm = NewStateMachine();
        stm.Start(gates, "param");
        await stm.Quiescence().WaitAsync(TimeSpan.FromSeconds(5));

        _ = stm.DeliverMessage("hello");
        await WaitUntilCompleted(stm);

        Assert.AreEqual(StateMachineStatus.Completed, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsCompleted_WhenFlowThrows()
    {
        var stm = new StateMachineFactory<Gates, string, string>(
            startFlow: (_, _, _) => Task.FromException<string>(new InvalidOperationException("boom"))
        ).New();
        stm.Start(new Gates(), "param");
        await WaitUntilCompleted(stm);

        Assert.AreEqual(StateMachineStatus.Completed, stm.Status);
    }

    private static async Task WaitUntilCompleted(StateMachine<Gates, string, string> stm)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!stm.IsCompleted && DateTime.UtcNow < deadline)
            await Task.Delay(10);
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
                    gates.InCapture.TrySetResult();
                    await gates.ReleaseCapture.Task;
                    return 1;
                }, flush: false);

                return await ctx.Message<string>();
            }
        ).New();
}
