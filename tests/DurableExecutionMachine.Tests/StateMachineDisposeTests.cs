using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class StateMachineDisposeTests
{
    [TestMethod]
    public async Task Dispose_FailsAWaitingDelivery_AndALaterOne()
    {
        var stm = NewStateMachine();
        stm.Start(new object(), "param");
        var waiting = stm.DeliverMessage("hello");

        stm.Dispose();

        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => waiting.WaitAsync(TimeSpan.FromSeconds(5)));
        await Assert.ThrowsExactlyAsync<ObjectDisposedException>(() => stm.DeliverMessage("again").WaitAsync(TimeSpan.FromSeconds(5)));
        Assert.ThrowsExactly<ObjectDisposedException>(() => stm.StartSync());
    }

    [TestMethod]
    public async Task Dispose_FailsACaptureWaitingForItsFlush()
    {
        var stm = new StateMachineFactory<object, string, string>(
            startFlow: async (_, _, ctx) =>
            {
                await ctx.Capture(() => 42);
                return "done";
            }
        ).New();
        stm.Start(new object(), "param");
        var requested = await stm.NextQuiescenceOrSync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.IsTrue(requested.IsSyncRequested);

        stm.Dispose();
        Assert.IsFalse(stm.IsSyncRequested, "a disposed machine cannot sync, so it requests none");

        var result = await stm.NextQuiescenceOrSync().WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreEqual(StateMachineStatus.Completed, result.Status);
        Assert.ThrowsExactly<AggregateException>(() => stm.Completion());
    }

    private static StateMachine<object, string, string> NewStateMachine()
        => new StateMachineFactory<object, string, string>(
            startFlow: (_, _, ctx) => ctx.Message<string>(m => m == "never")
        ).New();
}
