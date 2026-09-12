namespace DurableExecutionMachine.Tests;

[TestClass]
public class CaptureIdTests
{
    private static readonly string[] ExpectedLines =
    [
        "Id: 0 - Parent: ",
        "Id: 0.0 - Parent: 0",
        "Id: 0.0.0 - Parent: 0.0",
        "Id: 0.0.1 - Parent: 0.0",
        "Id: 0.1 - Parent: 0",
        "Id: 0.1.0 - Parent: 0.1",
        "Id: 0.1.1 - Parent: 0.1",
        "Id: 1 - Parent: ",
        "Id: 1.0 - Parent: 1",
        "Id: 1.0.0 - Parent: 1.0",
        "Id: 1.0.1 - Parent: 1.0",
        "Id: 1.1 - Parent: 1",
        "Id: 1.1.0 - Parent: 1.1",
        "Id: 1.1.1 - Parent: 1.1",
    ];

    [TestMethod]
    public async Task TestFlow_AssignsHierarchicalIds_WhenAllWorkCompletesSynchronously()
    {
        var output = new StringWriter();
        var stm = new StateMachine<TestFlow, int, string>((flow, i, ctx) => flow.Run(i, ctx));
        var ctx = new Context(new ExecutionScope(), output);

        stm.Start(new TestFlow(), ctx, param: 1);
        await stm.Sync();

        Assert.IsTrue(stm.IsCompleted);
        CollectionAssert.AreEqual(ExpectedLines, Lines(output));
    }

    [TestMethod]
    public async Task YieldingFlow_AssignsHierarchicalIds_WhenWorkResumesOnOtherThreads()
    {
        var output = new StringWriter();
        var stm = new StateMachine<YieldingFlow, int, string>((flow, i, ctx) => flow.Run(i, ctx));
        var ctx = new Context(new ExecutionScope(), output);

        stm.Start(new YieldingFlow(), ctx, param: 1);
        await stm.Sync();

        Assert.IsTrue(stm.IsCompleted);
        CollectionAssert.AreEqual(ExpectedLines, Lines(output));
    }

    private static string[] Lines(StringWriter output) =>
        output.ToString().Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

    /// <summary>
    /// Same shape as <see cref="TestFlow"/>, but every capture yields to the thread pool
    /// before and after doing its work, so each await resumes as a real continuation.
    /// </summary>
    private class YieldingFlow
    {
        public async Task<string> Run(int input, Context ctx)
        {
            await Yield();
            await ctx.Capture(() => Branch(ctx, "0"));
            await Yield();
            await ctx.Capture(() => Branch(ctx, "1"));
            await Yield();
            return "";
        }

        private static async Task<string> Branch(Context ctx, string name)
        {
            await Yield();
            await ctx.Capture(() => Leaves(ctx, name + ".0"));
            await Yield();
            await ctx.Capture(() => Leaves(ctx, name + ".1"));
            await Yield();
            return name;
        }

        private static async Task<string> Leaves(Context ctx, string name)
        {
            await Yield();
            await ctx.Capture(() => Leaf(name + ".0"));
            await Yield();
            await ctx.Capture(() => Leaf(name + ".1"));
            await Yield();
            return name;
        }

        private static async Task<string> Leaf(string name)
        {
            await Yield();
            return name;
        }

        private static async Task Yield() => await Task.Yield();
    }
}
