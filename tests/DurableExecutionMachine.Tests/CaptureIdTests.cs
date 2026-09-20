namespace DurableExecutionMachine.Tests;

[TestClass]
public class CaptureIdTests
{
    private static readonly string[] ExpectedScopes =
    [
        "",
        "0",
        "0.0",
        "0.0.0",
        "0.0.1",
        "0.1",
        "0.1.0",
        "0.1.1",
        "1",
        "1.0",
        "1.0.0",
        "1.0.1",
        "1.1",
        "1.1.0",
        "1.1.1",
    ];

    [TestMethod]
    public async Task Capture_AssignsHierarchicalIds_WhenAllWorkCompletesSynchronously()
    {
        var scopes = await RunFlow(yield: () => Task.CompletedTask);

        CollectionAssert.AreEqual(ExpectedScopes, scopes);
    }

    [TestMethod]
    public async Task Capture_AssignsHierarchicalIds_WhenWorkResumesOnOtherThreads()
    {
        var scopes = await RunFlow(yield: async () => await Task.Yield());

        CollectionAssert.AreEqual(ExpectedScopes, scopes);
    }

    private static readonly StateMachineFactory<RecordingFlow, int, string> Factory =
        new((f, i, ctx) => f.Run(i, ctx));

    private static async Task<List<string>> RunFlow(Func<Task> yield)
    {
        var flow = new RecordingFlow(yield);
        var stm = Factory.New();

        stm.Start(flow, param: 1);
        await stm.Sync();

        Assert.IsTrue(stm.IsCompleted);
        return flow.Scopes;
    }

    /// <summary>
    /// Two levels of nested captures with two leaves each. Every piece of work records the
    /// scope it observes on entry, so the recorded sequence is the capture tree in execution order.
    /// <paramref name="yield"/> runs before and after each capture, so the same shape can be run
    /// both synchronously and with every await resuming as a real continuation.
    /// </summary>
    private class RecordingFlow(Func<Task> yield)
    {
        public List<string> Scopes { get; } = [];

        public async Task<string> Run(int input, Context ctx)
        {
            await yield();
            Scopes.Add(ctx.Scope.ToString());
            await ctx.Capture(async () =>
            {
                await yield();
                Scopes.Add(ctx.Scope.ToString());
                await ctx.Capture(async () =>
                {
                    await yield();
                    Scopes.Add(ctx.Scope.ToString());
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    return "";
                });
                await yield();
                await ctx.Capture(async () =>
                {
                    await yield();
                    Scopes.Add(ctx.Scope.ToString());
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    return "";
                });
                await yield();
                return "";
            });
            await yield();
            await ctx.Capture(async () =>
            {
                await yield();
                Scopes.Add(ctx.Scope.ToString());
                await ctx.Capture(async () =>
                {
                    await yield();
                    Scopes.Add(ctx.Scope.ToString());
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    return "";
                });
                await yield();
                await ctx.Capture(async () =>
                {
                    await yield();
                    Scopes.Add(ctx.Scope.ToString());
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    await ctx.Capture(async () => { await yield(); Scopes.Add(ctx.Scope.ToString()); return ""; });
                    await yield();
                    return "";
                });
                await yield();
                return "";
            });
            await yield();
            return "";
        }
    }
}
