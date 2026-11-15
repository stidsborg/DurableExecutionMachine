using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class StatusTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    private static StateMachine<object, int, string> NewMachine(Func<Context, Task<string>> run, SyncCallback? sync = null)
        => new StateMachineFactory<object, int, string>((_, _, ctx) => run(ctx), sync ?? NoSync).New();

    [TestMethod]
    public void Status_IsNotStarted_BeforeStart()
    {
        var stm = NewMachine(_ => Task.FromResult("done"));

        Assert.AreEqual(StateMachineStatus.NotStarted, stm.Status);
        Assert.IsFalse(stm.IsSuspended);
    }

    [TestMethod]
    public async Task Status_IsCompleted_RightAfterSuspend()
    {
        var stm = NewMachine(async ctx =>
        {
            await ctx.Capture(() => "captured");
            return "done";
        });

        stm.Start(new object(), param: 1);
        await stm.Suspend();

        Assert.AreEqual(StateMachineStatus.Completed, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsFailed_WhenFlowThrows()
    {
        var stm = NewMachine(async _ =>
        {
            await Task.Yield();
            throw new InvalidOperationException("boom");
        });

        stm.Start(new object(), param: 1);
        await stm.Suspend();

        Assert.AreEqual(StateMachineStatus.Failed, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsRunning_WhileFlowExecutes_AndSuspensionWaitsForIt()
    {
        var work = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stm = NewMachine(async ctx => await ctx.Capture(() => work.Task, flush: false));

        stm.Start(new object(), param: 1);
        Assert.AreEqual(StateMachineStatus.Running, stm.Status);

        var suspended = stm.Suspend();
        Assert.IsFalse(stm.IsSuspended);

        work.SetResult("done");
        await suspended;

        Assert.IsTrue(stm.IsSuspended);
        Assert.AreEqual(StateMachineStatus.Completed, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsAwaitingSync_WhileSyncCallbackRuns()
    {
        var syncEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowSync = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stm = NewMachine(
            async ctx =>
            {
                await ctx.Capture(() => "captured");
                return "done";
            },
            sync: async (_, _) =>
            {
                syncEntered.TrySetResult();
                await allowSync.Task;
            });

        stm.Start(new object(), param: 1);
        await syncEntered.Task;

        Assert.AreEqual(StateMachineStatus.AwaitingSync, stm.Status);

        allowSync.SetResult();
        await stm.Suspend();

        Assert.AreEqual(StateMachineStatus.Completed, stm.Status);
    }

    [TestMethod]
    public async Task Status_IsSettled_WhenFlowWaitsForMessage()
    {
        var stm = NewMachine(async ctx => await ctx.Message<string>());

        stm.Start(new object(), param: 1);
        await stm.Suspend();

        Assert.AreEqual(StateMachineStatus.Settled, stm.Status);
        Assert.IsTrue(stm.IsSuspended);

        stm.ResumeAfterSuspend();
        Assert.IsFalse(stm.IsSuspended);
    }
}
