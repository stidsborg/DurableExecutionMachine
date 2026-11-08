using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;
using ExecutionScope = DurableExecutionMachine.Runtime.ExecutionScope;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class WhenAllTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public async Task WhenAll_ReturnsResultsInArgumentOrder_WhenChildrenCompleteOutOfOrder()
    {
        var states = new States(new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var suspensionBarrier = new SuspensionBarrier();
        var ctx = CreateContext(states, suspensionBarrier);
        var firstGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var whenAll = ctx.WhenAll([
            async () => { await firstGate.Task; return 1; },
            async () => { await Task.Yield(); return 2; },
            () => Task.FromResult(3)
        ]);
        firstGate.SetResult();

        CollectionAssert.AreEqual(new[] { 1, 2, 3 }, await whenAll);
        Assert.AreEqual(0, suspensionBarrier.Total);
    }

    [TestMethod]
    public async Task WhenAll_ReturnsStoredResults_OnReplay_WithoutRunningChildren()
    {
        var firstStates = new States(new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates, new SuspensionBarrier());
        var sideEffects = 0;
        var firstResult = await firstRun.WhenAll([
            () => firstRun.Capture(() => ++sideEffects),
            () => firstRun.Capture(() => ++sideEffects),
            () => firstRun.Capture(() => ++sideEffects)
        ]);

        CollectionAssert.AreEquivalent(new[] { 1, 2, 3 }, firstResult);
        Assert.AreEqual(3, sideEffects);

        var replayStates = States.Deserialize(firstStates.GetBytes(), new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates, new SuspensionBarrier());
        var childRan = false;
        var replayResult = await replay.WhenAll([
            () => { childRan = true; return Task.FromResult(-1); },
            () => { childRan = true; return Task.FromResult(-1); },
            () => { childRan = true; return Task.FromResult(-1); }
        ]);

        CollectionAssert.AreEqual(firstResult, replayResult);
        Assert.IsFalse(childRan);
        Assert.AreEqual(3, sideEffects);
    }

    [TestMethod]
    public async Task WhenAll_ReplaysOnlyUnfinishedChildren_WhenRestartedDuringJoin()
    {
        var firstStates = new States(new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var firstRun = CreateContext(firstStates, new SuspensionBarrier());
        var sideEffects = 0;
        var neverCompletes = new TaskCompletionSource<int>();
        _ = firstRun.WhenAll([
            () => firstRun.Capture(() => ++sideEffects),
            () => neverCompletes.Task
        ]);

        Assert.AreEqual(1, sideEffects);
        var snapshot = firstStates.GetBytes();

        var replayStates = States.Deserialize(snapshot, new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates, new SuspensionBarrier());
        var replayResult = await replay.WhenAll([
            () => replay.Capture(() => ++sideEffects),
            () => Task.FromResult(2)
        ]);

        CollectionAssert.AreEqual(new[] { 1, 2 }, replayResult);
        Assert.AreEqual(1, sideEffects);
    }

    [TestMethod]
    public async Task WhenAll_JoinsRemainingChildren_BeforeThrowingFailedChildsException()
    {
        var firstStates = new States(new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var firstSuspensionBarrier = new SuspensionBarrier();
        var firstRun = CreateContext(firstStates, firstSuspensionBarrier);
        var siblingGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var whenAll = firstRun.WhenAll<int>([
            () => throw new InvalidOperationException("boom"),
            async () => { await siblingGate.Task; return 2; }
        ]);

        await Task.Delay(50);
        Assert.IsFalse(whenAll.IsCompleted);

        siblingGate.SetResult();
        await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(() => whenAll);
        Assert.AreEqual(0, firstSuspensionBarrier.Total);

        var replayStates = States.Deserialize(firstStates.GetBytes(), new SuspensionBarrier(), NoSync, new TimeoutsManager());
        var replay = CreateContext(replayStates, new SuspensionBarrier());
        var childRan = false;
        var exception = await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(
            () => replay.WhenAll([
                () => { childRan = true; return Task.FromResult(1); },
                () => { childRan = true; return Task.FromResult(2); }
            ])
        );

        Assert.IsFalse(childRan);
        Assert.AreEqual("boom", exception.FlowErrorMessage);
    }

    [TestMethod]
    public async Task WhenAll_FlushesAfterEachChildAndAtTheEnd_WhenFlushIsTrue()
    {
        var flushes = 0;
        var states = new States(new SuspensionBarrier(), (_, _) => { flushes++; return Task.CompletedTask; }, new TimeoutsManager());
        var ctx = CreateContext(states, new SuspensionBarrier());

        await ctx.WhenAll([() => Task.FromResult(1), () => Task.FromResult(2)]);

        Assert.AreEqual(3, flushes);
    }

    [TestMethod]
    public async Task WhenAll_NeverFlushes_WhenFlushIsFalse()
    {
        var flushes = 0;
        var states = new States(new SuspensionBarrier(), (_, _) => { flushes++; return Task.CompletedTask; }, new TimeoutsManager());
        var ctx = CreateContext(states, new SuspensionBarrier());

        var results = await ctx.WhenAll([() => Task.FromResult(1), () => Task.FromResult(2)], flush: false);

        CollectionAssert.AreEqual(new[] { 1, 2 }, results);
        Assert.AreEqual(0, flushes);
    }

    [TestMethod]
    public async Task Suspend_Completes_WhileWhenAllChildIsParkedInDelay()
    {
        var factory = new StateMachineFactory<object, int, int>(
            async (_, _, ctx) =>
            {
                var results = await ctx.WhenAll([async () =>
                {
                    await ctx.Delay(TimeSpan.FromHours(1));
                    return 1;
                }]);
                return results[0];
            },
            NoSync
        );
        var stm = factory.New();

        stm.Start(new object(), param: 0);
        await stm.Suspend().WaitAsync(TimeSpan.FromSeconds(5));
        stm.ResumeAfterSuspend();

        Assert.IsFalse(stm.IsCompleted);
    }

    private static Context CreateContext(States states, SuspensionBarrier suspensionBarrier)
    {
        var timeoutsManager = new TimeoutsManager();
        var ctx = new Context(new ExecutionScope(), states, messages: null!, suspensionBarrier, timeoutsManager, utcNow: () => DateTime.UtcNow);
        ctx.Scope.SetRoot();
        return ctx;
    }
}
