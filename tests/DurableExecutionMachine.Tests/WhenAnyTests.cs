using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;
using ExecutionScope = DurableExecutionMachine.Runtime.ExecutionScope;

namespace DurableExecutionMachine.Tests;

[TestClass]
public class WhenAnyTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public async Task WhenAny_ReturnsFirstToFinish_AndCancelsTheOthersToken()
    {
        var runningAndWaiting = new RunningAndWaiting();
        var ctx = CreateContext(NewStates(), runningAndWaiting);
        var loserToken = CancellationToken.None;

        var winner = await ctx.WhenAny([
            async ct => { loserToken = ct; await Task.Delay(Timeout.Infinite, ct); return "loser"; },
            async _ => { await Task.Yield(); return "winner"; }
        ]);

        Assert.AreEqual("winner", winner);
        Assert.IsTrue(loserToken.IsCancellationRequested);
        Assert.AreEqual(0, runningAndWaiting.Total);
        Assert.AreEqual(0, runningAndWaiting.Suspended);
    }

    [TestMethod]
    public async Task WhenAny_CancelsLosingDelay_AndRemovesItsTimeout()
    {
        var timeoutsManager = new TimeoutsManager();
        var runningAndWaiting = new RunningAndWaiting();
        var ctx = CreateContext(NewStates(), runningAndWaiting, timeoutsManager);
        var loserCancelled = false;

        var winner = await ctx.WhenAny([
            async _ =>
            {
                try { await ctx.Delay(TimeSpan.FromHours(1)); }
                catch (OperationCanceledException) { loserCancelled = true; throw; }
                return 1;
            },
            _ => Task.FromResult(2)
        ]).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual(2, winner);
        Assert.IsTrue(loserCancelled);
        Assert.IsNull(timeoutsManager.MinimumTimeout);
        Assert.AreEqual(0, runningAndWaiting.Suspended);
    }

    [TestMethod]
    public async Task WhenAny_CancelsLosingMessage_SoALaterWaitReceivesTheMessage()
    {
        var states = NewStates();
        var timeoutsManager = new TimeoutsManager();
        var messages = new Messages(states, timeoutsManager, () => DateTime.UtcNow);
        messages.Initialize();
        var ctx = CreateContext(states, new RunningAndWaiting(), timeoutsManager, messages);

        var winner = await ctx.WhenAny([
            async _ => await ctx.Message<string>(),
            _ => Task.FromResult("fast")
        ]).WaitAsync(TimeSpan.FromSeconds(5));
        var later = ctx.Message<string>();
        _ = messages.Deliver("hello", idempotencyKey: null, timeout: null);

        Assert.AreEqual("fast", winner);
        Assert.AreEqual("hello", await later.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public async Task WhenAny_CancelsNestedWhenAny_InLosingBranch()
    {
        var timeoutsManager = new TimeoutsManager();
        var ctx = CreateContext(NewStates(), new RunningAndWaiting(), timeoutsManager);
        var innerToken = CancellationToken.None;

        var winner = await ctx.WhenAny([
            _ => ctx.WhenAny([
                async ct => { innerToken = ct; await ctx.Delay(TimeSpan.FromHours(1)); return "inner"; },
                async _ => { await ctx.Delay(TimeSpan.FromHours(2)); return "inner"; }
            ]),
            async _ => { await Task.Yield(); return "outer"; }
        ]).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.AreEqual("outer", winner);
        Assert.IsTrue(innerToken.IsCancellationRequested);
        Assert.IsNull(timeoutsManager.MinimumTimeout);
    }

    [TestMethod]
    public async Task WhenAny_NestedWinner_DoesNotCancelOuterSiblings()
    {
        var ctx = CreateContext(NewStates(), new RunningAndWaiting());
        var siblingGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var siblingToken = CancellationToken.None;

        var outer = ctx.WhenAny([
            async _ =>
            {
                var inner = await ctx.WhenAny([_ => Task.FromResult(1)]);
                await siblingGate.Task;
                return inner;
            },
            async ct => { siblingToken = ct; await Task.Delay(Timeout.Infinite, ct); return 2; }
        ]);

        await Task.Delay(50);
        Assert.IsFalse(siblingToken.IsCancellationRequested);

        siblingGate.SetResult();
        Assert.AreEqual(1, await outer);
        Assert.IsTrue(siblingToken.IsCancellationRequested);
    }

    [TestMethod]
    public async Task WhenAny_ThrowsWinnersException_WhenFirstToFinishFails()
    {
        var ctx = CreateContext(NewStates(), new RunningAndWaiting());
        var loserToken = CancellationToken.None;

        await Assert.ThrowsExactlyAsync<FatalWorkflowException<InvalidOperationException>>(() => ctx.WhenAny<int>([
            _ => throw new InvalidOperationException("boom"),
            async ct => { loserToken = ct; await Task.Delay(Timeout.Infinite, ct); return 2; }
        ]));

        Assert.IsTrue(loserToken.IsCancellationRequested);
    }

    [TestMethod]
    public async Task WhenAny_ReturnsStoredResult_OnReplay_WithoutRunningChildren()
    {
        var firstStates = NewStates();
        var firstRun = CreateContext(firstStates, new RunningAndWaiting());
        var firstResult = await firstRun.WhenAny([
            async ct => { await Task.Delay(Timeout.Infinite, ct); return "loser"; },
            _ => Task.FromResult("winner")
        ]);
        Assert.AreEqual("winner", firstResult);

        var replay = CreateContext(Restart(firstStates), new RunningAndWaiting());
        var childRan = false;
        var replayResult = await replay.WhenAny([
            _ => { childRan = true; return Task.FromResult("first"); },
            _ => { childRan = true; return Task.FromResult("second"); }
        ]);

        Assert.AreEqual("winner", replayResult);
        Assert.IsFalse(childRan);
    }

    [TestMethod]
    public async Task WhenAny_StartsLosersCancelled_OnReplay_WhenRestartedDuringJoin()
    {
        var firstStates = NewStates();
        var firstRun = CreateContext(firstStates, new RunningAndWaiting());
        var loserIgnoresCancellation = new TaskCompletionSource<string>();
        _ = firstRun.WhenAny([
            _ => loserIgnoresCancellation.Task,
            async _ => { await Task.Yield(); return "winner"; }
        ]);
        await Task.Delay(50);

        var replay = CreateContext(Restart(firstStates), new RunningAndWaiting());
        var loserStartedCancelled = false;
        var winnerRan = false;
        var replayResult = await replay.WhenAny([
            ct => { loserStartedCancelled = ct.IsCancellationRequested; return Task.FromResult("loser"); },
            _ => { winnerRan = true; return Task.FromResult("not stored"); }
        ]);

        Assert.AreEqual("winner", replayResult);
        Assert.IsTrue(loserStartedCancelled);
        Assert.IsFalse(winnerRan);
    }

    [TestMethod]
    public async Task Sync_Completes_WhileWhenAnyChildrenAreParkedInDelay()
    {
        var factory = new StateMachineFactory<object, int, int>(
            (_, _, ctx) => ctx.WhenAny([
                async _ => { await ctx.Delay(TimeSpan.FromHours(1)); return 1; },
                async _ => { await ctx.Delay(TimeSpan.FromHours(2)); return 2; }
            ]),
            NoSync
        );
        var stm = factory.New();

        stm.Start(new object(), param: 0);
        await stm.Sync().WaitAsync(TimeSpan.FromSeconds(5));

        Assert.IsFalse(stm.IsCompleted);
    }

    private static States NewStates() => new(new RunningAndWaiting(), NoSync, new TimeoutsManager());

    private static States Restart(States states)
        => States.Deserialize(states.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());

    private static Context CreateContext(
        States states,
        RunningAndWaiting runningAndWaiting,
        TimeoutsManager? timeoutsManager = null,
        Messages? messages = null)
    {
        var ctx = new Context(new ExecutionScope(), states, messages!, runningAndWaiting, timeoutsManager ?? new TimeoutsManager(), utcNow: () => DateTime.UtcNow);
        ctx.Scope.SetRoot();
        return ctx;
    }
}
