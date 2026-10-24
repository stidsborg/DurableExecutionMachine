namespace DurableExecutionMachine.Tests;

[TestClass]
public class MessagesTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;
    private static readonly UtcNow Now = () => DateTime.UtcNow;

    [TestMethod]
    public async Task Deliver_IgnoresDuplicateIdempotencyKey_AfterRestart()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRun = new Messages(firstStates, new TimeoutsManager(), Now);
        firstRun.Initialize();
        _ = firstRun.Deliver("hello", idempotencyKey: "key", timeout: null);

        var restartedStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var restarted = new Messages(restartedStates, new TimeoutsManager(), Now);
        restarted.Initialize();
        var duplicate = restarted.Deliver("hello", idempotencyKey: "key", timeout: null);

        var storedMessages = States
            .DeserializeToDictionary(restartedStates.GetBytes())
            .Keys
            .Count(id => id.Id.StartsWith("-1.0."));
        Assert.AreEqual(1, storedMessages);

        Assert.IsFalse(duplicate.IsCompleted);
        await restartedStates.Flush();
        await duplicate;
    }

    [TestMethod]
    public async Task Deliver_DuplicateIdempotencyKey_CompletesOnlyAfterNextFlush()
    {
        var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var messages = new Messages(states, new TimeoutsManager(), Now);
        messages.Initialize();
        var original = messages.Deliver("hello", idempotencyKey: "key", timeout: null);
        var duplicate = messages.Deliver("hello", idempotencyKey: "key", timeout: null);

        Assert.IsFalse(original.IsCompleted);
        Assert.IsFalse(duplicate.IsCompleted);

        await states.Flush();
        await original;
        await duplicate;
    }

    [TestMethod]
    public async Task Deliver_DuplicateIdempotencyKey_FlushesWithinTimeout()
    {
        var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var messages = new Messages(states, new TimeoutsManager(), Now);
        messages.Initialize();
        _ = messages.Deliver("hello", idempotencyKey: "key", timeout: null);
        var duplicate = messages.Deliver("hello", idempotencyKey: "key", timeout: TimeSpan.FromMilliseconds(10));

        await duplicate.WaitAsync(TimeSpan.FromSeconds(5));
    }

    [TestMethod]
    public void Deliver_StoresIdempotencyKeyCreationTime_FromUtcNow()
    {
        var createdAt = new DateTime(2026, 10, 18, 12, 0, 0, DateTimeKind.Utc);
        var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var messages = new Messages(states, new TimeoutsManager(), utcNow: () => createdAt);
        messages.Initialize();
        _ = messages.Deliver("hello", idempotencyKey: "key", timeout: null);

        var stored = States.DeserializeToDictionary(states.GetBytes());

        Assert.AreEqual("key", stored[new ExecutionScopeId("-1.1.0.0")]);
        Assert.AreEqual(createdAt.Ticks, stored[new ExecutionScopeId("-1.1.0.1")]);
    }

    [TestMethod]
    public async Task Deliver_SkipsMessage_WhenFilterThrows()
    {
        var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var messages = new Messages(states, new TimeoutsManager(), Now);
        messages.Initialize();
        var messageId = new ExecutionScopeId("0");
        var subscription = messages.Subscribe(
            filter: m => int.Parse((string) m) > 0,
            messageId,
            timeoutId: messageId.CreateChild(0),
            timeout: null
        );

        _ = messages.Deliver("not a number", idempotencyKey: null, timeout: null);
        _ = messages.Deliver("5", idempotencyKey: null, timeout: null);

        Assert.AreEqual("5", await subscription.WaitAsync(TimeSpan.FromSeconds(5)));
        var inbox = States
            .DeserializeToDictionary(states.GetBytes())
            .Where(kv => kv.Key.Id.StartsWith("-1.0."))
            .Select(kv => kv.Value)
            .ToList();
        CollectionAssert.AreEqual(new object[] { "not a number" }, inbox);
    }

    [TestMethod]
    public async Task Deliver_IsHeldBack_WhileSubflowsAreSuspended()
    {
        var runningAndWaiting = new RunningAndWaiting();
        var states = new States(runningAndWaiting, NoSync, new TimeoutsManager());
        var messages = new Messages(states, new TimeoutsManager(), Now);
        messages.Initialize();
        runningAndWaiting.SubflowStarted();
        var messageId = new ExecutionScopeId("0");
        var subscription = messages.Subscribe(filter: _ => true, messageId, timeoutId: messageId.CreateChild(0), timeout: null);

        runningAndWaiting.Suspend();
        await runningAndWaiting.WaitForAllSuspended().WaitAsync(TimeSpan.FromSeconds(5));
        var bytesWhenSuspended = states.GetBytes();
        _ = messages.Deliver("hello", idempotencyKey: null, timeout: null);

        CollectionAssert.AreEqual(bytesWhenSuspended, states.GetBytes());
        Assert.IsFalse(subscription.IsCompleted);

        runningAndWaiting.Resume();
        Assert.AreEqual("hello", await subscription.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [TestMethod]
    public async Task Subscribe_PrefersMessageInInbox_OverExpiredTimeout()
    {
        for (var i = 0; i < 1_000; i++)
        {
            var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
            var messages = new Messages(states, new TimeoutsManager(), Now);
            messages.Initialize();
            _ = messages.Deliver("hello", idempotencyKey: null, timeout: null);

            var messageId = new ExecutionScopeId("0");
            var timeoutId = messageId.CreateChild(0);
            var subscription = messages.Subscribe(filter: _ => true, messageId, timeoutId, timeout: DateTime.UtcNow.AddMinutes(-1));

            Assert.AreEqual("hello", await subscription.WaitAsync(TimeSpan.FromSeconds(5)));
            var stored = States.DeserializeToDictionary(states.GetBytes());
            Assert.AreEqual("hello", stored[messageId]);
            Assert.IsFalse(stored.ContainsKey(timeoutId));
        }
    }

    [TestMethod]
    public async Task Subscribe_LeavesNoTimeoutRegistered_WhenMessageIsDeliveredConcurrently()
    {
        for (var i = 0; i < 1_000; i++)
        {
            var timeoutsManager = new TimeoutsManager();
            var states = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
            var messages = new Messages(states, timeoutsManager, Now);
            messages.Initialize();
            var messageId = new ExecutionScopeId("0");

            var subscription = Task.Run(() => messages.Subscribe(filter: _ => true, messageId, timeoutId: messageId.CreateChild(0), timeout: DateTime.UtcNow.AddHours(1)));
            _ = Task.Run(() => messages.Deliver("hello", idempotencyKey: null, timeout: null));

            Assert.AreEqual("hello", await subscription.WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.IsNull(timeoutsManager.MinimumTimeout);
        }
    }
}
