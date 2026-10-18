namespace DurableExecutionMachine.Tests;

[TestClass]
public class MessagesTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;
    private static readonly UtcNow Now = () => DateTime.UtcNow;

    [TestMethod]
    public void Deliver_IgnoresDuplicateIdempotencyKey_AfterRestart()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRun = new Messages(firstStates, new TimeoutsManager(), Now);
        firstRun.Initialize();
        _ = firstRun.Deliver("hello", idempotencyKey: "key", timeout: null);

        var restartedStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var restarted = new Messages(restartedStates, new TimeoutsManager(), Now);
        restarted.Initialize();
        var duplicate = restarted.Deliver("hello", idempotencyKey: "key", timeout: null);

        Assert.IsTrue(duplicate.IsCompleted);
        var storedMessages = States
            .DeserializeToDictionary(restartedStates.GetBytes())
            .Keys
            .Count(id => id.Id.StartsWith("-1.0."));
        Assert.AreEqual(1, storedMessages);
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
}
