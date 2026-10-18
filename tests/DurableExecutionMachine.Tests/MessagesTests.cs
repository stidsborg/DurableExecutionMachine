namespace DurableExecutionMachine.Tests;

[TestClass]
public class MessagesTests
{
    private static readonly SyncCallback NoSync = (_, _) => Task.CompletedTask;

    [TestMethod]
    public void Deliver_IgnoresDuplicateIdempotencyKey_AfterRestart()
    {
        var firstStates = new States(new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var firstRun = new Messages(firstStates, new TimeoutsManager());
        firstRun.Initialize();
        _ = firstRun.Deliver("hello", idempotencyKey: "key", timeout: null);

        var restartedStates = States.Deserialize(firstStates.GetBytes(), new RunningAndWaiting(), NoSync, new TimeoutsManager());
        var restarted = new Messages(restartedStates, new TimeoutsManager());
        restarted.Initialize();
        var duplicate = restarted.Deliver("hello", idempotencyKey: "key", timeout: null);

        Assert.IsTrue(duplicate.IsCompleted);
        var storedMessages = States
            .DeserializeToDictionary(restartedStates.GetBytes())
            .Keys
            .Count(id => id.Id.StartsWith("-1.0."));
        Assert.AreEqual(1, storedMessages);
    }
}
