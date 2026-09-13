namespace DurableExecutionMachine.Tests;

[TestClass]
public class StatesTests
{
    [TestMethod]
    public void GetBytes_RoundTrips_ThroughDeserialize()
    {
        var states = new States();
        states.SetState(new ExecutionScopeId(""), "root", typeof(string), removeChildren: false);
        states.SetState(new ExecutionScopeId("0.1"), 42, typeof(int), removeChildren: false);
        states.SetState(new ExecutionScopeId("1"), [], removeChildren: false);

        var restored = States.Deserialize(states.GetBytes());

        CollectionAssert.AreEqual(states.GetBytes(), restored.GetBytes());
    }

    [TestMethod]
    public void GetBytes_RoundTrips_WhenEmpty()
    {
        var restored = States.Deserialize(new States().GetBytes());

        Assert.AreEqual(0, restored.GetBytes().Length);
    }
}
