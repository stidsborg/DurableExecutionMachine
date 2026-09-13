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

    [TestMethod]
    public async Task DeserializeToDictionary_ReturnsCapturedValues()
    {
        var states = new States();
        var ctx = new Context(new ExecutionScope(), new AsyncGate(), states);

        await ctx.Capture(() => Task.FromResult("hello"));
        await ctx.Capture(() => Task.FromResult(42));
        await ctx.Capture(() => Task.FromResult(new Order("abc", Quantity: 3)));

        var dictionary = States.DeserializeToDictionary(states.GetBytes());

        Assert.AreEqual(3, dictionary.Count);
        Assert.AreEqual("hello", dictionary[new ExecutionScopeId("0")]);
        Assert.AreEqual(42, dictionary[new ExecutionScopeId("1")]);
        Assert.AreEqual(new Order("abc", Quantity: 3), dictionary[new ExecutionScopeId("2")]);
    }

    private record Order(string Id, int Quantity);
}
