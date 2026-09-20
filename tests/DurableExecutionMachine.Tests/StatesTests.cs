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
        var ctx = new Context(new ExecutionScope(), new AsyncGate(), states, messages: null!);

        await ctx.Capture(() => Task.FromResult("hello"));
        await ctx.Capture(() => Task.FromResult(42));
        await ctx.Capture(() => Task.FromResult(new Order("abc", Quantity: 3)));

        var dictionary = States.DeserializeToDictionary(states.GetBytes());

        Assert.AreEqual(3, dictionary.Count);
        Assert.AreEqual("hello", dictionary[new ExecutionScopeId("0")]);
        Assert.AreEqual(42, dictionary[new ExecutionScopeId("1")]);
        Assert.AreEqual(new Order("abc", Quantity: 3), dictionary[new ExecutionScopeId("2")]);
    }

    [TestMethod]
    public void RemoveChildren_RemovesDescendants_ButNotSiblingsWithSamePrefix()
    {
        var states = new States();
        foreach (var id in new[] { "1", "1.0", "1.0.0", "10", "10.0" })
            states.SetState(new ExecutionScopeId(id), id, typeof(string), removeChildren: false);

        states.RemoveChildren(new ExecutionScopeId("1"));

        var remaining = States.DeserializeToDictionary(states.GetBytes()).Keys.Select(k => k.Id).Order().ToList();
        CollectionAssert.AreEqual(new[] { "1", "10", "10.0" }, remaining);
    }

    private record Order(string Id, int Quantity);
}
