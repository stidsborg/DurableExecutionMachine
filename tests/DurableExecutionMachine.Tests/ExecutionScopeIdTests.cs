namespace DurableExecutionMachine.Tests;

[TestClass]
public class ExecutionScopeIdTests
{
    [TestMethod]
    public void IsDescendant_IsTrue_ForChildrenAndGrandchildren()
    {
        var id = new ExecutionScopeId("1");

        Assert.IsTrue(id.IsDescendant(new ExecutionScopeId("1.0")));
        Assert.IsTrue(id.IsDescendant(new ExecutionScopeId("1.0.3")));
    }

    [TestMethod]
    public void IsDescendant_IsFalse_ForSelfAndAncestors()
    {
        var id = new ExecutionScopeId("1.0");

        Assert.IsFalse(id.IsDescendant(new ExecutionScopeId("1.0")));
        Assert.IsFalse(id.IsDescendant(new ExecutionScopeId("1")));
    }

    [TestMethod]
    public void IsDescendant_IsFalse_ForSiblingWithSamePrefix()
    {
        var id = new ExecutionScopeId("1");

        Assert.IsFalse(id.IsDescendant(new ExecutionScopeId("10")));
        Assert.IsFalse(id.IsDescendant(new ExecutionScopeId("10.0")));
        Assert.IsFalse(id.IsDescendant(new ExecutionScopeId("11")));
    }

    [TestMethod]
    public void IsDescendant_ForRoot_IsTrueForEverythingExceptRoot()
    {
        var root = new ExecutionScopeId("");

        Assert.IsTrue(root.IsDescendant(new ExecutionScopeId("0")));
        Assert.IsTrue(root.IsDescendant(new ExecutionScopeId("10.2")));
        Assert.IsFalse(root.IsDescendant(new ExecutionScopeId("")));
    }
}
