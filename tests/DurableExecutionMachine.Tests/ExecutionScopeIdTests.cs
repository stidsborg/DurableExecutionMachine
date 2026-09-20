namespace DurableExecutionMachine.Tests;

[TestClass]
public class ExecutionScopeIdTests
{
    [TestMethod]
    public void IsChild_IsTrue_ForDirectAndNestedChildren()
    {
        var id = new ExecutionScopeId("1");

        Assert.IsTrue(id.IsChild(new ExecutionScopeId("1.0")));
        Assert.IsTrue(id.IsChild(new ExecutionScopeId("1.0.3")));
    }

    [TestMethod]
    public void IsChild_IsFalse_ForSelfAndAncestors()
    {
        var id = new ExecutionScopeId("1.0");

        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1.0")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1")));
    }

    [TestMethod]
    public void IsChild_IsFalse_ForSiblingWithSamePrefix()
    {
        var id = new ExecutionScopeId("1");

        Assert.IsFalse(id.IsChild(new ExecutionScopeId("10")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("10.0")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("11")));
    }

    [TestMethod]
    public void IsChild_ForRoot_IsTrueForEverythingExceptRoot()
    {
        var root = new ExecutionScopeId("");

        Assert.IsTrue(root.IsChild(new ExecutionScopeId("0")));
        Assert.IsTrue(root.IsChild(new ExecutionScopeId("10.2")));
        Assert.IsFalse(root.IsChild(new ExecutionScopeId("")));
    }
}
