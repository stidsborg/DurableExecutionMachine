using DurableExecutionMachine.Runtime;

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

    [TestMethod]
    public void IsChild_IsTrue_ForChildrenOnly()
    {
        var id = new ExecutionScopeId("1");

        Assert.IsTrue(id.IsChild(new ExecutionScopeId("1.0")));
        Assert.IsTrue(id.IsChild(new ExecutionScopeId("1.12")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1.0.3")));
    }

    [TestMethod]
    public void IsChild_IsFalse_ForSelfAncestorsAndSiblingsWithSamePrefix()
    {
        var id = new ExecutionScopeId("1.0");

        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1.0")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1.01")));
        Assert.IsFalse(id.IsChild(new ExecutionScopeId("1.01.2")));
    }

    [TestMethod]
    public void IsChild_ForRoot_IsTrueForTopLevelIdsOnly()
    {
        var root = new ExecutionScopeId("");

        Assert.IsTrue(root.IsChild(new ExecutionScopeId("0")));
        Assert.IsTrue(root.IsChild(new ExecutionScopeId("-1")));
        Assert.IsFalse(root.IsChild(new ExecutionScopeId("10.2")));
        Assert.IsFalse(root.IsChild(new ExecutionScopeId("")));
    }

    [TestMethod]
    public void Parent_DropsLastSegment()
    {
        Assert.AreEqual(new ExecutionScopeId("1.0"), new ExecutionScopeId("1.0.3").Parent);
        Assert.AreEqual(new ExecutionScopeId("-1.1"), new ExecutionScopeId("-1.1.12").Parent);
    }

    [TestMethod]
    public void Parent_OfTopLevelIdAndRoot_IsRoot()
    {
        Assert.AreEqual(ExecutionScopeId.Root, new ExecutionScopeId("10").Parent);
        Assert.AreEqual(ExecutionScopeId.Root, new ExecutionScopeId("-1").Parent);
        Assert.AreEqual(ExecutionScopeId.Root, ExecutionScopeId.Root.Parent);
    }
}
