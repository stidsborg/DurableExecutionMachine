namespace DurableExecutionMachine;

public class ExecutionScope(string parent, int nextId)
{
    private readonly AsyncLocal<Node> _current = new();

    public void SetRoot()
        => _current.Value = new Node(parent, nextId);

    public ExecutionScopeId GetNextId()
    {
        var active = _current.Value!;

        return active.ParentId == ""
            ? new("" + active.NextId++)
            : new(active.ParentId + "." + active.NextId++);
    }

    public void SetParent(ExecutionScopeId parentId)
        => _current.Value = new Node(parentId.Id, nextId: 0);

    public override string ToString() => _current.Value!.ParentId;

    private sealed class Node(string parentId, int nextId)
    {
        public readonly string ParentId = parentId;
        public int NextId = nextId;
    }
}
