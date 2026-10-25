namespace DurableExecutionMachine;

public class ExecutionScope
{
    // shared by reference so ids handed out inside a nested async method are visible to its caller,
    // which a plain AsyncLocal<ExecutionScopeId> would lose when the nested method returns
    private readonly AsyncLocal<Current> _current = new();

    public void SetRoot() => _current.Value = new(ExecutionScopeId.Root);

    public void SetParent(ExecutionScopeId parentId) => _current.Value = new(parentId);

    public ExecutionScopeId GetNextId()
    {
        var current = _current.Value!;
        return current.ScopeId.CreateChild(current.NextChildId++);
    }

    public override string ToString() => _current.Value!.ScopeId.Id;

    private sealed class Current(ExecutionScopeId scopeId)
    {
        public readonly ExecutionScopeId ScopeId = scopeId;
        public long NextChildId;
    }
}
