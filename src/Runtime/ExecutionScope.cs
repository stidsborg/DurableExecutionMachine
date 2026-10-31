namespace DurableExecutionMachine.Runtime;

internal class ExecutionScope
{
    // shared by reference so ids handed out inside a nested async method are visible to its caller,
    // which a plain AsyncLocal<ExecutionScopeId> would lose when the nested method returns
    private readonly AsyncLocal<Current> _current = new();

    public void SetRoot() => _current.Value = new(ExecutionScopeId.Root, CancellationToken.None);

    // a child scope is cancelled when its parent is
    public void SetParent(ExecutionScopeId parentId) => _current.Value = new(parentId, CancellationToken);

    public ExecutionScopeId GetNextId()
    {
        var current = _current.Value!;
        return current.ScopeId.CreateChild(current.NextChildId++);
    }

    // durable waits started in the current scope stop waiting when this token is cancelled
    public CancellationToken CancellationToken
    {
        get => _current.Value?.CancellationToken ?? CancellationToken.None;
        set => _current.Value!.CancellationToken = value;
    }

    public override string ToString() => _current.Value!.ScopeId.Id;

    private sealed class Current(ExecutionScopeId scopeId, CancellationToken cancellationToken)
    {
        public readonly ExecutionScopeId ScopeId = scopeId;
        public long NextChildId;
        public CancellationToken CancellationToken = cancellationToken;
    }
}
