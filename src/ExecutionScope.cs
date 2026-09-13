namespace DurableExecutionMachine;

public class ExecutionScope
{
    private readonly AsyncLocal<ExecutionScopeId> _parent =  new();
    private readonly AsyncLocal<int> _nextId = new();
    
    public ExecutionScopeId GetNextId() => 
        new(_parent.Value is null ? (_nextId.Value++).ToString() : _parent.Value.Id + "." + _nextId.Value++);

    public void SetChild(ExecutionScopeId parentId)
    {
        _parent.Value = parentId;
        _nextId.Value = 0;
    }
    
    public override string ToString() => _parent.Value?.Id ?? "";
}
