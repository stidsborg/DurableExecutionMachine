namespace DurableExecutionMachine;

public class ExecutionScope
{
    private readonly AsyncLocal<string> _parent =  new();
    private AsyncLocal<int> _nextId = new();
    
    public string GetNextId() => 
        _parent.Value is null ? (_nextId.Value++).ToString() : _parent.Value + "." + _nextId.Value++;

    public void SetChild(string parentId)
    {
        _parent.Value = parentId;
        _nextId.Value = 0;
    }
    
    public override string ToString() => _parent.Value ?? "";
}