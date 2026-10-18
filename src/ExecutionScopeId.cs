namespace DurableExecutionMachine;

public record ExecutionScopeId(string Id)
{
    public static ExecutionScopeId Root { get; } = new(string.Empty);
    public bool IsRoot => Id == string.Empty;
    
    public ExecutionScopeId Parent
    {
        get
        {
            var lastDot = Id.LastIndexOf('.');
            return lastDot == -1 ? Root : new(Id[..lastDot]);
        }
    }
    
    public bool IsDescendant(ExecutionScopeId descendantId) 
        => Id == "" ? descendantId.Id != "" : descendantId.Id.StartsWith(Id + ".");
    
    public bool IsChild(ExecutionScopeId childId)
    {
        if (!IsDescendant(childId))
            return false;

        var start = Id == "" ? 0 : Id.Length + 1;
        return !childId.Id.AsSpan(start).Contains('.');
    }
    
    public ExecutionScopeId CreateChild(long childId) 
        => new(Id == "" ? childId.ToString() : Id + "." + childId);

    public long ChildId => long.Parse(Id[(Id.LastIndexOf('.') + 1)..]);
}
