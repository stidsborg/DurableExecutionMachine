namespace DurableExecutionMachine;

public record ExecutionScopeId(string Id)
{
    public bool IsChild(ExecutionScopeId childId) 
        => Id == "" ? childId.Id != "" : childId.Id.StartsWith(Id + ".");
    
    public ExecutionScopeId CreateChild(long childId) 
        => new(Id == "" ? childId.ToString() : Id + "." + childId);

    public long Position => long.Parse(Id[(Id.LastIndexOf('.') + 1)..]);
}
