namespace DurableExecutionMachine;

public record ExecutionScopeId(string Id)
{
    public bool IsDescendant(ExecutionScopeId descendantId) 
        => Id == "" ? descendantId.Id != "" : descendantId.Id.StartsWith(Id + ".");
    
    public ExecutionScopeId CreateChild(long childId) 
        => new(Id == "" ? childId.ToString() : Id + "." + childId);
}
