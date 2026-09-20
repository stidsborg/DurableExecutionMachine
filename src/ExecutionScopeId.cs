namespace DurableExecutionMachine;

public record ExecutionScopeId(string Id)
{
    public bool IsChild(ExecutionScopeId childId) 
        => Id == "" ? childId.Id != "" : childId.Id.StartsWith(Id + ".");
}
