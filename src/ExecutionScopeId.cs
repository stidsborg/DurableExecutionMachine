namespace DurableExecutionMachine;

public record ExecutionScopeId(string Id)
{
    public bool IsChild(ExecutionScopeId childId) 
        => childId.Id.StartsWith(Id) && childId.Id.Length > Id.Length;
}