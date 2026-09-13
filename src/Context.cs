namespace DurableExecutionMachine;

public class Context
{
    internal ExecutionScope Scope { get; }
    
    public Context(ExecutionScope scope)
    {
        Scope = scope;
    }

    public Task<T> Capture<T>(Func<Task<T>> work)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetChild(id);
            return await work();
        }
        
        return ExecuteWork();
    }

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter)
    {
        throw new NotImplementedException();
    }

    public async Task<T> Parallel<T>(Func<Task<T>> subTask)
    {
        throw new NotImplementedException();
    }
}
