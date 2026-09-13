namespace DurableExecutionMachine;

public class Context
{
    private readonly AsyncGate _gate;
    internal ExecutionScope Scope { get; }
    
    public Context(ExecutionScope scope, AsyncGate gate)
    {
        _gate = gate;
        Scope = scope;
    }

    public Task<T> Capture<T>(Func<Task<T>> work, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetChild(id);
            return await work();
        }
        
        return ExecuteWork();
    }
    
    public Task<T> Parallel<T>(Func<Task<T>> subTask, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetChild(id);
            await _gate.Start(id);
            try
            {
                return await subTask();
            }
            finally
            {
                _gate.Complete(id);
            }
        }
        
        return ExecuteWork();
    }

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter)
    {
        throw new NotImplementedException();
    }
}
