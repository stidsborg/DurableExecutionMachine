namespace DurableExecutionMachine;

public class Context
{
    private readonly AsyncGate _gate;
    private readonly States _states;
    internal ExecutionScope Scope { get; }
    
    public Context(ExecutionScope scope, AsyncGate gate, States states)
    {
        _gate = gate;
        _states = states;
        Scope = scope;
    }

    public Task<T> Capture<T>(Func<Task<T>> work, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetChild(id);
            var result = await work();
            if (flush)
                _states.SetState(id, result!, result!.GetType(), removeChildren: true);

            return result;
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
                var result = await subTask();
                if (flush)
                    _states.SetState(id, result!, result!.GetType(), removeChildren: true);
                return result;
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
