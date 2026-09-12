namespace DurableExecutionMachine;

public class Context
{
    private readonly ExecutionScope _scope;
    private readonly TextWriter _output;

    public Context(ExecutionScope scope, TextWriter? output = null)
    {
        _scope = scope;
        _output = output ?? Console.Out;
    }

    public Task<T> Capture<T>(Func<Task<T>> work)
    {
        var id = _scope.GetNextId();
        _output.WriteLine("Id: " + id + " - Parent: " + _scope);
        
        async Task<T> ExecuteWork()
        {
            _scope.SetChild(id);
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
