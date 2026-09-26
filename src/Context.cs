namespace DurableExecutionMachine;

public class Context
{
    private readonly AsyncGate _gate;
    private readonly States _states;
    private readonly Messages _messages;
    
    internal ExecutionScope Scope { get; }
    
    public Context(ExecutionScope scope, AsyncGate gate, States states, Messages messages)
    {
        _gate = gate;
        _states = states;
        Scope = scope;
        _messages = messages;
    }

    public Task<T> Capture<T>(Func<Task<T>> work, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetChild(id);
            var result = await work();
            CaptureFlushless(() => result);
            if (flush)
                await _states.Flush();

            return result;
        }
        
        return ExecuteWork();
    }
    
    public Task<T> Capture<T>(Func<T> work, bool flush = true)
        => Capture(() => Task.FromResult(work()), flush);

    public T CaptureFlushless<T>(Func<T> work)
    {
        var id = Scope.GetNextId();
        var (parent, nextId) = Scope.Current;
        Scope.SetChild(id);
        
        try
        {
            var prevResult = _states.Deserialize(id, out var hasPrevResult);
            if (hasPrevResult)
                return (T)prevResult!;
            
            var result = work();
            if (result == null)
                _states.SetNull(id, removeChildren: true);
            else
                _states.SetState(id, result, result.GetType(), removeChildren: true);

            return result;
        }
        catch (FatalWorkflowException e)
        {
            _states.SetException(id, e, removeChildren: true);
            throw;
        }
        catch (Exception e)
        {
            var workflowException = FatalWorkflowException.CreateNonGeneric(e);
            _states.SetException(id, workflowException, removeChildren: true);
            throw workflowException;
        }
        finally
        {
            Scope.Restore(parent, nextId);
        }
    }
    
    public Task Delay(TimeSpan delay)
    {
        var wakeUpAt = Capture(() => DateTime.UtcNow + delay);
        throw new NotImplementedException();
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

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter, TimeSpan? timeout = null)
    {
        throw new NotImplementedException();
    }
}
