namespace DurableExecutionMachine;

public class Context
{
    private readonly States _states;
    private readonly Messages _messages;
    private readonly RunningAndWaiting _runningAndWaiting;
    
    internal ExecutionScope Scope { get; }
    
    public Context(ExecutionScope scope, States states, Messages messages, RunningAndWaiting runningAndWaiting)
    {
        _states = states;
        Scope = scope;
        _messages = messages;
        _runningAndWaiting = runningAndWaiting;
    }

    public Task<T> Capture<T>(Func<Task<T>> work, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetParent(id);

            try
            {
                var prevResult = _states.Deserialize(id, out var hasPrevResult);
                if (hasPrevResult)
                    return (T)prevResult!;

                var result = await work();
                if (result == null)
                    _states.SetNull(id, removeChildren: true);
                else
                    _states.SetState(id, result, result.GetType(), removeChildren: true);

                if (flush)
                    await _states.Flush();
                
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
        }
        
        return ExecuteWork();
    }
    
    public Task<T> Capture<T>(Func<T> work, bool flush = true)
        => Capture(() => Task.FromResult(work()), flush);
    
    public Task Delay(TimeSpan delay)
    {
        /*
        var wakeUpAt = _states.SetState()
        var wakeUpIn = DateTime.UtcNow - wakeUpAt;
        if (wakeUpIn <= TimeSpan.Zero)
            return Task.CompletedTask;
        */
        throw new NotImplementedException();
    }
    
    
    
    public Task<T> Parallel<T>(Func<Task<T>> subTask, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetParent(id);
            _runningAndWaiting.SubflowStarted();
            await _runningAndWaiting.SuspendIfNeeded();
            try
            {
                var result = await subTask();
                if (flush)
                    _states.SetState(id, result!, result!.GetType(), removeChildren: true);
                return result;
            }
            finally
            {
                _runningAndWaiting.SubflowCompleted();
            }
        }
        
        return ExecuteWork();
    }

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter, TimeSpan? timeout = null)
    {
        throw new NotImplementedException();
    }
}
