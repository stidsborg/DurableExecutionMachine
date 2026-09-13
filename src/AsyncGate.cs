namespace DurableExecutionMachine;

public sealed class AsyncGate
{
    private bool _raised = true;
    private readonly Lock _lock = new();

    private readonly Dictionary<ExecutionScopeId, TaskCompletionSource?> _executions = new();
    private TaskCompletionSource? _allWaiting;
    
    public void Raise()
    {
        List<TaskCompletionSource> waitingTcs;
        lock (_lock)
        {
            if (_raised)
                return;
            
            _raised = true;
            _allWaiting = null;
            
            waitingTcs = _executions.Values.OfType<TaskCompletionSource>().ToList();
            foreach (var id in _executions.Keys.ToList())
                _executions[id] = null;
        }

        foreach (var tcs in waitingTcs)
            tcs.TrySetResult();
    }
    
    public void Lower()
    {
        lock (_lock)
            _raised = false;
    }

    public Task Start(ExecutionScopeId id)
    {
        TaskCompletionSource? tcs;
        lock (_lock)
        {
            if (_raised)
                tcs = _executions[id] = null;
            else
                tcs = _executions[id] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        return tcs?.Task ?? Task.CompletedTask;
    }

    public void Complete(ExecutionScopeId id)
    {
        lock (_lock)
        {
            _executions.Remove(id);
            if (_allWaiting is not null && _executions.Values.All(t => t is not null))
                _allWaiting.TrySetResult();
        }
    }
 
    public Task WaitAsync(ExecutionScopeId id)
    {
        lock (_lock)
        {
            if (_raised)
                return Task.CompletedTask;
            
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _executions[id] = tcs;

            if (_allWaiting is not null && _executions.Values.All(t => t is not null))
                _allWaiting.TrySetResult();
                
            return tcs.Task;
        }
    }

    public Task WaitForAllWaitingAsync()
    {
        lock (_lock)
        {
            if (_raised)
                throw new InvalidOperationException("Cannot wait for all waiting on raised gate");

            var isAllWaiting = _executions.Values.All(tcs => tcs is not null);
            if (isAllWaiting)    
                return Task.CompletedTask;

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _allWaiting = tcs;
            return tcs.Task;
        }
    }
}