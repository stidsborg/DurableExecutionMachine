namespace DurableExecutionMachine;

public class TimeoutsManager : IDisposable
{
    private readonly Lock _lock = new Lock();
    private readonly Dictionary<ExecutionScopeId, RegisteredTimeout> _timeouts = new();
    
    private record RegisteredTimeout(DateTime Timeout, Action Callback, CancellationTokenSource Cts);

    public DateTime? MinimumTimeout
    {
        get
        {
            lock (_lock)
                return _timeouts.Count != 0
                    ? _timeouts.Values.Select(t => t.Timeout).Min()
                    : null;
        }
    }

    public void NotifyTimeoutExpired(ExecutionScopeId id)
    {
        RegisteredTimeout? registeredTimeout;
        lock (_lock)
        {
            _timeouts.TryGetValue(id, out registeredTimeout);
            _timeouts.Remove(id);
        }
        
        registeredTimeout?.Callback.Invoke();
    }

    public void CancelTimeout(ExecutionScopeId id)
    {
        lock (_lock)
            _timeouts.Remove(id);
    }
    
    public void RegisterTimeout(ExecutionScopeId id, DateTime timeout, Action timeoutCallback)
    {
        var cts = new CancellationTokenSource();
        lock (_lock)
            _timeouts[id] = new RegisteredTimeout(timeout, timeoutCallback, cts);

        var delay = timeout - DateTime.UtcNow;
        delay = delay < TimeSpan.Zero ? TimeSpan.Zero : delay;

        _ = Task.Delay(delay, cts.Token).ContinueWith(
            _ => NotifyTimeoutExpired(id),
            TaskContinuationOptions.OnlyOnRanToCompletion
        ); 
    }

    public void Dispose()
    {
        List<CancellationTokenSource> ctss;
        lock (_lock)
            ctss = _timeouts.Values.Select(t => t.Cts).ToList();

        foreach (var cts in ctss)
            cts.Cancel();
    }
}
