namespace DurableExecutionMachine;

public class TimeoutsManager : IDisposable
{
    private readonly Lock _lock = new Lock();
    private readonly Dictionary<ExecutionScopeId, RegisteredTimeout> _timeouts = new();
    private bool _disposed;
    
    private record RegisteredTimeout(DateTime Timeout, TaskCompletionSource Tcs);

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
    
    public void NotifyExpiredTimeouts()
    {
        var now = DateTime.UtcNow;
        List<RegisteredTimeout> expired;
        lock (_lock)
        {
            if (_disposed)
                return;
            
            expired = new List<RegisteredTimeout>();
            foreach (var (id, registeredTimeout) in _timeouts.ToList())
                if (registeredTimeout.Timeout <= now)
                {
                    expired.Add(registeredTimeout);
                    _timeouts.Remove(id);
                }
        }

        foreach (var registeredTimeout in expired)
            registeredTimeout.Tcs.SetResult();
    }

    public void NotifyTimeoutExpired(ExecutionScopeId id)
    {
        RegisteredTimeout? registeredTimeout;
        lock (_lock)
        {
            _timeouts.TryGetValue(id, out registeredTimeout);
            _timeouts.Remove(id);
        }
        
        registeredTimeout?.Tcs.SetResult();;
    }

    public void CancelTimeout(ExecutionScopeId id)
    {
        lock (_lock)
            _timeouts.Remove(id);
    }
    
    public Task RegisterTimeout(ExecutionScopeId id, DateTime timeout)
    {
        var tcs = new TaskCompletionSource();
        lock (_lock)
        {
            if (!MinimumTimeout.HasValue || timeout < MinimumTimeout.Value)
                Task.Delay((timeout - DateTime.UtcNow).ZeroIfNegative())
                    .ContinueWith(_ => NotifyExpiredTimeouts());

            _timeouts[id] = new RegisteredTimeout(timeout, tcs);
        }
        
        return tcs.Task;
    }

    public void Dispose()
    {
        lock (_lock)
            _disposed = true;
    }
}
