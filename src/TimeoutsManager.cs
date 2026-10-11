namespace DurableExecutionMachine;

public class TimeoutsManager : IDisposable
{
    private readonly Lock _lock = new();
    private readonly Dictionary<ExecutionScopeId, RegisteredTimeout> _timeouts = new();
    private readonly Timer _timer;
    private bool _disposed;
    
    private record RegisteredTimeout(DateTime Timeout, Action Callback);

    public TimeoutsManager()
        => _timer = new Timer(_ => NotifyExpiredTimeouts(), state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

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

            if (MinimumTimeout is { } nextTimeout)
                ScheduleWakeUp(nextTimeout);
        }

        foreach (var registeredTimeout in expired)
            registeredTimeout.Callback();
    }
    
    public void CancelTimeout(ExecutionScopeId id)
    {
        lock (_lock)
            _timeouts.Remove(id);
    }
    
    public void RegisterTimeout(ExecutionScopeId id, DateTime timeout, Action callback)
    {
        lock (_lock)
        {
            if (!MinimumTimeout.HasValue || timeout < MinimumTimeout.Value)
                ScheduleWakeUp(timeout);

            _timeouts[id] = new RegisteredTimeout(timeout, callback);
        }
    }

    private void ScheduleWakeUp(DateTime timeout)
    {
        var delay = (timeout - DateTime.UtcNow)
            .ZeroIfNegative()
            .CapAtOneDay();
        
        _timer.Change(delay, Timeout.InfiniteTimeSpan);
    }

    public void Dispose()
    {
        lock (_lock)
            _disposed = true;

        _timer.Dispose();
    }
}
