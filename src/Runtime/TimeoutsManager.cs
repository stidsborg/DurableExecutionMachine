using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

internal class TimeoutsManager : IDisposable
{
    private readonly Lock _lock = new();
    private readonly Dictionary<ExecutionScopeId, RegisteredTimeout> _timeouts = new();
    private readonly Timer _timer;
    private bool _disposed;
    
    // a waiter without a deadline is registered too, but never fires
    private record RegisteredTimeout(DateTime? Timeout, Action Callback);

    public TimeoutsManager()
        => _timer = new Timer(_ => NotifyExpiredTimeouts(), state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    public DateTime? MinimumTimeout
    {
        get
        {
            lock (_lock)
                return _timeouts.Values.Min(t => t.Timeout);
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
            foreach (var registeredTimeout in _timeouts.Values)
                if (registeredTimeout.Timeout <= now)
                    // the waiter removes its timeout once it is running again
                    expired.Add(registeredTimeout);

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
    
    public void RegisterTimeout(ExecutionScopeId id, DateTime? timeout, Action callback)
    {
        lock (_lock)
        {
            if (timeout is { } at && (MinimumTimeout is not { } minimum || at < minimum))
                ScheduleWakeUp(at);

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
