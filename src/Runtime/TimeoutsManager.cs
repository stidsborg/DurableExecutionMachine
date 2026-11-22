using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

// a fired timeout stays registered until its waiter cancels it after resuming,
// so MinimumTimeout never misses a timeout whose waiter has not yet run again
internal class TimeoutsManager : IDisposable
{
    private readonly Lock _lock = new();
    private readonly Dictionary<ExecutionScopeId, RegisteredTimeout> _timeouts = new();
    private readonly Timer _timer;
    private bool _disposed;

    private record RegisteredTimeout(DateTime Timeout, Action Callback, bool Fired = false);

    public TimeoutsManager()
        => _timer = new Timer(_ => NotifyExpiredTimeouts(), state: null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

    // includes fired timeouts, whose waiters are about to run again
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

    // the earliest timeout that has not fired yet, which the timer is scheduled for
    private DateTime? NextWakeUp
    {
        get
        {
            lock (_lock)
            {
                var pending = _timeouts.Values.Where(t => !t.Fired).ToList();
                return pending.Count != 0
                    ? pending.Select(t => t.Timeout).Min()
                    : null;
            }
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
                if (!registeredTimeout.Fired && registeredTimeout.Timeout <= now)
                {
                    expired.Add(registeredTimeout);
                    _timeouts[id] = registeredTimeout with { Fired = true };
                }

            if (NextWakeUp is { } nextTimeout)
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
            if (!NextWakeUp.HasValue || timeout < NextWakeUp.Value)
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
