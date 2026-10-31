using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;
using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Flows;

public class Context
{
    private readonly States _states;
    private readonly Messages _messages;
    private readonly RunningAndWaiting _runningAndWaiting;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly UtcNow _utcNow;
    
    internal ExecutionScope Scope { get; }
    internal Messages Messages => _messages;
    
    public Context(ExecutionScope scope, States states, Messages messages, RunningAndWaiting runningAndWaiting, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        _states = states;
        Scope = scope;
        _messages = messages;
        _runningAndWaiting = runningAndWaiting;
        _timeoutsManager = timeoutsManager;
        _utcNow = utcNow;
    }

    public Task<T> Capture<T>(Func<Task<T>> work, bool flush = true)
    {
        var id = Scope.GetNextId();
        
        async Task<T> ExecuteWork()
        {
            Scope.SetParent(id);

            var prevResult = _states.Deserialize(id, out var hasPrevResult);
            if (hasPrevResult)
                return prevResult is FatalWorkflowException fatal ? throw fatal : (T)prevResult!;

            try
            {
                var result = await work();
                if (result == null)
                    _states.SetNull(id, removeDescendants: true);
                else
                    _states.SetState(id, result, result.GetType(), removeDescendants: true);

                if (flush)
                    await _states.Flush();
                
                return result;
            }
            catch (FatalWorkflowException e)
            {
                _states.SetException(id, e, removeDescendants: true);
                throw;
            }
            catch (Exception e)
            {
                var workflowException = FatalWorkflowException.CreateNonGeneric(e);
                _states.SetException(id, workflowException, removeDescendants: true);
                throw workflowException;
            }
        }
        
        return ExecuteWork();
    }
    
    public Task<T> Capture<T>(Func<T> work, bool flush = true)
        => Capture(() => Task.FromResult(work()), flush);
    
    public async Task Delay(TimeSpan delay)
    {
        var timeoutId = Scope.GetNextId();
        var timeoutAt = _states
            .Deserialize(timeoutId, out var hasTimeout)
            .CastTo<DateTime>();

        if (!hasTimeout)
        {
            timeoutAt = _utcNow() + delay;
            _states.SetState(timeoutId, timeoutAt, typeof(DateTime), removeDescendants: false);           
        }

        var expired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _timeoutsManager.RegisterTimeout(timeoutId, timeoutAt, () => expired.TrySetResult());
        _runningAndWaiting.SubflowSuspended();
        await expired.Task;
        await _runningAndWaiting.ResumeSubflow();
    }
    
    private async Task<T> Parallel<T>(Func<Task<T>> subTask, bool flush = true)
    {
        _runningAndWaiting.SubflowStarted();
        try
        {
            return await Capture(async () =>
            {
                await _runningAndWaiting.SuspendIfNeeded();
                return await subTask();
            }, flush);
        }
        finally
        {
            _runningAndWaiting.SubflowCompleted();
        }
    }

    public Task<T[]> WhenAll<T>(IEnumerable<Func<Task<T>>> children, bool flush = true)
        => Capture(async () =>
        {
            var tasks = children.Select(child => Parallel(child, flush)).ToArray();

            // the parent counts as suspended while it waits, so a sync can complete while the children are parked
            _runningAndWaiting.SubflowSuspended();
            try
            {
                return await Task.WhenAll(tasks);
            }
            finally
            {
                await _runningAndWaiting.ResumeSubflow();
            }
        }, flush);

    // without a timeout the flow waits until a matching message arrives, so the result is never null
    public async Task<TMessage> Message<TMessage>() where TMessage : class
        => (await WaitForMessage<TMessage>(filter: null, timeout: null))!;

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter) where TMessage : class
        => (await WaitForMessage(filter, timeout: null))!;

    // with a timeout the result is null when no matching message arrives in time
    public Task<TMessage?> Message<TMessage>(TimeSpan timeout) where TMessage : class
        => WaitForMessage<TMessage>(filter: null, timeout);

    public Task<TMessage?> Message<TMessage>(Func<TMessage, bool> filter, TimeSpan timeout) where TMessage : class
        => WaitForMessage(filter, timeout);

    private async Task<TMessage?> WaitForMessage<TMessage>(Func<TMessage, bool>? filter, TimeSpan? timeout) where TMessage : class
    {
        var messageId = Scope.GetNextId();
        var timeoutId = messageId.CreateChild(0);
        
        var existingMessage = _states.Deserialize(messageId, out var hasMessage);
        if (hasMessage)
            return (TMessage?)existingMessage;

        return (TMessage?) await _messages.Subscribe(
            msg => msg is TMessage t && (filter == null || filter(t)),
            messageId,
            timeoutId,
            timeout == null ? null : _utcNow().Add(timeout.Value)
        );
    }
}
