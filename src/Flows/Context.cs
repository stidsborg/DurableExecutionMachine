using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Flows;

public class Context
{
    private readonly States _states;
    private readonly Messages _messages;
    private readonly SuspensionBarrier _suspensionBarrier;
    private readonly UtcNow _utcNow;
    
    internal ExecutionScope Scope { get; }
    internal Messages Messages => _messages;
    
    internal Context(ExecutionScope scope, States states, Messages messages, SuspensionBarrier suspensionBarrier, UtcNow utcNow)
    {
        _states = states;
        Scope = scope;
        _messages = messages;
        _suspensionBarrier = suspensionBarrier;
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
                return prevResult is FatalWorkflowException fatal ? throw fatal.ToThrown() : (T)prevResult!;

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
                // a cancellation is stored like any other failure, but thrown as the cancellation it is (see ToThrown)
                var workflowException = FatalWorkflowException.CreateNonGeneric(e);
                _states.SetException(id, workflowException, removeDescendants: true);
                if (e.GetType() == typeof(OperationCanceledException))
                    throw;
                throw workflowException;
            }
        }
        
        return ExecuteWork();
    }
    
    public Task<T> Capture<T>(Func<T> work, bool flush = true)
        => Capture(() => Task.FromResult(work()), flush);
    
    // cancellationToken is meant for a token handed out by WhenAny, so a losing child stops waiting. its cancellation is not stored:
    // on replay the stored winner cancels the losers again. any other token would make a replay take a different path
    public async Task Delay(TimeSpan delay, CancellationToken cancellationToken = default)
    {
        var timeoutId = Scope.GetNextId();
        cancellationToken.ThrowIfCancellationRequested();

        if (!_states.TryGet(timeoutId, out DateTime timeoutAt))
        {
            timeoutAt = _utcNow() + delay;
            _states.SetState(timeoutId, timeoutAt, typeof(DateTime), removeDescendants: false);           
        }

        var cancelled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        // failed rather than cancelled, so awaiting it throws an OperationCanceledException and not its subtype TaskCanceledException
        await using var cancellation = cancellationToken.Register(() => cancelled.TrySetException(new OperationCanceledException(cancellationToken)));
        await _suspensionBarrier.SuspendedUntil(cancelled.Task, timeoutId, timeoutAt);
    }
    
    private async Task<T> Parallel<T>(Func<Task<T>> subTask, bool flush = true)
    {
        _suspensionBarrier.Enter();
        try
        {
            return await Capture(async () =>
            {
                await _suspensionBarrier.SuspendIfNeeded();
                return await subTask();
            }, flush);
        }
        finally
        {
            _suspensionBarrier.Leave();
        }
    }

    public Task<T[]> WhenAll<T>(IEnumerable<Func<Task<T>>> children, bool flush = true)
        => Capture(async () =>
        {
            var tasks = children.Select(child => Parallel(child, flush)).ToArray();

            // the parent counts as suspended while it waits, so a sync can complete while the children are parked
            return await _suspensionBarrier.SuspendedWhile(Task.WhenAll(tasks));
        }, flush);

    // the first child to finish wins, whether it succeeds or fails. the others are cancelled and still joined, so a child must pass
    // its token to its waits or the join blocks until they complete. cancellationToken is the token of an outer WhenAny, if any
    public Task<T> WhenAny<T>(IEnumerable<Func<CancellationToken, Task<T>>> children, bool flush = true, CancellationToken cancellationToken = default)
    {
        var childArray = children.ToArray();

        // checked before the capture, so the mistake is thrown on every run instead of being stored as the flow's failure
        if (childArray.Length == 0)
            throw new ArgumentException("WhenAny needs at least one child", nameof(children));

        return Capture(async () =>
        {
            var winnerIndexId = Scope.GetNextId();
            var hasWinner = _states.TryGet(winnerIndexId, out int winnerIndex);

            // linked, so cancelling an outer WhenAny reaches this one's children, but not the other way round
            using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

            // on replay the winner is already known, so the losers start cancelled and stop at their next wait
            if (hasWinner)
                await cancellation.CancelAsync();

            var tasks = childArray
                .Select(child => Parallel(() => child(cancellation.Token), flush))
                .ToArray();
            
            if (!hasWinner)
            {
                var first = await _suspensionBarrier.SuspendedWhile(Task.WhenAny(tasks));
                winnerIndex = Array.IndexOf(tasks, first);

                // stored before the losers are cancelled, so a restart during the join cancels them again
                _states.SetState(winnerIndexId, winnerIndex, typeof(int), removeDescendants: false);
            }

            await cancellation.CancelAsync();
            try
            {
                await _suspensionBarrier.SuspendedWhile(Task.WhenAll(tasks));
            }
            catch
            {
                // the losers' results and exceptions are thrown away
            }

            return await tasks[winnerIndex];
        }, flush);
    }

    // without a timeout the flow waits until a matching message arrives, so the result is never null.
    // cancellationToken is meant for a token handed out by WhenAny, as for Delay
    public async Task<TMessage> Message<TMessage>(CancellationToken cancellationToken = default) where TMessage : class
        => (await WaitForMessage<TMessage>(filter: null, timeout: null, cancellationToken))!;

    public async Task<TMessage> Message<TMessage>(Func<TMessage, bool> filter, CancellationToken cancellationToken = default) where TMessage : class
        => (await WaitForMessage(filter, timeout: null, cancellationToken))!;

    // with a timeout the result is null when no matching message arrives in time
    public Task<TMessage?> Message<TMessage>(TimeSpan timeout, CancellationToken cancellationToken = default) where TMessage : class
        => WaitForMessage<TMessage>(filter: null, timeout, cancellationToken);

    public Task<TMessage?> Message<TMessage>(Func<TMessage, bool> filter, TimeSpan timeout, CancellationToken cancellationToken = default) where TMessage : class
        => WaitForMessage(filter, timeout, cancellationToken);

    private async Task<TMessage?> WaitForMessage<TMessage>(Func<TMessage, bool>? filter, TimeSpan? timeout, CancellationToken cancellationToken) where TMessage : class
    {
        var messageId = Scope.GetNextId();
        var timeoutId = messageId.CreateChild(0);
        
        if (_states.TryGet(messageId, out TMessage? existingMessage))
            return existingMessage;

        return (TMessage?) await _messages.Subscribe(
            msg => msg is TMessage t && (filter == null || filter(t)),
            messageId,
            timeoutId,
            timeout == null ? null : _utcNow().Add(timeout.Value),
            cancellationToken
        );
    }
}
