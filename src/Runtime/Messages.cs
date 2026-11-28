using DurableExecutionMachine.Storage;
using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Runtime;

internal class Messages
{
    private readonly States _states;
    private readonly TimeoutsManager _timeoutsManager;
    private readonly UtcNow _utcNow;
    private readonly SortedDictionary<int, object> _inbox = new();
    private readonly Dictionary<ExecutionScopeId, Subscription> _subscriptions = new();

    private readonly ExecutionScopeId _rootExecutionScope = new("-1");
    private readonly ExecutionScopeId _messagesExecutionScope;
    private readonly ExecutionScopeId _idempotencyScope;
    private readonly ExecutionScopeId _nextMessageIdScope;
    
    private readonly Dictionary<string, long> _idempotencyKeys = new();
    private int _nextMessageId;
    private readonly Lock _lock = new();
    
    public Messages(States states, TimeoutsManager timeoutsManager, UtcNow utcNow)
    {
        _states = states;
        _timeoutsManager = timeoutsManager;
        _utcNow = utcNow;
        
        _messagesExecutionScope = _rootExecutionScope.CreateChild(0);
        _idempotencyScope = _rootExecutionScope.CreateChild(1);
        _nextMessageIdScope = _rootExecutionScope.CreateChild(2);
        
        _nextMessageId = _states.TryGet(_nextMessageIdScope, out int nextMessageId) ? nextMessageId : 0;
    }

    public void Initialize()
    {
        lock (_lock)
        {
            // messages still in the messages scope were not delivered to a subscription before the previous run ended
            foreach (var messageId in _states.GetDescendants(_messagesExecutionScope))
            {
                var message = _states.Deserialize(messageId, out _)!;
                _inbox[(int) messageId.ChildId] = message;
            }
            
            foreach (var idempotencyValueId in _states.GetDescendants(_idempotencyScope).Where(id => id.ChildId == 0))
            {
                var idempotencyKey = (string) _states.Deserialize(idempotencyValueId, out _)!;
                var createdAtId = idempotencyValueId.Parent.CreateChild(1);
                var createdAt = (long) _states.Deserialize(createdAtId, out _)!;
                _idempotencyKeys[idempotencyKey] = createdAt;
            }
        }
    }
    
    public async Task<object?> Subscribe(
        Func<object, bool> filter, 
        ExecutionScopeId messageId, 
        ExecutionScopeId timeoutId, 
        DateTime? timeout,
        CancellationToken cancellationToken = default)
    {
        if (_states.TryGet(messageId, out object? message))
            return message;

        cancellationToken.ThrowIfCancellationRequested();

        var tcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
   
        var timeoutExists = _states.TryGet(timeoutId, out long existingTimeout);
        DateTime? timeoutAt = timeoutExists
            ? new DateTime(existingTimeout, DateTimeKind.Utc)
            : timeout?.ToUniversalTime();
        if (!timeoutExists && timeoutAt is not null)
            _states.TryAddState(timeoutId, timeoutAt.Value.Ticks, typeof(long), removeDescendants: false);
        
        lock (_lock)
            _subscriptions[messageId] = new Subscription(filter, tcs, messageId, timeoutId);
        
        // deliver before counting as suspended so the subscription's writes to states happen before a sync can observe all subflows suspended
        TryToDeliver();
        
        await using var cancellation = cancellationToken.Register(() =>
        {
            lock (_lock)
            {
                // the subscription is removed when a message is delivered to it or its timeout fires
                if (!_subscriptions.Remove(messageId))
                    return;

                _timeoutsManager.CancelTimeout(timeoutId);
                // failed rather than cancelled, so awaiting it throws an OperationCanceledException and not its subtype TaskCanceledException
                tcs.SetException(new OperationCanceledException(cancellationToken));
            }
        });

        if (timeoutAt is null)
            return await _states.SuspensionBarrier.SuspendedWhile(tcs.Task);

        if (!await _states.SuspensionBarrier.SuspendedUntil(tcs.Task, timeoutId, timeoutAt.Value))
            return await tcs.Task;

        lock (_lock)
        {
            // the subscription is removed by whoever resolves the wait first: a delivery, a cancellation or this timeout.
            // a message delivered after the timeout fired, but before this subflow ran again, wins
            if (!_subscriptions.Remove(messageId))
                return tcs.Task.GetAwaiter().GetResult();

            _states.SetNull(messageId, removeDescendants: false);
            return null;
        }
    }

    private void TryToDeliver()
    {
        lock (_lock)
        {
            foreach (var (number, message) in _inbox.ToList())
            foreach (var subscription in _subscriptions.Values.ToList())
            {
                // a throwing filter does not match the message, so the message stays in the inbox for other subscriptions
                if (!Safe.Try(() => subscription.Filter(message), onError: _ => false))
                    continue;

                // moving the message from the inbox to the subscription is atomic with respect to flushes.
                // the timeout cannot have resolved the subscription as it removes the subscription under the same lock
                _states.Lock(doWhileLocked: () =>
                {
                    _states.SetState(subscription.MessageId, message, message.GetType(), removeDescendants: false);
                    _states.Remove(_messagesExecutionScope.CreateChild(number));
                });

                _timeoutsManager.CancelTimeout(subscription.TimeoutId);
                _subscriptions.Remove(subscription.MessageId);
                _inbox.Remove(number);
                subscription.CompletionSource.SetResult(message);
                break;
            }
        }
    }

    // the message is in states when this returns, so every later flush or serialization of the machine contains it, in the inbox or at the wait it was handed to.
    // the returned task completes when such a flush completes. the delivery does not request one.
    // it does not wait for a suspension to be released: a wait it is handed to then continues when the machine is resumed
    public Task Deliver(object message, string? idempotencyKey)
    {
        var flushed = AddToInbox(message, idempotencyKey);
        TryToDeliver();
        return flushed;
    }

    // returns the next flush, which persists the message or, for a duplicate, the original message
    private Task AddToInbox(object message, string? idempotencyKey)
    {
        Task nextFlush = Task.CompletedTask;
        lock (_lock)
            _states.Lock(doWhileLocked: () =>
            {
                nextFlush = _states.WaitForNextFlush();

                // a duplicate waits for the next flush, as the original delivery might not have been flushed yet
                if (idempotencyKey != null && _idempotencyKeys.ContainsKey(idempotencyKey))
                    return;

                var number = _nextMessageId++;
                _states.SetState(_nextMessageIdScope, _nextMessageId, typeof(int), removeDescendants: false);

                if (idempotencyKey is not null)
                {
                    var createdAt = _utcNow().Ticks;
                    _idempotencyKeys[idempotencyKey] = createdAt;
                    SaveIdempotencyKey(number, idempotencyKey, createdAt);
                }

                var messageId = _messagesExecutionScope.CreateChild(number);
                _states.SetState(messageId, message, message.GetType(), removeDescendants: false);
                _inbox[number] = message;
            });

        return nextFlush;
    }

    private void SaveIdempotencyKey(int number, string idempotencyKey, long createdAt)
    {
        // each key is stored under <idempotency scope>.<message number> with the key at .0 and its creation ticks at .1
        var idempotencyKeyId = _idempotencyScope.CreateChild(number);
        _states.SetState(idempotencyKeyId.CreateChild(0), instance: idempotencyKey, typeof(string), removeDescendants: false);
        _states.SetState(idempotencyKeyId.CreateChild(1), instance: createdAt, typeof(long), removeDescendants: false);
    }
    
    private record Subscription(
        Func<object, bool> Filter,
        TaskCompletionSource<object?> CompletionSource,
        ExecutionScopeId MessageId,
        ExecutionScopeId TimeoutId
    );
}