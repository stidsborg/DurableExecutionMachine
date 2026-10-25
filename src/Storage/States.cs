using System.Runtime.ExceptionServices;
using System.Text.Json;
using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Runtime;
using DurableExecutionMachine.Serialization;
using DurableExecutionMachine.Utils;

namespace DurableExecutionMachine.Storage;

public class States(RunningAndWaiting runningAndWaiting, SyncCallback syncCallback, TimeoutsManager timeoutsManager)
{
    private readonly Dictionary<ExecutionScopeId, Lazy<byte[]>> _states = new();
    private static readonly Lazy<byte[]> Null = new(Array.Empty<byte>());
    private readonly Lock _lock = new();
    private readonly AsyncSignal _signal = new();
    private readonly SemaphoreSlim _flushSemaphore = new(1);
    private ExceptionDispatchInfo? _thrownException;
    
    internal RunningAndWaiting RunningAndWaiting { get; } = runningAndWaiting;
    internal SyncCallback SyncCallback { get; } = syncCallback;
    internal TimeoutsManager TimeoutsManager { get; } = timeoutsManager;

    public void Lock(Action doWhileLocked)
    {
        lock (_lock)
            doWhileLocked();
    }
    
    public void SetState(ExecutionScopeId id, object instance, Type instanceType, bool removeDescendants)
    {
        lock (_lock)
        {
            _states[id] = new Lazy<byte[]>(() => Serialize(instance, instanceType));
            if (removeDescendants)
                RemoveDescendants(id);
        }
    }
    
    public void SetNull(ExecutionScopeId id, bool removeDescendants)
    {
        lock (_lock)
        {
            _states[id] = Null;
            if (removeDescendants)
                RemoveDescendants(id);
        }
    }
    
    public bool TryAddState(ExecutionScopeId id, object instance, Type instanceType, bool removeDescendants)
        => TryAdd(id, new Lazy<byte[]>(() => Serialize(instance, instanceType)), removeDescendants);

    private bool TryAdd(ExecutionScopeId id, Lazy<byte[]> data, bool removeDescendants)
    {
        lock (_lock)
        {
            if (!_states.TryAdd(id, data))
                return false;

            if (removeDescendants)
                RemoveDescendants(id);

            return true;
        }
    }
    
    public void SetException(ExecutionScopeId id, FatalWorkflowException exception, bool removeDescendants)
    {
        lock (_lock)
        {
            _states[id] = new Lazy<byte[]>(() => SerializeException(exception));
            if (removeDescendants)
                RemoveDescendants(id);
        }
    }    
    
    public void SetState(ExecutionScopeId id, byte[] data, bool removeDescendants)
    {
        lock (_lock)
        {
            _states[id] = new Lazy<byte[]>(data);
            if (removeDescendants)
                RemoveDescendants(id);
        }
    }    
    
    public void Remove(ExecutionScopeId id)
    {
        lock (_lock)
            _states.Remove(id);
    }

    public void RemoveDescendants(ExecutionScopeId id)
    {
        lock (_lock)
            _states.Keys.Where(id.IsDescendant).ToList().ForEach(descendantId => _states.Remove(descendantId));
    }

    public List<ExecutionScopeId> GetDescendants(ExecutionScopeId id)
    {
        lock (_lock)
            return _states.Keys.Where(id.IsDescendant).ToList();
    }

    public List<ExecutionScopeId> GetChildren(ExecutionScopeId id)
    {
        lock (_lock)
            return _states.Keys.Where(id.IsChild).ToList();
    }

    private static byte[] Serialize(object instance, Type instanceType)
    {
        var json = JsonSerializer.Serialize(instance, instanceType).ToUtf8Bytes();
        var type = instanceType.SimpleQualifiedName().ToUtf8Bytes();
        return ByteArrayMarshaller.Serialize(type, json);
    }

    private static byte[] SerializeException(FatalWorkflowException exception)
    {
        var (exceptionMessage, exceptionStackTrace, exceptionType) = 
            exception.ToStoredException();
        
        return ByteArrayMarshaller.Serialize(
            exceptionMessage.ToUtf8Bytes(),
            exceptionStackTrace?.ToUtf8Bytes(),
            exceptionType.ToUtf8Bytes()
        );
    }

    private static object? DeserializeState(byte[] bytes)
    {
        if (bytes.Length == 0)
            return null;

        var arr = ByteArrayMarshaller.Deserialize(bytes, expectedCount: 3);
        if (arr.Count == 3)
            return DeserializeException(arr);
        if (arr.Count != 2)
            throw new InvalidOperationException($"Serialized state must contain 2 or 3 segments but contained {arr.Count}");

        var typeName = arr[0]!.Value.ToStringFromUtf8Bytes();
        var type = Type.GetType(typeName, throwOnError: true)!;

        return JsonSerializer.Deserialize(arr[1]!.Value.Span, type)!;
    }

    private static FatalWorkflowException DeserializeException(IReadOnlyList<ReadOnlyMemory<byte>?> arr)
    {
        var storedException = new StoredException(
            ExceptionMessage: arr[0]!.Value.ToStringFromUtf8Bytes(),
            ExceptionStackTrace: arr[1]?.ToStringFromUtf8Bytes(),
            ExceptionType: arr[2]!.Value.ToStringFromUtf8Bytes()
        );

        return FatalWorkflowException.Create(storedException);
    }

    public byte[] GetBytes()
    {
        lock (_lock)
        {
            var segments = new byte[_states.Count * 2][];
            var i = 0;
            foreach (var (key, value) in _states)
            {
                segments[i++] = key.Id.ToUtf8Bytes();
                segments[i++] = value.Value;
            }

            return ByteArrayMarshaller.Serialize(segments);
        }
    }

    public object? Deserialize(ExecutionScopeId id, out bool exists)
    {
        Lazy<byte[]>? bytes;
        lock (_lock)
            exists = _states.TryGetValue(id, out bytes);

        return exists ? DeserializeState(bytes!.Value) : null;
    }

    public static States Deserialize(byte[] bytes, RunningAndWaiting runningAndWaiting, SyncCallback syncCallback, TimeoutsManager timeoutsManager)
    {
        var segments = ByteArrayMarshaller.Deserialize(bytes);
        if (segments.Count % 2 != 0)
            throw new InvalidOperationException("Serialized states must contain an even number of segments");

        var states = new States(runningAndWaiting, syncCallback, timeoutsManager);
        for (var i = 0; i < segments.Count; i += 2)
        {
            var id = new ExecutionScopeId(segments[i]!.Value.ToStringFromUtf8Bytes());
            var data = segments[i + 1]!.Value.ToArray();
            states._states[id] = new Lazy<byte[]>(data);
        }

        return states;
    }

    public static Dictionary<ExecutionScopeId, object?> DeserializeToDictionary(byte[] bytes)
    {
        using var timeoutsManager = new TimeoutsManager();
        var states = Deserialize(bytes, new RunningAndWaiting(), (_, _) => Task.CompletedTask, timeoutsManager);
        return states._states.ToDictionary(kv => kv.Key, kv => DeserializeState(kv.Value.Value));
    }

    public SerializedStateMachine Serialize()
    {
        lock (_lock)
            return new SerializedStateMachine(GetBytes(), TimeoutsManager.MinimumTimeout);
    }

    public Task WaitForNextFlush()
    {
        lock (_lock)
            return _signal.Wait();
    }

    public async Task Flush()
    {
        TaskCompletionSource? signalTcs = null;

        await _flushSemaphore.WaitAsync();
        try
        {
            byte[] bytes;
            DateTime? minimumTimeout;
            lock (_lock)
            {
                signalTcs = _signal.MoveNext();
                if (_thrownException != null)
                {
                    signalTcs.TrySetException(_thrownException.SourceException);
                    _thrownException.Throw();
                }

                (bytes, minimumTimeout) = Serialize();
            }

            await SyncCallback(bytes, minimumTimeout);
            signalTcs.TrySetResult();
        }
        catch (Exception e)
        {
            lock (_lock)
            {
                _thrownException ??= ExceptionDispatchInfo.Capture(e);
                signalTcs!.TrySetException(e);
            }

            throw;
        }
        finally
        {
            _flushSemaphore.Release();
        }
    }
}