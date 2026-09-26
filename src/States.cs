using System.Text.Json;

namespace DurableExecutionMachine;

public class States(RunningAndWaiting runningAndWaiting)
{
    private readonly Dictionary<ExecutionScopeId, byte[]> _states = new();
    private readonly Lock _lock = new();

    internal RunningAndWaiting RunningAndWaiting { get; } = runningAndWaiting;
    
    public void SetState(ExecutionScopeId id, object instance, Type instanceType, bool removeChildren)
    {
        var data = Serialize(instance, instanceType);
        lock (_lock)
        {
            _states[id] = data;
            if (removeChildren)
                RemoveChildren(id);
        }
    }
    
    public void SetNull(ExecutionScopeId id, bool removeChildren)
    {
        lock (_lock)
        {
            _states[id] = [];
            if (removeChildren)
                RemoveChildren(id);
        }
    }
    
    public void SetException(ExecutionScopeId id, FatalWorkflowException exception, bool removeChildren)
    {
        var data = SerializeException(exception);
        lock (_lock)
        {
            _states[id] = data;
            if (removeChildren)
                RemoveChildren(id);
        }
    }    
    
    public void SetState(ExecutionScopeId id, byte[] data, bool removeChildren)
    {
        lock (_lock)
        {
            _states[id] = data;
            if (removeChildren)
                RemoveChildren(id);
        }
    }    
    
    public void RemoveChildren(ExecutionScopeId id)
    {
        lock (_lock)
            _states.Keys.Where(id.IsChild).ToList().ForEach(childId => _states.Remove(childId));
    }

    private byte[] Serialize(object instance, Type instanceType)
    {
        var json = JsonSerializer.Serialize(instance, instanceType).ToUtf8Bytes();
        var type = instanceType.SimpleQualifiedName().ToUtf8Bytes();
        return ByteArrayMarshaller.Serialize(type, json);
    }

    private byte[] SerializeException(FatalWorkflowException exception)
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
                segments[i++] = value;
            }

            return ByteArrayMarshaller.Serialize(segments);
        }
    }

    public object? Deserialize(ExecutionScopeId id, out bool exists)
    {
        byte[]? bytes;
        lock (_lock)
            exists = _states.TryGetValue(id, out bytes);

        return exists ? DeserializeState(bytes!) : null;
    }

    public static States Deserialize(byte[] bytes, RunningAndWaiting runningAndWaiting)
    {
        var segments = ByteArrayMarshaller.Deserialize(bytes);
        if (segments.Count % 2 != 0)
            throw new InvalidOperationException("Serialized states must contain an even number of segments");

        var states = new States(runningAndWaiting);
        for (var i = 0; i < segments.Count; i += 2)
        {
            var id = new ExecutionScopeId(segments[i]!.Value.ToStringFromUtf8Bytes());
            var data = segments[i + 1]!.Value.ToArray();
            states._states[id] = data;
        }

        return states;
    }

    public static Dictionary<ExecutionScopeId, object?> DeserializeToDictionary(byte[] bytes)
    {
        var states = Deserialize(bytes, new RunningAndWaiting());
        return states._states.ToDictionary(kv => kv.Key, kv => DeserializeState(kv.Value));
    }

    public Task Flush() => RunningAndWaiting.SuspendIfNeeded(); //todo persist states
}