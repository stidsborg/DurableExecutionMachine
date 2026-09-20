using System.Text.Json;

namespace DurableExecutionMachine;

public class States
{
    private readonly Dictionary<ExecutionScopeId, byte[]> _states = new();
    private readonly Lock _lock = new();
    
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
    
    public void SetState(ExecutionScopeId id, byte[] data, bool removeChildren)
    {
        lock (_lock)
        {
            _states[id] = data;
            if (removeChildren)
                RemoveChildren(id);
        }
    }    
    
    public void Lock(Action action)
    {
        lock (_lock)
            action();
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

    private static object DeserializeState(byte[] bytes)
    {
        var arr = ByteArrayMarshaller.Deserialize(bytes, expectedCount: 2);
        var typeName = arr[0]!.Value.ToStringFromUtf8Bytes();
        var type = Type.GetType(typeName, throwOnError: true)!;
        
        return JsonSerializer.Deserialize(arr[1]!.Value.Span, type)!;
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

    public static States Deserialize(byte[] bytes)
    {
        var segments = ByteArrayMarshaller.Deserialize(bytes);
        if (segments.Count % 2 != 0)
            throw new InvalidOperationException("Serialized states must contain an even number of segments");

        var states = new States();
        for (var i = 0; i < segments.Count; i += 2)
        {
            var id = new ExecutionScopeId(segments[i]!.Value.ToStringFromUtf8Bytes());
            var data = segments[i + 1]!.Value.ToArray();
            states._states[id] = data;
        }

        return states;
    }

    public static Dictionary<ExecutionScopeId, object> DeserializeToDictionary(byte[] bytes)
    {
        var states = Deserialize(bytes);
        return states._states.ToDictionary(kv => kv.Key, kv => DeserializeState(kv.Value));
    }
}