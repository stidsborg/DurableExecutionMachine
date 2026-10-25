namespace DurableExecutionMachine.Samples;

/// <summary>
/// Stands in for the host's database: keeps the latest bytes handed to the sync callback and prints what they contain.
/// </summary>
public class InMemoryStore
{
    public byte[]? Bytes { get; private set; }
    public DateTime? MinimumTimeout { get; private set; }

    public Task Save(byte[] bytes, DateTime? minimumTimeout)
    {
        Bytes = bytes;
        MinimumTimeout = minimumTimeout;

        Console.WriteLine($"  [store] saved {bytes.Length} bytes (minimum timeout: {minimumTimeout?.ToString("O") ?? "none"})");
        foreach (var (id, value) in States.DeserializeToDictionary(bytes).OrderBy(kv => kv.Key.Id, StringComparer.Ordinal))
            Console.WriteLine($"  [store]   {id.Id} = {value ?? "null"}");

        return Task.CompletedTask;
    }
}
