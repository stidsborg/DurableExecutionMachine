namespace DurableExecutionMachine.Samples;

/// <summary>
/// Stands in for the host's database: persists the latest bytes handed to the sync callback to a file.
/// </summary>
public class FileStore(string filePath)
{
    public byte[]? Bytes => File.Exists(filePath) ? File.ReadAllBytes(filePath) : null;

    public Task Save(byte[] bytes, DateTime? minimumTimeout) => File.WriteAllBytesAsync(filePath, bytes);
}
