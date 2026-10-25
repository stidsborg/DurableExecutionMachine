using System.Text.Json;

namespace DurableExecutionMachine;

public record StoredException(string ExceptionMessage, string? ExceptionStackTrace, string ExceptionType)
{
    public byte[] Serialize() => JsonSerializer.Serialize(this).ToUtf8Bytes();
    public static StoredException Deserialize(byte[] bytes) => Deserialize(bytes.ToStringFromUtf8Bytes());
    public static StoredException Deserialize(string json) => JsonSerializer.Deserialize<StoredException>(json)!;
}
