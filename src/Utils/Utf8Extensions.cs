using System.Text;

namespace DurableExecutionMachine;

public static class Utf8Extensions
{
    public static byte[] ToUtf8Bytes(this string str) => Encoding.UTF8.GetBytes(str);
    public static string ToStringFromUtf8Bytes(this byte[] bytes) => Encoding.UTF8.GetString(bytes);
    // Decodes a marshalled segment in place - ByteArrayMarshaller hands back views over the source
    // buffer, so this avoids copying each segment out to its own array first.
    public static string ToStringFromUtf8Bytes(this ReadOnlyMemory<byte> bytes) => Encoding.UTF8.GetString(bytes.Span);
}