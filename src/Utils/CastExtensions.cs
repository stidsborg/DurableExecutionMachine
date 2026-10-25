namespace DurableExecutionMachine;

internal static class CastExtensions
{
    public static T? CastTo<T>(this object? obj) => obj is T value ? value : default;
}