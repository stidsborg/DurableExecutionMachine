namespace DurableExecutionMachine;

internal static class TimeSpanExtensions
{
    public static TimeSpan ZeroIfNegative(this TimeSpan ts)
        => ts < TimeSpan.Zero ? TimeSpan.Zero : ts;
}