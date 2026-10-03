namespace DurableExecutionMachine;

internal static class TimeSpanExtensions
{
    public static TimeSpan ZeroIfNegative(this TimeSpan ts)
        => ts < TimeSpan.Zero ? TimeSpan.Zero : ts;
    
    public static TimeSpan CapAtOneDay(this TimeSpan ts)
        => ts > TimeSpan.FromDays(1) ? TimeSpan.FromDays(1) : ts;
}