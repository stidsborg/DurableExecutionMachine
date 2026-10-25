namespace DurableExecutionMachine.Utils;

internal static class Safe
{
    public static T Try<T>(Func<T> f, Func<Exception, T> onError)
    {
        try
        {
            return f();
        }
        catch (Exception ex)
        {
            return onError(ex);
        }
    }
}