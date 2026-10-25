namespace DurableExecutionMachine;

public abstract class FatalWorkflowException : Exception
{
    public string FlowErrorMessage { get; }
    public string? FlowStackTrace { get; }
    public Type ErrorType { get; }

    public FatalWorkflowException(string errorMessage, string? stackTrace, Type errorType) : base(errorMessage)
    {
        FlowErrorMessage = errorMessage;
        FlowStackTrace = stackTrace;
        ErrorType = errorType;
    }

    public static FatalWorkflowException Create(StoredException storedException)
    {
        var (message, stackTrace, exceptionTypeString) = storedException;
        var exceptionType = Type.GetType(exceptionTypeString, throwOnError: true);
        var genericFatalExceptionType = typeof(FatalWorkflowException<>).MakeGenericType(exceptionType!);
        return (FatalWorkflowException?) Activator.CreateInstance(genericFatalExceptionType, args: [message, stackTrace, exceptionType])
               ?? throw new InvalidOperationException("Unable to create FatalWorkflowException from StoredException: " + storedException);
    }

    public static FatalWorkflowException<TException> Create<TException>(TException exception) where TException : Exception
        => new FatalWorkflowException<TException>(
            exception.Message,
            exception.StackTrace,
            typeof(TException)
        );

    public static FatalWorkflowException CreateNonGeneric(Exception exception)
    {
        var genericFatalExceptionType = typeof(FatalWorkflowException<>).MakeGenericType(exception.GetType());
        var message = exception.Message;
        var stackTrace = exception.StackTrace;
        var exceptionType = exception.GetType();

        return (FatalWorkflowException?) Activator.CreateInstance(genericFatalExceptionType, args: [message, stackTrace, exceptionType])
               ?? throw new InvalidOperationException("Unable to create FatalWorkflowException from Exception: " + exception);
    }

    public StoredException ToStoredException()
        => new(FlowErrorMessage, FlowStackTrace, ErrorType.SimpleQualifiedName());
}

public class FatalWorkflowException<TException>(string errorMessage, string? stackTrace, Type errorType)
    : FatalWorkflowException(errorMessage, stackTrace, errorType)
    where TException : Exception;
