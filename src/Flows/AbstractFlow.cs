namespace DurableExecutionMachine;

public abstract class AbstractFlow<TParam, TResult>
{
    public abstract Task<TResult> Run(TParam param, Context ctx);
}

public abstract class AbstractFlow<TParam>
{
    public abstract Task Run(TParam param, Context ctx);
}