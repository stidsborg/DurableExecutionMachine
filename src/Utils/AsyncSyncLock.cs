namespace DurableExecutionMachine;

public class AsyncSyncLock
{
    private readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

    public async Task Do(Func<Task> work)
    {
        try
        {
            await _semaphore.WaitAsync();
        }
        finally
        {
            _semaphore.Release();
        }
    }
}