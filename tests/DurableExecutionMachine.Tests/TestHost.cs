using DurableExecutionMachine.Hosting;
using DurableExecutionMachine.Storage;

namespace DurableExecutionMachine.Tests;

// stands in for a host that persists nothing: it completes each requested sync at once
internal static class TestHost
{
    // the host loop: syncs on each request and returns once no subflow is running
    public static async Task<StateMachineStatus> RunUntilQuiescent<TFlow, TParam, TResult>(StateMachine<TFlow, TParam, TResult> stm)
        where TParam : notnull
    {
        while (true)
        {
            var (status, syncRequested) = await stm.NextQuiescenceOrSync();
            if (syncRequested)
            {
                stm.StartSync();
                stm.CompleteSync();
            }
            else if (status != StateMachineStatus.Running)
                return status;
        }
    }

    // for tests that drive a bare States: syncs in the background whenever a sync is requested, for as long as the states live
    public static States AutoSynced(this States states, Action? onSync = null)
    {
        _ = SyncOnRequest(states, onSync);
        return states;
    }

    private static async Task SyncOnRequest(States states, Action? onSync)
    {
        while (true)
        {
            await states.FlushRequested();
            states.StartSync();
            onSync?.Invoke();
            states.CompleteSync();
        }
    }
}
