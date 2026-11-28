using DurableExecutionMachine.Hosting;

namespace DurableExecutionMachine.Samples.Scenarios.CaptureGuidAndWaitForMessages;

/// <summary>
/// Spans two runs of the process. The first run starts the flow, delivers the first greeting, stores the state and exits while
/// the flow is still waiting. The second run restores the machine from the file: on replay the guid and the first
/// greeting are read back from state instead of being produced again, and the flow completes once the remaining
/// greetings are delivered. The file is then deleted, so the run after that starts over.
/// </summary>
public static class Setup
{
    // relative to the current working directory
    private static readonly string FilePath = "./capture-guid-and-wait-for-messages.state";

    public static async Task Run()
    {
        var store = new FileStore(FilePath);
        var factory = new StateMachineFactory<Flow, int, string>(
            startFlow: (flow, greetingsToWaitFor, ctx) => flow.Run(greetingsToWaitFor, ctx)
        );

        if (store.Bytes is { } bytes)
        {
            Console.WriteLine($"Restoring the execution from {FilePath}");
            var stm = factory.Deserialize(bytes);
            stm.Resume(new Flow());

            Console.WriteLine("Delivering 'hi'");
            _ = stm.DeliverMessage("hi");
            Console.WriteLine("Delivering 'hey'");
            _ = stm.DeliverMessage("hey");

            Console.WriteLine($"Result: {stm.Completion()}");

            File.Delete(FilePath);
            Console.WriteLine("Deleted the stored state - the next run starts a new execution");
        }
        else
        {
            Console.WriteLine("Starting a new execution");
            var stm = factory.New();
            stm.Start(new Flow(), param: 3);

            Console.WriteLine("Delivering 'hello'");
            var delivered = stm.DeliverMessage("hello");

            Console.WriteLine("Storing the state each time the flow asks for it, until the flow waits");
            await SyncUntilQuiescent(stm, store);

            // the delivery does not request a sync, so a last one stores 'hello' and completes the delivery
            Console.WriteLine("Storing the delivered message");
            await Sync(stm, store);
            await delivered;

            Console.WriteLine($"Stored state in {FilePath} - run the scenario again to resume the execution");
        }
    }

    // the host's side of the machine: syncs whenever the flow asks for it, and returns once no subflow is running
    private static async Task SyncUntilQuiescent(StateMachine<Flow, int, string> stm, FileStore store)
    {
        while (true)
        {
            var (status, syncRequested) = await stm.NextQuiescenceOrSync();
            if (syncRequested)
                await Sync(stm, store);
            else if (status != StateMachineStatus.Running)
                return;
        }
    }

    private static async Task Sync(StateMachine<Flow, int, string> stm, FileStore store)
    {
        var (states, minimumTimeout) = stm.StartSync();
        await store.Save(states, minimumTimeout);
        stm.CompleteSync();
    }
}
