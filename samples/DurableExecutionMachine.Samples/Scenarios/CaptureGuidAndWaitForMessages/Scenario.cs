namespace DurableExecutionMachine.Samples.Scenarios.CaptureGuidAndWaitForMessages;

/// <summary>
/// Spans two runs of the process. The first run starts the flow, delivers the first greeting, syncs and exits while
/// the flow is still waiting. The second run restores the machine from the file: on replay the guid and the first
/// greeting are read back from state instead of being produced again, and the flow completes once the remaining
/// greetings are delivered. The file is then deleted, so the run after that starts over.
/// </summary>
public static class Scenario
{
    // a delivery completes once it has been flushed, so flush it if nothing else does within this time
    private static readonly TimeSpan FlushWithin = TimeSpan.FromMilliseconds(100);

    // Path.GetTempPath() is $TMPDIR on macOS, e.g. /var/folders/xx/.../T/
    private static readonly string FilePath = "./capture-guid-and-wait-for-messages.state";

    public static async Task Run()
    {
        var store = new FileStore(FilePath);
        var factory = new StateMachineFactory<Flow, int, string>(
            startFlow: (flow, greetingsToWaitFor, ctx) => flow.Run(greetingsToWaitFor, ctx),
            syncCallback: store.Save
        );

        if (store.Bytes is not { } bytes)
        {
            Console.WriteLine("Starting a new execution");
            var stm = factory.New();
            stm.Start(new Flow(), param: 3);

            Console.WriteLine("Delivering 'hello'");
            await stm.DeliverMessage(new Greeting("hello"), timeout: FlushWithin);

            Console.WriteLine("Syncing");
            await stm.Sync();

            Console.WriteLine($"Stored state in {FilePath} - run the scenario again to resume the execution");
        }
        else
        {
            Console.WriteLine($"Restoring the execution from {FilePath}");
            var stm = factory.Deserialize(bytes);
            stm.Start(new Flow(), param: 3);

            Console.ReadLine();
            
            Console.WriteLine("Delivering 'hi'");
            await stm.DeliverMessage(new Greeting("hi"), timeout: FlushWithin);
            Console.WriteLine("Delivering 'hey'");
            await stm.DeliverMessage(new Greeting("hey"), timeout: FlushWithin);

            Console.WriteLine($"Result: {stm.GetResult()}");

            File.Delete(FilePath);
            Console.WriteLine("Deleted the stored state - the next run starts a new execution");
        }
    }
}
