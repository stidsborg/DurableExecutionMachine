using CaptureGuidAndWaitForMessages = DurableExecutionMachine.Samples.Scenarios.CaptureGuidAndWaitForMessages;

namespace DurableExecutionMachine.Samples;

public static class Program
{
    private static readonly Dictionary<string, Func<Task>> Scenarios = new()
    {
        ["capture-guid-and-wait-for-messages"] = CaptureGuidAndWaitForMessages.Setup.Run,
    };

    public static async Task Main(string[] args)
    {
        if (args.Length == 0 || !Scenarios.TryGetValue(args[0], out var scenario))
        {
            Console.WriteLine("Usage: dotnet run --project samples/DurableExecutionMachine.Samples -- <scenario>");
            Console.WriteLine();
            Console.WriteLine("Scenarios:");
            foreach (var name in Scenarios.Keys)
                Console.WriteLine($"  {name}");
            return;
        }

        await scenario();
    }
}
