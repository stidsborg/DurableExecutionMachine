using DurableExecutionMachine.Flows;

namespace DurableExecutionMachine.Samples.Scenarios.CaptureGuidAndWaitForMessages;

/// <summary>
/// Captures a guid and then waits for a number of greetings.
/// </summary>
public class Flow
{
    public async Task<string> Run(int greetingsToWaitFor, Context ctx)
    {
        var id = await ctx.Capture(() =>
        {
            var guid = Guid.NewGuid();
            Console.WriteLine($"  [flow] generated guid {guid}");
            return guid;
        });
        Console.WriteLine($"  [flow] running with guid {id}");

        var greetings = new List<string>();
        for (var i = 0; i < greetingsToWaitFor; i++)
        {
            var greeting = await ctx.Message<Greeting>();
            Console.WriteLine($"  [flow] received '{greeting!.Text}'");
            greetings.Add(greeting.Text);
        }

        return $"{id}: {string.Join(", ", greetings)}";
    }
}
