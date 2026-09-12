namespace DurableExecutionMachine;

public class TestFlow
{
    public async Task<string> Run(int input, Context ctx)
    {
        await ctx.Capture(async () =>
        {
            await ctx.Capture(async () =>
            {
                await ctx.Capture(() => Task.FromResult("0.0.0"));
                await ctx.Capture(() => Task.FromResult("0.0.1"));
                return Task.FromResult("0.0");
            });
            
            await ctx.Capture(async () =>
            {
                await ctx.Capture(() => Task.FromResult("inner#0.1.0"));
                await ctx.Capture(() => Task.FromResult("inner#0.1.1"));
                return Task.FromResult("0.1");
            });
           
            return "0";
        });
            
        await ctx.Capture(async () =>
        {
            await ctx.Capture(async () =>
            {
                await ctx.Capture(() => Task.FromResult("1.0.0"));
                await ctx.Capture(() => Task.FromResult("1.0.1"));
                return Task.FromResult("1.0");
            });
            
            await ctx.Capture(async () =>
            {
                await ctx.Capture(() => Task.FromResult("inner#1.1.0"));
                await ctx.Capture(() => Task.FromResult("inner#1.1.1"));
                return Task.FromResult("1.1");
            });
           
            return "1";
        });

        return "";
    }
}