namespace DurableExecutionMachine;

public static class Program
{
    public static void Main(string[] args)
    {
        var stm = new StateMachine<TestFlow, int, string>(startFlow: (testFlow, i, c) => testFlow.Run(i, c));
        var flow = new TestFlow();
        var ctx = new Context(new ExecutionScope());
        stm.Start(flow, ctx, param: 1);

        Console.ReadLine();
    }
}
