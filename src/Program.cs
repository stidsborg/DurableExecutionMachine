namespace DurableExecutionMachine;

public static class Program
{
    public static void Main(string[] args)
    {
        var stm = new StateMachine<TestFlow, int, string>(
            startFlow: (testFlow, i, c) => testFlow.Run(i, c),
            new States(),
            new TimeoutsManager()
        );
        var flow = new TestFlow();
        var ctx = new Context(new ExecutionScope(), new AsyncGate(), new States(), messages: null!);
        stm.Start(flow, ctx, param: 1);

        Console.ReadLine();
    }
}
