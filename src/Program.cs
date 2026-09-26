namespace DurableExecutionMachine;

public static class Program
{
    public static void Main(string[] args)
    {
        var factory = new StateMachineFactory<TestFlow, int, string>(
            startFlow: (testFlow, i, c) => testFlow.Run(i, c),
            syncCallback: () => Task.CompletedTask
        );

        var stm = factory.New();
        stm.Start(new TestFlow(), param: 1);

        Console.ReadLine();
    }
}
