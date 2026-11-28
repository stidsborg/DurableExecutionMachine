using System.Runtime.ExceptionServices;
using DurableExecutionMachine.Flows;
using DurableExecutionMachine.Hosting;
using Wolverine;

namespace Dormouse;

/// <summary>
/// A Wolverine saga whose Run method is a durable execution.
/// Wolverine owns the durability: the saga's only state is the machine's serialized bytes, and Wolverine stores them when the handler returns.
/// Each message is one turn. The machine is restored from the bytes, the message is delivered, the flow runs until every part of it
/// waits for a message that has not arrived, and the machine is serialized back onto the saga.
/// A turn also ends at a sync point, as the state is only persisted when the handler returns: the snapshot is put on the saga and the flow
/// sends itself a <see cref="Continue"/>, which is committed with it. The next turn runs on from the committed state.
/// </summary>
/// <remarks>
/// A turn ends when the machine's suspension completes, and that only waits for flow code that is running. A wait woken up by a timer
/// or by the join of a <c>WhenAll</c>/<c>WhenAny</c> is not counted as running until it resumes, so the turn can end before it does,
/// and the flow then does not move on until its next message. So flows hosted here only use <c>Capture</c> and <c>Message</c>, and timeouts
/// are messages Wolverine schedules (see <see cref="Messages.OrderTimeout"/>).
/// </remarks>
public abstract class Flow<T1> : Saga where T1 : notnull
{
    // Wolverine's saga storage requires a non-null identity, and it does not assign one when it creates the saga
    public string? Id { get; set; }

    // the serialized machine
    public byte[]? State { get; set; }

    private IMessageContext? _bus;

    // the context of the message being handled. messages published with it are sent when the handler succeeds, so they are committed with the state
    protected IMessageContext Bus
        => _bus ?? throw new InvalidOperationException($"Flow '{Id}' has no message context: it is only set while a message is being handled");

    // how the flow of a turn ended. each turn has its own, as a turn that ends at a sync point leaves its machine behind,
    // and the flow in it may still end after the next turn has started
    private sealed class Outcome
    {
        public bool Completed;
        public ExceptionDispatchInfo? Failure;
    }

    private Outcome _outcome = new();

    protected abstract Task Run(T1 message, Context ctx);

    // called both when no saga exists for the id and when one does, so the message either starts the flow or is delivered to it
    public Task StartOrHandle(T1 message, string sagaId, IMessageContext bus)
    {
        Id = sagaId;
        return State is null
            ? Turn(bus, Factory.New(), stm => stm.Start(this, message))
            : Deliver(message, bus);
    }

    protected Task Deliver(object message, IMessageContext bus)
        => Turn(bus, Factory.Deserialize(State!), stm =>
        {
            // delivered before the flow is resumed, so a wait for it is satisfied while replaying instead of being woken up afterwards.
            // the envelope id makes a redelivered message a duplicate. Wolverine persists the state, so the turn does not wait for a sync,
            // and the task fails once the turn disposes the machine
            _ = stm.DeliverMessage(message, idempotencyKey: bus.Envelope?.Id.ToString());
            stm.Resume(this);
        });

    // a Continue can arrive after the flow has completed, e.g. when another message continued it in the meantime. as the start of a saga,
    // Wolverine then hands it an empty saga, which is marked completed so it is not stored. A static NotFound would be declared on every flow,
    // as Wolverine does not find static methods declared on a saga's base class
    public Task StartOrHandle(Continue message, IMessageContext bus)
    {
        if (State is null)
        {
            MarkCompleted();
            return Task.CompletedTask;
        }

        return Turn(bus, Factory.Deserialize(State), stm => stm.Resume(this));
    }

    private async Task Turn(IMessageContext bus, StateMachine<Flow<T1>, T1, Unit> stm, Action<StateMachine<Flow<T1>, T1, Unit>> run)
    {
        _bus = bus;
        var outcome = _outcome = new Outcome();
        // the turn owns the machine and disposes it when it ends, also when it fails, so the machine's timer stops
        using var _ = stm;
        try
        {
            run(stm);
            while (true)
            {
                var (status, syncRequested) = await stm.NextQuiescenceOrSync();
                if (syncRequested)
                {
                    // the sync completes when Wolverine commits the state, i.e. when the handler returns, so the turn ends here.
                    // the sync is never completed in this machine: the flow runs on in the next turn, from the committed state
                    State = stm.StartSync().States;
                    await bus.PublishAsync(new Continue(Id!));
                    return;
                }

                // the suspension is only requested once no subflow is running, as a requested suspension holds a subflow at its next wait,
                // even one whose message is already in the inbox
                if (status != StateMachineStatus.Running)
                    break;
            }

            // completes at once: only a message wakes a flow hosted here (see the remarks), and none is delivered during the turn
            await stm.Suspend();
        }
        finally
        {
            _bus = null;
        }

        // the machine is disposed while suspended, so nothing in it runs after the state has been taken
        outcome.Failure?.Throw();
        if (outcome.Completed)
            MarkCompleted();
        else
            State = stm.Serialize().States;
    }

    private static StateMachineFactory<Flow<T1>, T1, Unit> Factory { get; } = new(
        startFlow: (flow, message, ctx) => flow.RunToEnd(message, ctx)
    );

    // records how the flow ended before the machine counts it as suspended, so the turn sees it as soon as the suspension completes.
    // the outcome is taken when the flow starts, which is inside the turn's Start or Resume
    private async Task<Unit> RunToEnd(T1 message, Context ctx)
    {
        var outcome = _outcome;
        try
        {
            await Run(message, ctx);
            outcome.Completed = true;
        }
        catch (Exception e)
        {
            outcome.Failure = ExceptionDispatchInfo.Capture(e);
        }

        return new Unit();
    }
}

public abstract class Flow<T1, T2> : Flow<T1> where T1 : notnull
{
    public Task Handle(T2 message, IMessageContext bus) => Deliver(message!, bus);
}

public abstract class Flow<T1, T2, T3> : Flow<T1, T2> where T1 : notnull
{
    public Task Handle(T3 message, IMessageContext bus) => Deliver(message!, bus);
}

public abstract class Flow<T1, T2, T3, T4> : Flow<T1, T2, T3> where T1 : notnull
{
    public Task Handle(T4 message, IMessageContext bus) => Deliver(message!, bus);
}

public abstract class Flow<T1, T2, T3, T4, T5> : Flow<T1, T2, T3, T4> where T1 : notnull
{
    public Task Handle(T5 message, IMessageContext bus) => Deliver(message!, bus);
}
