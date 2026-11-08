using Dormouse.Messages;
using DurableExecutionMachine.Flows;
using Microsoft.Extensions.Logging;

namespace Dormouse;

/// <summary>
/// The order saga written as straight-line code: the order is paid and shipped, in either order, unless it times out first.
/// Run is replayed from the start on every message, and the captures make its side effects happen once.
/// </summary>
public class OrderFlow : Flow<StartOrder, PaymentReceived, OrderShipped, OrderTimeout>
{
    protected override async Task Run(StartOrder start, Context ctx)
    {
        var orderId = start.OrderId;

        await ctx.Capture(async () =>
        {
            Console.WriteLine($"  [flow] started order {orderId} for {start.Item}, scheduling its timeout");
            // a TimeoutMessage is scheduled, so this wakes the flow up after its delay
            await Bus.PublishAsync(new OrderTimeout(orderId));
            return new Unit();
        });

        // a message that is not waited for yet stays in the inbox, so a shipment that arrives before the payment is still received
        if (await ctx.Message<object>(m => m is PaymentReceived or OrderTimeout) is not PaymentReceived payment)
        {
            await Log(ctx, $"order {orderId} timed out before it was paid, abandoning it");
            return;
        }
        await Log(ctx, $"order {orderId} paid with {payment.Amount} EUR");

        if (await ctx.Message<object>(m => m is OrderShipped or OrderTimeout) is not OrderShipped shipped)
        {
            await Log(ctx, $"order {orderId} timed out before it was shipped, abandoning it");
            return;
        }
        await Log(ctx, $"order {orderId} shipped with tracking {shipped.TrackingNumber}");

        await ctx.Capture(async () =>
        {
            await Bus.PublishAsync(new OrderCompleted(orderId));
            return new Unit();
        });
    }

    // written from a capture, as a line written outside of one would be written again on every replay
    private static Task Log(Context ctx, string line)
        => ctx.Capture(() =>
        {
            Console.WriteLine($"  [flow] {line}");
            return new Unit();
        });
}

// an ordinary Wolverine handler, outside of the flow, for the message the flow publishes when the order is done
public static class OrderCompletedHandler
{
    public static void Handle(OrderCompleted completed, ILogger<OrderCompleted> logger)
        => logger.LogInformation("Order {Id} is complete, sending the receipt", completed.OrderId);
}
