using Dormouse;
using Dormouse.Messages;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Wolverine;
using Wolverine.Persistence.Sagas;
using Wolverine.Tracking;

var builder = Host.CreateApplicationBuilder(args);

builder.Logging.SetMinimumLevel(LogLevel.Information);
// Wolverine's own logging is noisy for a demo like this
builder.Logging.AddFilter("Wolverine", LogLevel.Warning);
builder.Logging.AddFilter("Microsoft.Hosting", LogLevel.Warning);

// no persistence is configured, so Wolverine falls back to in-memory saga storage.
// swapping in Marten or EF Core makes the flows durable, as the saga is all there is to store
builder.Services.AddWolverine(opts => opts.ApplicationAssembly = typeof(OrderFlow).Assembly);

using var host = builder.Build();
await host.StartAsync();

var sagas = host.Services.GetRequiredService<InMemorySagaPersistor>();
var log = host.Services.GetRequiredService<ILoggerFactory>().CreateLogger("Demo");

async Task Send(object message)
{
    log.LogInformation("Sending {Message}", message);
    try
    {
        await host.SendMessageAndWaitAsync(message);
    }
    catch (Exception e)
    {
        var inner = e.InnerException ?? e;
        log.LogError("!!! {Message} failed: {Type}: {Error}", message.GetType().Name, inner.GetType().Name, inner.Message);
    }
}

void ShowState(string id)
{
    var flow = sagas.Load<OrderFlow>(id);
    log.LogInformation("--> state of {Id}: {State}", id,
        flow is null ? "(none - completed or never started)" : $"{flow.State?.Length ?? 0} bytes");
}

log.LogInformation("=== order-1: shipped before it is paid, and completes ===");
await Send(new StartOrder("order-1", "Coffee grinder"));
ShowState("order-1");

await Send(new OrderShipped("order-1", "TRACK-123"));
ShowState("order-1");

await Send(new PaymentReceived("order-1", 249.95m));
ShowState("order-1");

log.LogInformation("=== order-2: paid, but times out before it is shipped ===");
await Send(new StartOrder("order-2", "Kettle"));
await Send(new PaymentReceived("order-2", 49.95m));
ShowState("order-2");

// the OrderTimeouts published when the orders started are delivered after 5 seconds. the one for order-1 is dropped as its saga is gone
log.LogInformation("Waiting for the scheduled OrderTimeouts...");
await Task.Delay(TimeSpan.FromSeconds(7));
ShowState("order-1");
ShowState("order-2");

await host.StopAsync();
