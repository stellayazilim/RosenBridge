using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Stella.RosenBridge;
using Stella.RosenBridge.Hosting;

var smoke = args.Contains("--smoke");
var builder = RosenBridgeApp.CreateBuilder(args.Where(arg => arg != "--smoke").ToArray());
builder.Services
    .AddRosenBridge(options => options.Server = new()
    {
        AllowInsecureLoopback = true
    })
    .UseTcp(new Uri(smoke ? "rb://127.0.0.1:0" : "rb://127.0.0.1:7000"));

await using var host = builder.Build();
host.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());
    await channel.Completion.WaitAsync(ct);
});

if (!smoke)
{
    await host.RunAsync();
    return;
}

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await host.StartAsync(timeout.Token);
try
{
    var endpoint = host.Services.GetRequiredService<RosenBridgeHost>().Server.LocalEndPoint;
    await using var client = await new RosenBridgeFactory().ConnectAsync(
        new Uri($"rb://127.0.0.1:{endpoint.Port}"), new() { AllowInsecureLoopback = true }, timeout.Token);
    await using var channel = await client.RequestChannelAsync("/echo", timeout.Token);
    using var output = new MemoryStream();
    channel.Send(Encoding.UTF8.GetBytes("hello from Generic Host"));
    await channel.ReadPipe(output);
    Console.WriteLine(Encoding.UTF8.GetString(output.ToArray()));
}
finally { await host.StopAsync(timeout.Token); }
