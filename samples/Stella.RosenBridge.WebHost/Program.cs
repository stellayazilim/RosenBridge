using System.Text;
using Stella.RosenBridge;
using Stella.RosenBridge.Hosting;
using Stella.RosenBridge.Hosting.AspNetCore;

var smoke = args.Contains("--smoke");
var builder = WebApplication.CreateBuilder(args.Where(arg => arg != "--smoke").ToArray());
builder.WebHost.UseUrls(smoke ? "http://127.0.0.1:0" : "http://127.0.0.1:5080");
builder.Services
    .AddRosenBridge(options => options.Server = new()
    {
        AllowAnonymous = true,
        AllowInsecureLoopback = true
    });

await using var app = builder.Build();
app.MapChannel("/echo", async (channel, ct) =>
{
    channel
        .OnData((data, token) => channel.WriteAsync(data, token))
        .OnEnd(() => channel.CompleteWrites());
    await channel.Completion.WaitAsync(ct);
});

app.MapRosenBridge("/rb");
app.MapGet("/health", () => "OK");
if (!smoke)
{
    await app.RunAsync();
    return;
}

using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
await app.StartAsync(timeout.Token);
try
{
    var address = new Uri(app.Urls.Single());
    using var http = new HttpClient();
    Console.WriteLine(await http.GetStringAsync(new Uri(address, "/health"), timeout.Token));
    await using var client = await new RosenBridgeFactory().ConnectOverHttpAsync(
        new Uri(address, "/rb"), new() { AllowInsecureLoopback = true }, timeout.Token);
    await using var channel = await client.RequestChannelAsync("/echo", timeout.Token);
    using var output = new MemoryStream();
    channel.Send(Encoding.UTF8.GetBytes("hello from ASP.NET Core"));
    await channel.ReadPipe(output);
    Console.WriteLine(Encoding.UTF8.GetString(output.ToArray()));
}
finally { await app.StopAsync(timeout.Token); }
