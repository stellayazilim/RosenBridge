using System.Text;
using Stella.RosenBridge;

try
{
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));

    var factory = new RosenBridgeFactory();

    await using var server = factory.CreateServer(new Uri("rb://127.0.0.1:0"), new()
    {
        AllowInsecureLoopback = true,
        AllowAnonymous = true
    });

    server.MapChannel("/echo", async (channel, ct) =>
    {
        channel
            .OnData((data, token) => channel.WriteAsync(data, token))
            .OnEnd(() => channel.CompleteWrites());
        await channel.Completion.WaitAsync(ct);
    });


    await server.StartAsync(timeout.Token);

    var address = new Uri($"rb://127.0.0.1:{server.LocalEndPoint.Port}");
    await using var client = await factory.ConnectAsync(address,
        new() { AllowInsecureLoopback = true }, timeout.Token);

    await using var channel = await client.RequestChannelAsync("/echo", timeout.Token);
    using var source = new MemoryStream(Encoding.UTF8.GetBytes("hello world"));
    using var destination = new MemoryStream();

    channel.WritePipe(source);
    await channel.ReadPipe(destination);
    Console.WriteLine(Encoding.UTF8.GetString(destination.ToArray()));
}
catch (Exception error)
{
    Console.Error.WriteLine(error.Message);
    Environment.ExitCode = 1;
}
