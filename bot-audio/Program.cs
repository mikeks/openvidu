using System.Diagnostics;
using System.Security.Cryptography;
using BotAudio;

if (args.Contains("--self-test")) { SelfTest.Run(); return; }
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls("http://127.0.0.1:" + (Environment.GetEnvironmentVariable("BOT_AUDIO_STATUS_PORT") ?? "8097"));
string path = Environment.GetEnvironmentVariable("BOT_AUDIO_MODEL") ?? "models/silero_vad.onnx";
const string expectedHash = "1A153A22F4509E292A94E67D6F9B85E8DEB25B4988682B7E174C65279D8788E3";
using (var file = File.OpenRead(path))
    if (Convert.ToHexString(SHA256.HashData(file)) != expectedHash)
        throw new InvalidDataException("Unexpected Silero model hash");
builder.Services.AddSingleton(new VadModel(path));
builder.Services.AddSingleton<SpeechDelivery>();
builder.Services.AddHostedService(p => p.GetRequiredService<SpeechDelivery>());
builder.Services.AddSingleton<AudioMonitor>();
builder.Services.AddHostedService(p => p.GetRequiredService<AudioMonitor>());
var app = builder.Build();
app.MapGet("/health", () => Results.Ok(new { ready=true }));
app.MapGet("/status", (AudioMonitor monitor) => {
    using var process = Process.GetCurrentProcess();
    return Results.Ok(new { pid=process.Id, rssBytes=process.WorkingSet64,
        cpuSeconds=process.TotalProcessorTime.TotalSeconds, threadCount=process.Threads.Count,
        audio=monitor.Status() });
});
var metricsClock = Stopwatch.StartNew();
app.MapGet("/metrics", (AudioMonitor monitor) => {
    using var process = Process.GetCurrentProcess();
    return Results.Ok(new { instanceId=monitor.InstanceId, uptimeSeconds=metricsClock.Elapsed.TotalSeconds,
        cpuSeconds=process.TotalProcessorTime.TotalSeconds, rssBytes=process.WorkingSet64, streamCount=monitor.StreamCount });
});
app.Run();