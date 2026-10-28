using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace BotAudio;

public sealed class AudioMonitor : BackgroundService
{
    readonly HttpClient http;
    readonly VadModel model;
    readonly SpeechDelivery delivery;
    readonly Dictionary<string, ListeningAssignment> listening = new();
    readonly HashSet<string> observed = new();
    static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    readonly int maxStreams;
    readonly ConcurrentDictionary<string, AudioStream> streams = new();
    long controlErrors;
    public AudioMonitor(VadModel model, SpeechDelivery delivery)
    {
        this.model = model; this.delivery = delivery;

        maxStreams = Math.Clamp(int.Parse(Environment.GetEnvironmentVariable("BOT_AUDIO_MAX_STREAMS") ?? "32"),1,128);
        var url = Environment.GetEnvironmentVariable("BOT_AUDIO_OPENVIDU_URL") ?? "http://127.0.0.1:5443/openvidu/api/";
        var uri = new Uri(url.EndsWith('/') ? url : url + "/");
        if (uri.Scheme != "https" && !uri.IsLoopback) throw new InvalidOperationException("Remote OpenVidu requires HTTPS");
        http = new HttpClient { BaseAddress = uri, Timeout = TimeSpan.FromSeconds(3) };
        string secret = Environment.GetEnvironmentVariable("BOT_AUDIO_OPENVIDU_SECRET")
            ?? throw new InvalidOperationException("BOT_AUDIO_OPENVIDU_SECRET required");
        http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
            Convert.ToBase64String(Encoding.UTF8.GetBytes("OPENVIDUAPP:" + secret)));
    }
    public StreamLoad Load { get; } = new();
    public int StreamCount => streams.Count;
    public object Status() => new { instanceId = InstanceId, controlErrors = Interlocked.Read(ref controlErrors),
        delivery = new { delivery.Delivered, delivery.Failed, delivery.Rejected, delivery.SttFailed }, streamCount = streams.Count, maxStreams, streams = streams.Values.Select(s => s.Snapshot()).ToArray() };
    public string InstanceId { get; } = Guid.NewGuid().ToString("N");
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                try { await Reconcile(stop); }
                catch (OperationCanceledException) when (stop.IsCancellationRequested) { break; }
                catch (Exception) {
                    Interlocked.Increment(ref controlErrors);
                    // No observations are interpreted as silence while discovery/control is unavailable.
                    await RemoveAll();
                }
                await Task.Delay(TimeSpan.FromSeconds(1), stop);
            }
        }
        catch (OperationCanceledException) { }
        finally { await RemoveAll(); http.Dispose(); }
    }
    async Task Reconcile(CancellationToken stop)
    {
        using var discovery = await http.GetAsync("bot-audio/publishers", stop);
        discovery.EnsureSuccessStatusCode();
        using var discovered = JsonDocument.Parse(await discovery.Content.ReadAsStringAsync(stop));
        var next = discovered.RootElement.GetProperty("assignments").Deserialize<List<ListeningAssignment>>(JsonOptions)!.Where(a=>a.ExpiresAt>DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()).ToList();
        string AssignmentKey(ListeningAssignment a) => JsonSerializer.Serialize(new object[]{a.SessionId,a.UserId,a.AnalysisId});
        var nextKeys=next.Select(AssignmentKey).ToHashSet();
        foreach(var old in listening.ToArray()) if(!nextKeys.Contains(old.Key)) {
            if(!observed.Contains(old.Key)) new SpeechAccumulator(old.Value,s=>delivery.Submit(s)).Finish(old.Value.ExpiresAt<=DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()?"deadline":"explicit-end");
            listening.Remove(old.Key); observed.Remove(old.Key);
        }
        foreach(var a in next) listening[AssignmentKey(a)]=a;
        var wanted=new Dictionary<string,(ListeningAssignment Assignment,string Stream)>();
        foreach(var p in discovered.RootElement.GetProperty("publishers").EnumerateArray()) {
            var a=p.GetProperty("assignment").Deserialize<ListeningAssignment>(JsonOptions)!;
            if(a.ExpiresAt<=DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()) continue;
            string stream=p.GetProperty("streamId").GetString()!;
            string key=JsonSerializer.Serialize(new object[]{a,stream});
            if(wanted.Count<maxStreams) wanted[key]=(a,stream);
        }
        foreach(var entry in streams.ToArray()) if(!wanted.ContainsKey(entry.Key)) {
            entry.Value.FinishReason=entry.Value.Assignment!.ExpiresAt<=DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()?"deadline":nextKeys.Contains(AssignmentKey(entry.Value.Assignment!)) ? "source-lost" : "explicit-end";
            await Remove(entry.Key);
        }
        foreach(var entry in wanted) if(!streams.ContainsKey(entry.Key)) {
            var a=entry.Value.Assignment;
            streams[entry.Key]=new AudioStream(a.SessionId,a.ConnectionId,entry.Value.Stream,model,a,s=>delivery.Submit(s));
            Load.Change(1);
            observed.Add(AssignmentKey(a));
        }        await Parallel.ForEachAsync(streams.ToArray(),
            new ParallelOptions { MaxDegreeOfParallelism = 4, CancellationToken = stop }, async (entry, token) => {
                var s = entry.Value;
                try {
                    if (s.ProcessingFailed) { await Remove(entry.Key); return; }
                    using var response = await http.PutAsJsonAsync("bot-audio/" + s.LeaseId,
                        new { sessionId=s.SessionId, connectionId=s.ConnectionId, streamId=s.StreamId, port=s.Port }, token);
                    response.EnsureSuccessStatusCode();
                    using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync(token));
                    s.TapState = json.RootElement.GetProperty("state").GetString()!;
                    if (json.RootElement.TryGetProperty("sourcePort", out var port)) s.ExpectedSourcePort = port.GetInt32();
                    if (s.TapState == "failed") await Remove(entry.Key);
                } catch (OperationCanceledException) when (token.IsCancellationRequested) { }
                catch (Exception) { Interlocked.Increment(ref controlErrors); await Remove(entry.Key); }
            });
    }
    async Task Remove(string key)
    {
        if (!streams.TryRemove(key, out var stream)) return;
        Load.Change(-1);
        await stream.DisposeAsync();
        try { using var response = await http.DeleteAsync("bot-audio/" + stream.LeaseId); }
        catch (Exception) { /* server lease expires even after kill -9 */ }
    }
    async Task RemoveAll()
    {
        await Parallel.ForEachAsync(streams.Keys.ToArray(), new ParallelOptions { MaxDegreeOfParallelism=4 },
            async (key, _) => await Remove(key));
    }
}