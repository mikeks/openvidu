using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
namespace BotAudio;
public sealed class SpeechDelivery : BackgroundService
{
    readonly Channel<PreparedSpeech> pending = Channel.CreateBounded<PreparedSpeech>(new BoundedChannelOptions(32) { FullMode=BoundedChannelFullMode.Wait });
    readonly HttpClient http;
    public SpeechDelivery() : this(new HttpClientHandler { AllowAutoRedirect=false }) { }
    internal SpeechDelivery(HttpMessageHandler handler) { http=new HttpClient(handler) { Timeout=TimeSpan.FromSeconds(60) }; }
    public long Rejected, Delivered, Failed, SttFailed;
    public bool Submit(PreparedSpeech speech) { if(pending.Writer.TryWrite(speech)) return true; Interlocked.Increment(ref Rejected); return false; }
    protected override async Task ExecuteAsync(CancellationToken stop)
    {
        try { await foreach(var speech in pending.Reader.ReadAllAsync(stop)) {
            var result=speech.Result;
            result=await Recognize(speech,stop);
            if (speech.Result.Assignment.SttMode != "dry-run" && Environment.GetEnvironmentVariable("BOT_AUDIO_SAVE_DIAGNOSTICS") == "1") {
                try { SaveDryRun(speech with { Result = result }); }
                catch (Exception ex) { Console.Error.WriteLine("Audio diagnostic save failed: " + ex.GetType().Name); }
            }
            // Retry delivery only: a callback failure must not repeat a paid transcription.
            bool accepted=false;
            for(int attempt=0;attempt<8 && !accepted;attempt++) {
                try {
                    var uri=new Uri(Environment.GetEnvironmentVariable("BOT_AUDIO_RESULT_URL") ?? "");
                    if(uri.Scheme!="https") throw new InvalidOperationException("HTTPS required");
                    using var request=new HttpRequestMessage(HttpMethod.Post,uri);
                    request.Headers.Authorization=new AuthenticationHeaderValue("Bearer",Environment.GetEnvironmentVariable("BOT_AUDIO_OPENVIDU_SECRET"));
                    request.Content=JsonContent.Create(result);
                    using var timeout=CancellationTokenSource.CreateLinkedTokenSource(stop); timeout.CancelAfter(TimeSpan.FromSeconds(5));
                    using var response=await http.SendAsync(request,timeout.Token);
                    if(response.IsSuccessStatusCode) {
                        using var ack=JsonDocument.Parse(await response.Content.ReadAsStringAsync(stop));
                        accepted=ack.RootElement.TryGetProperty("accepted",out var ok) && ok.GetBoolean();
                    }
                } catch(OperationCanceledException) when(stop.IsCancellationRequested) { throw; }
                catch(Exception) { }
                if(!accepted) await Task.Delay(TimeSpan.FromSeconds(Math.Min(30,attempt*3+1)),stop);
            }
            if(accepted) Interlocked.Increment(ref Delivered); else Interlocked.Increment(ref Failed);
        } } catch(OperationCanceledException) when(stop.IsCancellationRequested) { }
    }
    internal async Task<SpeechResult> Recognize(PreparedSpeech speech,CancellationToken stop)
    {
        var result=speech.Result;
        if(result.SttStatus!="pending" && result.Assignment.SttMode!="dry-run") return result;
        try {
            if(result.Assignment.SttMode=="dry-run") { SaveDryRun(speech); return result with { SttStatus="dry-run" }; }
            if(result.Assignment.SttMode!="azure") throw new InvalidOperationException("Unknown STT mode");
            // Defense in depth: never pay to transcribe a near-silent prepared segment.
            if (!SpeechEnergy.HasAudibleFrame(speech.Wav)) return result with { SttStatus="no-speech" };
            var timer = System.Diagnostics.Stopwatch.StartNew();
            try { return result with { Text=await Transcribe(speech,stop), SttStatus="succeeded", SttElapsedMilliseconds=timer.Elapsed.TotalMilliseconds }; }
            catch (OperationCanceledException) when (stop.IsCancellationRequested) { throw; }
            catch { Interlocked.Increment(ref SttFailed); return result with { SttStatus="failed", SttElapsedMilliseconds=timer.Elapsed.TotalMilliseconds }; }
        } catch(OperationCanceledException) when(stop.IsCancellationRequested) { throw; }
        catch(Exception) { Interlocked.Increment(ref SttFailed); return result with { SttStatus="failed" }; }
    }    public static void SaveDryRun(PreparedSpeech speech)
    {
        string root=Environment.GetEnvironmentVariable("BOT_AUDIO_DRY_RUN_DIR") ?? "/data/stt-dry-run";
        Directory.CreateDirectory(root);
        const long budget=256L*1024*1024;
        var files=new DirectoryInfo(root).GetFiles().Where(f=>f.Extension is ".wav" or ".json").OrderBy(f=>f.LastWriteTimeUtc).ToList();
        long size=files.Sum(f=>f.Length);
        foreach(var file in files) {
            if(file.LastWriteTimeUtc<DateTime.UtcNow.AddHours(-24) || size+speech.Wav.Length+(speech.OriginalWav?.Length ?? 0)+8192>budget) { size-=file.Length; file.Delete(); }
        }
        string name=DateTime.UtcNow.ToString("yyyyMMddTHHmmssfff")+"-"+speech.Result.ResultId;
        if(speech.Wav.Length > 0) File.WriteAllBytes(Path.Combine(root,name+".wav"),speech.Wav);
        if(speech.OriginalWav != null) File.WriteAllBytes(Path.Combine(root,name+".original.wav"),speech.OriginalWav);
        File.WriteAllText(Path.Combine(root,name+".json"),JsonSerializer.Serialize(new {
            result=speech.Result, sampleRate=8000, preparedSeconds=Math.Max(0,speech.Wav.Length-44)/16000.0,
            originalSeconds=speech.OriginalWav == null ? 0 : (speech.OriginalWav.Length-44)/16000.0,
            speech.ContinuityGaps, speech.Transport, originalFormat="Decoded PCMU before VAD framing; includes silence; missing packets are not reconstructed",
            ranges=speech.Ranges, rangeUnits="original decoded PCM samples; gaps are not reconstructed", paddingMilliseconds=320
        },new JsonSerializerOptions { WriteIndented=true }));
    }
    async Task<string> Transcribe(PreparedSpeech speech,CancellationToken stop)
    {
        string region=Environment.GetEnvironmentVariable("BOT_AUDIO_AZURE_REGION") ?? "";
        if(!System.Text.RegularExpressions.Regex.IsMatch(region,"^[a-z0-9]+$")) throw new InvalidOperationException("Azure region required");
        string key=Environment.GetEnvironmentVariable("BOT_AUDIO_AZURE_KEY") ?? throw new InvalidOperationException("Azure key required");
        using var request=new HttpRequestMessage(HttpMethod.Post,$"https://{region}.api.cognitive.microsoft.com/speechtotext/transcriptions:transcribe?api-version=2025-10-15");
        request.Headers.Add("Ocp-Apim-Subscription-Key",key);
        using var multipart=new MultipartFormDataContent();
        var audio=new ByteArrayContent(speech.Wav); audio.Headers.ContentType=new MediaTypeHeaderValue("audio/wav");
        multipart.Add(audio,"audio","speech.wav");
        multipart.Add(new StringContent(JsonSerializer.Serialize(new { locales=new[]{speech.Result.Assignment.Language} }),Encoding.UTF8,"application/json"),"definition");
        request.Content=multipart;
        using var response=await http.SendAsync(request,stop); response.EnsureSuccessStatusCode();
        using var json=JsonDocument.Parse(await response.Content.ReadAsStringAsync(stop));
        return string.Join(" ",json.RootElement.GetProperty("combinedPhrases").EnumerateArray().Select(p=>p.GetProperty("text").GetString()));
    }
    public override void Dispose() { http.Dispose(); base.Dispose(); }
}
