using System.Net;
using System.Text.Json;
namespace BotAudio;
public static class SpeechAnalysisTest
{
    static void Check(bool ok,string label) { if(!ok) throw new Exception(label); }
    static void Feed(SpeechAccumulator a,int frames,bool speech) { for(int n=0;n<frames;n++) a.Push(Enumerable.Repeat(speech?.2f:0,256).ToArray(),speech); }
    sealed class FakeAzure : HttpMessageHandler
    {
        public int Calls, Callbacks; public byte[]? Audio;
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,CancellationToken stop) {
            if(request.RequestUri!.AbsolutePath=="/result") {
                Callbacks++;
                Check(request.Headers.Authorization?.Scheme=="Bearer","result authentication");
                using var json=JsonDocument.Parse(await request.Content!.ReadAsStringAsync(stop));
                Check(json.RootElement.GetProperty("assignment").GetProperty("userId").GetInt32()==42,"result attribution");
                return new(Callbacks==1?HttpStatusCode.ServiceUnavailable:HttpStatusCode.OK) { Content=new StringContent("{\"accepted\":true}") };
            }
            Calls++;
            Check(request.RequestUri!.Scheme=="https" && request.Headers.Contains("Ocp-Apim-Subscription-Key"),"Azure credentials in HTTPS header");
            var parts=(MultipartFormDataContent)request.Content!;
            Audio=await parts.First().ReadAsByteArrayAsync(stop);
            return new(HttpStatusCode.OK) { Content=new StringContent("{\"combinedPhrases\":[{\"text\":\"Test transcript.\"}]}") };
        }
    }
    public static void Run()
    {
        var results=new List<PreparedSpeech>();
        var assignment=new ListeningAssignment("MafiaRoom1",42,"state-7","human","explicit-end","dry-run","en-US");
        var a=new SpeechAccumulator(assignment,results.Add);
        Feed(a,32,false); Feed(a,32,true); Feed(a,160,false); Feed(a,32,true); Feed(a,32,false);
        Check(results.Count==0,"explicit-end never splits on pauses"); a.Finish("explicit-end");
        var prepared=results.Single();
        Check(prepared.Ranges.Length==2,"two separate speech ranges retained");
        Check(prepared.Result.PreparedSeconds == (prepared.Wav.Length-44)/16000.0,"submitted audio duration includes retained padding");
        Check(prepared.Wav.Length==44+prepared.Ranges.Sum(r=>(r.EndSample-r.StartSample)*2),"WAV bytes match original range map");
        Check(Math.Abs(prepared.Result.SpokenSeconds-64*.032)<1e-9 && Math.Abs(prepared.Result.AnalyzedSeconds-288*.032)<1e-9,"VAD seconds");
        Check(prepared.Wav.Length<288*512,"silence trimming reduces upload");
        Check(System.Text.Encoding.ASCII.GetString(prepared.Wav,0,4)=="RIFF","valid WAV header");
        results.Clear(); a=new(assignment with { Mode="pause-delimited" },results.Add);
        Feed(a,32,true); Feed(a,93,false); Check(results.Count==0,"less than three seconds keeps listening");
        Feed(a,1,false); Check(results.Count==1 && results[0].Result.Reason=="pause","three seconds ends utterance");
        Feed(a,32,true); a.Gap(); Check(results.Count==1,"missing audio is not a pause"); a.Finish("source-lost");
        Check(results.Count==2 && results[1].Result.Coverage=="partial","interruption explicit and partial");
        results.Clear(); a=new(assignment with { SttMode="vad-only" },results.Add);
        Feed(a,32,true); a.Finish("explicit-end"); Check(results.Single().Wav.Length==0 && results[0].Result.SttStatus=="not-requested","VAD-only retains no PCM");
        results.Clear(); a=new(assignment,results.Add); a.Finish("explicit-end");
        Check(results.Single().Result.Coverage=="unavailable" && results[0].Result.SpeechPercentage==null,"no audio is unknown");
        results.Clear(); a=new(assignment,results.Add);
        a.CaptureDecoded(Enumerable.Repeat((byte)0xff,160).ToArray());
        a.Gap(); a.Finish("explicit-end");
        var silentOriginal=results.Single();
        Check(silentOriginal.OriginalWav?.Length==364 && silentOriginal.Wav.Length==0,"original keeps sub-frame audio even with no detected speech");
        Check(silentOriginal.ContinuityGaps==1,"continuity gaps recorded");
        var oldDiagnostics=Environment.GetEnvironmentVariable("BOT_AUDIO_SAVE_DIAGNOSTICS");
        try {
            Environment.SetEnvironmentVariable("BOT_AUDIO_SAVE_DIAGNOSTICS","1");
            results.Clear(); a=new(assignment with { SttMode="azure" },results.Add);
            a.CaptureDecoded(Enumerable.Repeat((byte)0xff,160).ToArray()); a.Finish("explicit-end");
            Check(results.Single().OriginalWav?.Length==364 && results[0].Result.SttStatus=="no-speech","Azure diagnostic retains silent original");
        } finally { Environment.SetEnvironmentVariable("BOT_AUDIO_SAVE_DIAGNOSTICS",oldDiagnostics); }
        string dir=Path.Combine(Path.GetTempPath(),"bot-audio-test-"+Guid.NewGuid().ToString("N"));
        var oldDir=Environment.GetEnvironmentVariable("BOT_AUDIO_DRY_RUN_DIR");
        var oldRegion=Environment.GetEnvironmentVariable("BOT_AUDIO_AZURE_REGION");
        var oldKey=Environment.GetEnvironmentVariable("BOT_AUDIO_AZURE_KEY");
        var oldResult=Environment.GetEnvironmentVariable("BOT_AUDIO_RESULT_URL");
        var oldSecret=Environment.GetEnvironmentVariable("BOT_AUDIO_OPENVIDU_SECRET");
        try {
            Environment.SetEnvironmentVariable("BOT_AUDIO_DRY_RUN_DIR",dir);
            Environment.SetEnvironmentVariable("BOT_AUDIO_AZURE_REGION","test"); Environment.SetEnvironmentVariable("BOT_AUDIO_AZURE_KEY","test");
            var handler=new FakeAzure(); using var delivery=new SpeechDelivery(handler);
            var dry=delivery.Recognize(prepared,CancellationToken.None).GetAwaiter().GetResult();
            Check(dry.SttStatus=="dry-run" && handler.Calls==0,"dry run makes no HTTP request");
            Check(File.ReadAllBytes(Directory.GetFiles(dir,"*.wav").Single()).SequenceEqual(prepared.Wav),"dry-run saves exact prepared bytes");
            var silent=delivery.Recognize(silentOriginal,CancellationToken.None).GetAwaiter().GetResult();
            Check(handler.Calls==0 && Directory.GetFiles(dir,"*.original.wav").Length==1,"dry-run saves original even without speech");
            Check(File.ReadAllBytes(Directory.GetFiles(dir,"*.original.wav").Single()).SequenceEqual(silentOriginal.OriginalWav!),"original bytes preserved");
            var cloud=prepared with { Result=prepared.Result with { Assignment=assignment with { SttMode="azure" } } };
            var transcript=delivery.Recognize(cloud,CancellationToken.None).GetAwaiter().GetResult();
            Check(transcript.SttStatus=="succeeded" && transcript.Text=="Test transcript." && handler.Audio!.SequenceEqual(prepared.Wav),"Azure receives identical dry-run audio and parses text");
            var silentCloud = cloud with { Wav = SpeechAccumulator.Wave(new short[8000]) };
            int paidBefore = handler.Calls;
            var rejectedSilent = delivery.Recognize(silentCloud,CancellationToken.None).GetAwaiter().GetResult();
            Check(rejectedSilent.SttStatus=="no-speech" && handler.Calls==paidBefore,"near-silent upload never calls Azure");
            Environment.SetEnvironmentVariable("BOT_AUDIO_RESULT_URL","https://observer-test.invalid/result");
            Environment.SetEnvironmentVariable("BOT_AUDIO_OPENVIDU_SECRET","test");
            delivery.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
            Check(delivery.Submit(cloud),"result admitted");
            for(int n=0;n<100 && Interlocked.Read(ref delivery.Delivered)==0;n++) Thread.Sleep(50);
            delivery.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
            Check(delivery.Delivered==1 && handler.Callbacks==2 && handler.Calls==2,"callback retry does not repeat paid transcription");
        } finally {
            Environment.SetEnvironmentVariable("BOT_AUDIO_DRY_RUN_DIR",oldDir); Environment.SetEnvironmentVariable("BOT_AUDIO_AZURE_REGION",oldRegion); Environment.SetEnvironmentVariable("BOT_AUDIO_AZURE_KEY",oldKey);
            Environment.SetEnvironmentVariable("BOT_AUDIO_RESULT_URL",oldResult); Environment.SetEnvironmentVariable("BOT_AUDIO_OPENVIDU_SECRET",oldSecret);
        }
        Console.WriteLine("PASS: segmentation, trimming, coverage, dry-run isolation, WAV parity and mock Azure transcription.");
    }
}
