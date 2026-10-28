using System.Text;
namespace BotAudio;

public sealed record ListeningAssignment(string SessionId, int UserId, string AnalysisId, string ConnectionId,
    string Mode, string SttMode, string Language, double PauseSeconds = 3, long ExpiresAt = long.MaxValue);
public sealed record AudioRange(int StartSample, int EndSample);
public sealed record SpeechResult(string ResultId, ListeningAssignment Assignment, int Segment,
    string Reason, string Coverage, double AnalyzedSeconds, double SpokenSeconds,
    double? SpeechPercentage, string SttStatus, string? Text = null, double PreparedSeconds = 0, double? SttElapsedMilliseconds = null);
public sealed record PreparedSpeech(SpeechResult Result, byte[] Wav, AudioRange[] Ranges, byte[]? OriginalWav = null, int ContinuityGaps = 0, object? Transport = null);

// Called on the stream processing lane; no network or filesystem work here.
public sealed class SpeechAccumulator
{
    const int Rate = 8000, Frame = 256, Padding = 2560;
    readonly ListeningAssignment assignment;
    readonly Action<PreparedSpeech> emit;
    readonly List<short> samples = new();
    readonly List<short> original = new();
    int continuityGaps;
    readonly List<AudioRange> speech = new();
    int analyzed, voiced, quiet, segment, runStart = -1;
    bool partial = true; // Startup/tap negotiation has unobserved time.
    public SpeechAccumulator(ListeningAssignment assignment, Action<PreparedSpeech> emit)
    { this.assignment = assignment; this.emit = emit; }
    // Capture decoded RTP before VAD framing, trimming or discarded partial frames.
    public void CaptureDecoded(byte[] payload) {
        if (assignment.SttMode != "dry-run" && Environment.GetEnvironmentVariable("BOT_AUDIO_SAVE_DIAGNOSTICS") != "1") return;
        foreach (byte value in payload) original.Add((short)Math.Clamp(PcmuRtp.Decode(value)*32768, short.MinValue, short.MaxValue));
    }
    public void Gap() { continuityGaps++; partial = true; CloseRun(); quiet = 0; }
    public void Push(float[] pcm, bool speaking)
    {
        if (assignment.SttMode != "vad-only")
            foreach (var value in pcm) samples.Add((short)Math.Clamp(value * 32768, short.MinValue, short.MaxValue));
        if (speaking) { if (runStart < 0) runStart = analyzed; voiced += pcm.Length; quiet = 0; }
        else { CloseRun(); quiet += pcm.Length; }
        analyzed += pcm.Length;
        if (assignment.Mode == "pause-delimited" && voiced > 0 && quiet >= assignment.PauseSeconds * Rate)
            Finish("pause");
        // Bound buffers even for a stuck explicit-end assignment. A continuation is explicit in the result.
        else if (analyzed >= 180 * Rate) Finish("duration-limit");
    }
    void CloseRun() { if (runStart >= 0) { speech.Add(new(runStart, analyzed)); runStart = -1; } }
    public void Finish(string reason)
    {
        CloseRun();
        var ranges = new List<AudioRange>();
        foreach (var span in speech)
        {
            var padded = new AudioRange(Math.Max(0, span.StartSample - Padding), Math.Min(analyzed, span.EndSample + Padding));
            if (ranges.Count > 0 && padded.StartSample <= ranges[^1].EndSample)
                ranges[^1] = ranges[^1] with { EndSample = Math.Max(ranges[^1].EndSample, padded.EndSample) };
            else ranges.Add(padded);
        }
        byte[] wav = Array.Empty<byte>();
        if (samples.Count > 0 && voiced > 0) wav = Wave(ranges.SelectMany(r => samples.Skip(r.StartSample).Take(r.EndSample-r.StartSample)).ToArray());
        var result = new SpeechResult(Guid.NewGuid().ToString("N"), assignment, ++segment, reason,
            analyzed == 0 ? "unavailable" : partial ? "partial" : "observed", analyzed / (double)Rate,
            voiced / (double)Rate, analyzed == 0 ? null : 100.0 * voiced / analyzed,
            assignment.SttMode == "vad-only" ? "not-requested" : voiced == 0 ? "no-speech" : "pending");
        result = result with { PreparedSeconds = Math.Max(0, wav.Length - 44) / 16000.0 };
        emit(new(result, wav, ranges.ToArray(), original.Count > 0 ? Wave(original.ToArray()) : null, continuityGaps));
        original.Clear(); continuityGaps = 0;
        samples.Clear(); speech.Clear(); analyzed = voiced = quiet = 0; partial = false;
    }
    public static byte[] Wave(short[] pcm)
    {
        using var stream = new MemoryStream(); using var w = new BinaryWriter(stream, Encoding.ASCII, true);
        w.Write(Encoding.ASCII.GetBytes("RIFF")); w.Write(36 + pcm.Length * 2); w.Write(Encoding.ASCII.GetBytes("WAVEfmt "));
        w.Write(16); w.Write((short)1); w.Write((short)1); w.Write(Rate); w.Write(Rate*2); w.Write((short)2); w.Write((short)16);
        w.Write(Encoding.ASCII.GetBytes("data")); w.Write(pcm.Length*2); foreach (short s in pcm) w.Write(s);
        return stream.ToArray();
    }
}
