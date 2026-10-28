using System.Buffers.Binary;
using Microsoft.ML.OnnxRuntime;
using Microsoft.ML.OnnxRuntime.Tensors;

namespace BotAudio;

// Each stream owns recurrent state. One bounded CPU inference lane is shared.
public sealed class VadModel : IDisposable
{
    readonly InferenceSession session;
    public readonly SemaphoreSlim Lane = new(1);
    public VadModel(string path)
    {
        using var options = new Microsoft.ML.OnnxRuntime.SessionOptions { IntraOpNumThreads = 1, InterOpNumThreads = 1 };
        session = new InferenceSession(path, options);
    }
    public float Predict(float[] samples, VadState state)
    {
        var input = new float[288];
        state.Context.CopyTo(input, 0);
        samples.CopyTo(input, 32);
        var values = new[] {
            NamedOnnxValue.CreateFromTensor("input", new DenseTensor<float>(input, new[]{1,288})),
            NamedOnnxValue.CreateFromTensor("state", new DenseTensor<float>(state.Memory, new[]{2,1,128})),
            NamedOnnxValue.CreateFromTensor("sr", new DenseTensor<long>(new long[]{8000}, Array.Empty<int>()))
        };
        using var result = session.Run(values);
        state.Memory = result.First(x => x.Name == "stateN").AsTensor<float>().ToArray();
        Array.Copy(input, 256, state.Context, 0, 32);
        return result.First(x => x.Name == "output").AsTensor<float>().First();
    }
    public void Dispose() { session.Dispose(); Lane.Dispose(); }
}
public sealed class VadState
{
    public float[] Memory = new float[256], Context = new float[32];
}
public sealed class SpeechGate
{
    int candidate, quiet;
    public bool Speaking { get; private set; }
    public long SpeechFrames { get; private set; }
    public void Push(float probability)
    {
        if (probability >= (Speaking || candidate > 0 ? .35f : .5f))
        {
            quiet = 0;
            if (Speaking) SpeechFrames++;
            else if (++candidate >= 8) { Speaking = true; SpeechFrames += candidate; candidate = 0; }
        }
        else
        {
            candidate = 0;
            if (Speaking && ++quiet >= 10) { Speaking = false; quiet = 0; }
        }
    }
    public void Reset() { candidate = quiet = 0; Speaking = false; }
}
public record RtpPacket(ushort Sequence, uint Timestamp, uint Ssrc, byte[] Payload);
public static class PcmuRtp
{
    public static RtpPacket? Parse(byte[] bytes)
    {
        if (bytes.Length < 12 || bytes.Length > 2048 || bytes[0] >> 6 != 2 || (bytes[1] & 127) != 0) return null;
        int offset = 12 + 4 * (bytes[0] & 15);
        if (offset > bytes.Length) return null;
        if ((bytes[0] & 16) != 0)
        {
            if (offset + 4 > bytes.Length) return null;
            offset += 4 + 4 * BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(offset + 2));
        }
        int end = bytes.Length;
        if ((bytes[0] & 32) != 0)
        {
            int padding = bytes[^1];
            if (padding == 0) return null;
            end -= padding;
        }
        if (offset >= end || end - offset > 1600) return null;
        return new(BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(2)),
            BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(4)),
            BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(8)), bytes[offset..end]);
    }
    public static float Decode(byte encoded)
    {
        int u = (~encoded) & 255;
        int value = (((u & 15) << 3) + 132) << ((u >> 4) & 7);
        return ((u & 128) != 0 ? 132 - value : value - 132) / 32768f;
    }
}
// Reject near-silence, not ordinary background noise; Silero still decides speech.
public static class SpeechEnergy
{
    public const double MinimumMeanSquare = 1e-7; // -70 dBFS RMS
    public static bool IsAudible(ReadOnlySpan<float> samples)
    {
        if (samples.Length == 0) return false;
        double sum = 0;
        foreach (float sample in samples) sum += (double)sample * sample;
        return sum / samples.Length >= MinimumMeanSquare;
    }
    public static bool HasAudibleFrame(byte[] wav)
    {
        // Internally generated mono PCM16 WAV with its fixed 44-byte header.
        for (int start = 44; start + 1 < wav.Length; start += 512)
        {
            int end = Math.Min(start + 512, wav.Length); double sum = 0; int count = 0;
            for (int i = start; i + 1 < end; i += 2) {
                double sample = BinaryPrimitives.ReadInt16LittleEndian(wav.AsSpan(i, 2)) / 32768.0;
                sum += sample * sample; count++;
            }
            if (count > 0 && sum / count >= MinimumMeanSquare) return true;
        }
        return false;
    }
}
