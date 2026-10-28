using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace BotAudio;

public static class SelfTest
{
    static void Check(bool ok, string message) { if (!ok) throw new Exception(message); }
    public static byte[] Packet(ushort sequence, uint timestamp, byte fill=255)
    {
        var bytes = new byte[172];
        bytes[0]=128; bytes[1]=0;
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(2),sequence);
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(4),timestamp);
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(8),123);
        Array.Fill(bytes,fill,12,160);
        return bytes;
    }
    public static void Run()
    {
        SpeechAnalysisTest.Run();
        var tiny = Enumerable.Repeat(2f/32768,256).ToArray();
        var quietVoice = Enumerable.Repeat(.001f,256).ToArray(); // -60 dBFS
        Check(!SpeechEnergy.IsAudible(tiny) && SpeechEnergy.IsAudible(quietVoice), "near-silence energy boundary");
        var energyGate = new SpeechGate();
        for(int i=0;i<20;i++) energyGate.Push(SpeechEnergy.IsAudible(tiny) ? .99f : 0);
        Check(!energyGate.Speaking, "high model confidence cannot turn near-silence into speech");
        Check(!SpeechEnergy.HasAudibleFrame(SpeechAccumulator.Wave(new short[8000])), "silent upload rejected");
        Check(SpeechEnergy.HasAudibleFrame(SpeechAccumulator.Wave(Enumerable.Repeat((short)33,256).ToArray())), "quiet upload retained");
        Check(PcmuRtp.Parse(new byte[10])==null,"short packet");
        var p=Packet(65535,uint.MaxValue-159);
        Check(PcmuRtp.Parse(p)!.Payload.Length==160,"valid RTP");
        p[1]=96; Check(PcmuRtp.Parse(p)==null,"unexpected codec"); p[1]=0;
        p[0]=144;p[14]=255;p[15]=255;Check(PcmuRtp.Parse(p)==null,"oversized extension");
        p=Packet(0,0);p[0]=160;p[^1]=0;Check(PcmuRtp.Parse(p)==null,"invalid padding");
        Check(PcmuRtp.Decode(255)==0 && PcmuRtp.Decode(127)==0,"PCMU silence");
        Check(PcmuRtp.Decode(0)<-.9 && PcmuRtp.Decode(128)>.9,"PCMU signs");
        var gate=new SpeechGate();
        for(int i=0;i<7;i++) gate.Push(.9f);
        Check(!gate.Speaking && gate.SpeechFrames==0,"short noise must not open speech");
        gate.Push(.9f); Check(gate.Speaking && gate.SpeechFrames==8,"minimum speech");
        for(int i=0;i<10;i++)gate.Push(0);
        Check(!gate.Speaking && gate.SpeechFrames==8,"hysteresis exit");
        gate.Push(.9f);gate.Reset();Check(!gate.Speaking,"gap resets observation");

        string path=Environment.GetEnvironmentVariable("BOT_AUDIO_MODEL")??"models/silero_vad.onnx";
        using var model=new VadModel(path);
        var state=new VadState();
        for(int i=0;i<100;i++) Check(model.Predict(new float[256],state)<.5,"silence");
        var noise=new Random(123);
        state=new();
        var noiseGate=new SpeechGate();
        for(int i=0;i<100;i++)
            noiseGate.Push(model.Predict(Enumerable.Range(0,256).Select(_=>(float)(noise.NextDouble()-.5)*.05f).ToArray(),state));
        Check(noiseGate.SpeechFrames==0,"synthetic white noise");
        UdpTest(model).GetAwaiter().GetResult();
        Console.WriteLine("PASS: RTP validation, PCMU decode, speech hysteresis, real ONNX silence/noise, UDP ingestion, gap reset, stream isolation and shutdown.");
    }
    static async Task UdpTest(VadModel model)
    {
        await using var a=new AudioStream("test","a","s1",model);
        await using var b=new AudioStream("test","b","s2",model);
        using var sender=new UdpClient(new IPEndPoint(IPAddress.Loopback,0));
        a.ExpectedSourcePort=((IPEndPoint)sender.Client.LocalEndPoint!).Port;
        for(ushort i=0;i<15;i++) {
            byte[] packet=Packet(i,(uint)i*160);
            await sender.SendAsync(packet,new IPEndPoint(IPAddress.Loopback,a.Port));
            await Task.Delay(20);
        }
        await Task.Delay(150);
        Check(a.Packets==15 && a.Frames>0 && b.Packets==0,"UDP isolation");
        await sender.SendAsync(Packet(30,4800),new IPEndPoint(IPAddress.Loopback,a.Port));
        await Task.Delay(100);
        Check(a.Resets>=2,"lost frames reset state");
        Check(a.SpeechFrames==0,"silent stream");
        await Task.Delay(1100);
        using var snapshot=JsonDocument.Parse(JsonSerializer.Serialize(a.Snapshot()));
        Check(snapshot.RootElement.GetProperty("observation").GetString()=="unknown","missing audio is unknown");
    }
}