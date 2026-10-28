using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;

namespace BotAudio;

public sealed class AudioStream : IAsyncDisposable
{
    public string LeaseId { get; } = Guid.NewGuid().ToString("N");
    public string SessionId { get; }
    public string ConnectionId { get; }
    public string StreamId { get; }
    public int Port { get; }
    public volatile int ExpectedSourcePort;
    public volatile string TapState = "pending";
    public long Packets, Dropped, Invalid, Resets, Frames, SpeechFrames;
    public volatile bool Speaking; public volatile bool ProcessingFailed;
    long lastPacket, lastProcessed;
    readonly UdpClient udp;
    readonly CancellationTokenSource stop = new();
    readonly Channel<(byte[] Bytes, long Received)> queue = Channel.CreateBounded<(byte[],long)>(
        new BoundedChannelOptions(32) { FullMode = BoundedChannelFullMode.Wait, SingleReader = true, SingleWriter = true });
    readonly Task receive, process;
    readonly SpeechAccumulator? analysis;
    public string FinishReason = "interrupted";
    public ListeningAssignment? Assignment { get; }
    public AudioStream(string session, string connection, string stream, VadModel model, ListeningAssignment? assignment = null, Action<PreparedSpeech>? emit = null)
    {
        Assignment = assignment;
        if (assignment != null && emit != null) analysis = new SpeechAccumulator(assignment, speech => emit(speech with { Transport = Snapshot() }));
        SessionId = session; ConnectionId = connection; StreamId = stream;
        for (int port = 58000; port <= 58998; port += 2)
        {
            try { udp = new UdpClient(new IPEndPoint(IPAddress.Loopback, port)); Port = port; break; }
            catch (SocketException) { }
        }
        if (udp == null) throw new InvalidOperationException("No free audio tap port");
        receive = Receive();
        process = Process(model);
    }
    async Task Receive()
    {
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var data = await udp.ReceiveAsync(stop.Token);
                if (!IPAddress.IsLoopback(data.RemoteEndPoint.Address) || data.RemoteEndPoint.Port != ExpectedSourcePort) { Interlocked.Increment(ref Invalid); continue; }
                long now = Stopwatch.GetTimestamp();
                Interlocked.Exchange(ref lastPacket, now);
                Interlocked.Increment(ref Packets);
                if (data.Buffer.Length > 2048 || !queue.Writer.TryWrite((data.Buffer, now)))
                    Interlocked.Increment(ref Dropped);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception) { ProcessingFailed = true; } finally { queue.Writer.TryComplete(); }
    }
    async Task Process(VadModel model)
    {
        var state = new VadState();
        var gate = new SpeechGate();
        var frame = new float[256];
        int used = 0;
        RtpPacket? previous = null;
        long previousTime = 0;
        try
        {
            await foreach (var item in queue.Reader.ReadAllAsync())
            {
                if (Stopwatch.GetElapsedTime(item.Received).TotalMilliseconds > 500)
                {
                    Interlocked.Increment(ref Dropped); previous = null; used = 0;
                    state = new(); gate.Reset(); Speaking = false; analysis?.Gap(); continue;
                }
                var packet = PcmuRtp.Parse(item.Bytes);
                if (packet == null) { Interlocked.Increment(ref Invalid); continue; }
                bool gap = previous == null || packet.Ssrc != previous.Ssrc
                    || packet.Sequence != unchecked((ushort)(previous.Sequence + 1))
                    || packet.Timestamp != unchecked(previous.Timestamp + (uint)previous.Payload.Length)
                    || Stopwatch.GetElapsedTime(previousTime, item.Received).TotalMilliseconds > 500;
                if (gap) { analysis?.Gap(); used = 0; state = new(); gate.Reset(); Speaking = false; Interlocked.Increment(ref Resets); }
                previous = packet; previousTime = item.Received;
                analysis?.CaptureDecoded(packet.Payload);
                foreach (byte sample in packet.Payload)
                {
                    frame[used++] = PcmuRtp.Decode(sample);
                    if (used != 256) continue;
                    await model.Lane.WaitAsync();
                    try {
                        // Always update recurrent state, including during silence.
                        float probability = model.Predict(frame, state);
                        gate.Push(SpeechEnergy.IsAudible(frame) ? probability : 0);
                    }
                    finally { model.Lane.Release(); }
                    used = 0;
                    Interlocked.Increment(ref Frames);
                    // Gate total is cumulative even when input continuity is reset.
                    Interlocked.Exchange(ref SpeechFrames, gate.SpeechFrames);
                    Speaking = gate.Speaking;
                    analysis?.Push(frame, Speaking);
                    Interlocked.Exchange(ref lastProcessed, Stopwatch.GetTimestamp());
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception) { ProcessingFailed = true; }
    }
    public object Snapshot()
    {
        long received = Interlocked.Read(ref lastPacket), processed = Interlocked.Read(ref lastProcessed);
        bool current = processed != 0 && Stopwatch.GetElapsedTime(processed).TotalSeconds < 1;
        return new { sessionId = SessionId, connectionId = ConnectionId, streamId = StreamId,
            leaseId = LeaseId, state = ProcessingFailed ? "processing-failed" : TapState,
            observation = ProcessingFailed || TapState != "active" || !current ? "unknown" : Speaking ? "speech" : "no-speech",
            packets = Interlocked.Read(ref Packets), droppedPackets = Interlocked.Read(ref Dropped),
            invalidPackets = Interlocked.Read(ref Invalid), continuityResets = Interlocked.Read(ref Resets),
            analyzedSeconds = Interlocked.Read(ref Frames) * .032,
            speechSeconds = Interlocked.Read(ref SpeechFrames) * .032,
            lastPacketAgeSeconds = received == 0 ? (double?)null : Stopwatch.GetElapsedTime(received).TotalSeconds };
    }
    public async ValueTask DisposeAsync()
    {
        stop.Cancel(); udp.Dispose();
        await Task.WhenAll(receive, process);
        analysis?.Gap();
        analysis?.Finish(FinishReason);
        stop.Dispose();
    }
}