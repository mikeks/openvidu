using System.Diagnostics;
namespace BotAudio;

// Integrates concurrent streams over monotonic time; only the health collector resets a window.
public sealed class StreamLoad
{
    readonly object gate = new();
    readonly Func<double> clock;
    double started, updated, streamSeconds;
    int current, peak;
    public StreamLoad(Func<double>? clock = null)
    {
        this.clock = clock ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);
        started = updated = this.clock();
    }
    void Advance(double now) { streamSeconds += current * (now - updated); updated = now; }
    public void Change(int delta)
    {
        lock (gate) {
            if (current + delta < 0) throw new InvalidOperationException("Negative stream count");
            Advance(clock()); current += delta; peak = Math.Max(peak, current);
        }
    }
    public (int Current, double? Average, int Peak, double Seconds) Sample(bool reset)
    {
        lock (gate) {
            double now = clock(); Advance(now);
            double seconds = now - started;
            var result = (current, seconds > 0 ? (double?)(streamSeconds / seconds) : null, peak, seconds);
            if (reset) { started = now; streamSeconds = 0; peak = current; }
            return result;
        }
    }
}
