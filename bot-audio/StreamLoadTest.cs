namespace BotAudio;
public static class StreamLoadTest
{
    public static void Run()
    {
        double now = 0; var load = new StreamLoad(() => now);
        void Check(bool ok) { if (!ok) throw new Exception("Stream load test failed"); }
        Check(load.Sample(false).Average == null);
        now = 30; load.Change(1); now = 60; load.Change(1); now = 90; load.Change(-2); now = 120;
        var a = load.Sample(false);
        Check(a.Current == 0 && a.Peak == 2 && a.Average == .75 && a.Seconds == 120);
        Check(load.Sample(true) == a); // Diagnostic reads must not consume the health window.
        now = 240; a = load.Sample(true); Check(a.Average == 0 && a.Peak == 0);
        load.Change(3); now = 250; a = load.Sample(true); Check(a.Average == 3 && a.Peak == 3);
        now = 260; a = load.Sample(true); Check(a.Average == 3 && a.Peak == 3); // Active baseline retained.
    }
}
