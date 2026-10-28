package io.openvidu.server.rest;

import java.util.*;
import java.util.concurrent.*;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.kurento.client.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import io.openvidu.server.core.*;
import io.openvidu.server.kurento.core.KurentoParticipant;
import io.openvidu.server.kurento.endpoint.PublisherEndpoint;

/**
 * Optional leased AUDIO-only branch; never called by publish/subscribe/leave.
 * Kurento setup runs on a dedicated bounded executor. Only owned RTP endpoints
 * are released, never source publishers, pipelines or player connections.
 * Protected by the existing API ADMIN authentication.
 */
@RestController
@RequestMapping(RequestMappings.API + "/bot-audio")
public class BotAudioTapController {
    @Autowired private SessionManager sessions;
    @Autowired private Environment environment;
    private boolean enabled;
    private int maxTaps;
    private final Set<String> observedSessions = new HashSet<>();
    private long selectionRevision;
    @Autowired private io.openvidu.server.config.OpenviduConfig config;
    private final ScheduledExecutorService bootstrap = Executors.newSingleThreadScheduledExecutor(
            r -> daemon(r, "BotAudioBootstrap"));
    protected Session findSession(String id) { return sessions.getSessionWithNotActive(id); }
    private boolean eligible(String id) { return observedSessions.contains(id) && findSession(id) != null; }
    private List<Assignment> assignments = new ArrayList<>();
    public static class Assignment {
        public String sessionId, connectionId, analysisId, mode, sttMode, language;
        public int userId;
        public double pauseSeconds;
        public long expiresAt;
    }
    public static class Selection {
        public Long revision;
        public List<Assignment> assignments;
    }
    private boolean valid(Assignment a) {
        return a != null && a.userId>0 && a.sessionId!=null && a.sessionId.length()<=256
            && a.analysisId!=null && a.analysisId.length()<=128 && a.connectionId!=null && a.connectionId.length()<=256
            && ("explicit-end".equals(a.mode) || "pause-delimited".equals(a.mode))
            && Arrays.asList("vad-only","dry-run","azure").contains(a.sttMode)
            && a.language!=null && a.language.length()<=32 && a.pauseSeconds>=1 && a.pauseSeconds<=10;
    }
    private boolean selected(String session, String connection) {
        return assignments.stream().anyMatch(a -> a.expiresAt>System.currentTimeMillis() && a.sessionId.equals(session) && a.connectionId.equals(connection));
    }
    @PutMapping("/rooms")
    public synchronized ResponseEntity<?> selectRooms(@RequestBody Selection selection) {
        if (!enabled) return ResponseEntity.status(503).body("disabled");
        if(selection==null || selection.revision==null || selection.revision<=0 || selection.assignments==null
            || selection.assignments.size()>4096 || selection.assignments.stream().anyMatch(a -> !valid(a)))
            return ResponseEntity.badRequest().body("invalid-selection");
        Set<String> keys=new HashSet<>();
        for(Assignment a:selection.assignments) if(!keys.add(a.sessionId+"/"+a.userId)) return ResponseEntity.badRequest().body("duplicate-user");
        com.google.gson.Gson gson=new com.google.gson.Gson();
        if(selection.revision<selectionRevision || (selection.revision==selectionRevision
            && !gson.toJson(selection.assignments).equals(gson.toJson(assignments))))
            return ResponseEntity.status(409).body("stale-or-conflicting-selection");
        assignments=new ArrayList<>(selection.assignments);
        observedSessions.clear(); for(Assignment a:assignments) observedSessions.add(a.sessionId);
        selectionRevision=selection.revision; expire(); notifyAll();
        Map<String,Object> ack=new HashMap<>(); ack.put("accepted",true); ack.put("revision",selectionRevision);
        return ResponseEntity.ok(ack);
    }    @org.springframework.context.event.EventListener(org.springframework.boot.context.event.ApplicationReadyEvent.class)
    public void recoverSelection() {
        if (enabled) bootstrap.execute(this::fetchInitialSelection);
    }
    private void fetchInitialSelection() {
        java.net.HttpURLConnection connection = null;
        boolean recovered = false;
        try {
            String address = environment.getProperty("MAFIA_BOT_AUDIO_AS_URL", "");
            java.net.URL url = new java.net.URL(address);
            if (!"https".equals(url.getProtocol())) throw new IllegalArgumentException("HTTPS callback required");
            connection = (java.net.HttpURLConnection) url.openConnection();
            connection.setInstanceFollowRedirects(false);
            connection.setConnectTimeout(3000); connection.setReadTimeout(3000);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Authorization", "Bearer " + config.getOpenViduSecret());
            // IIS requires an explicit body length for POST, including an empty bootstrap request.
            connection.setDoOutput(true);
            connection.setFixedLengthStreamingMode(0);
            connection.getOutputStream().close();
            if (connection.getResponseCode() != 200) throw new IllegalStateException("callback failed");
            byte[] bytes;
            try (java.io.InputStream input = connection.getInputStream()) { bytes = input.readNBytes(262145); }
            if (bytes.length > 262144) throw new IllegalArgumentException("snapshot too large");
            Selection selection = new com.google.gson.Gson().fromJson(
                    new String(bytes, java.nio.charset.StandardCharsets.UTF_8), Selection.class);
            int status = selectRooms(selection).getStatusCodeValue();
            // A newer acknowledged push can win the race with this startup fetch.
            recovered = status == 200 || status == 409;
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(BotAudioTapController.class)
                .warn("Bot audio startup selection unavailable: {}", e.getClass().getSimpleName());
        } finally { if (connection != null) connection.disconnect(); }
        if (!recovered && !bootstrap.isShutdown()) bootstrap.schedule(this::fetchInitialSelection, 10, TimeUnit.SECONDS);
    }    @GetMapping("/publishers")
    public synchronized ResponseEntity<?> publishers() {
        if(!enabled) return ResponseEntity.status(503).body("disabled");
        List<Map<String,Object>> result=new ArrayList<>();
        for(Assignment a:assignments) {
            if(a.expiresAt<=System.currentTimeMillis()) continue;
            Session session=findSession(a.sessionId);
            if(session==null) continue;
            Participant p=session.getParticipantByPublicId(a.connectionId);
            if(p==null || !p.isStreaming() || p.getPublisherStreamId()==null) continue;
            Map<String,Object> item=new HashMap<>();
            item.put("assignment",a); item.put("streamId",p.getPublisherStreamId()); result.add(item);
        }
        Map<String,Object> response=new HashMap<>(); response.put("revision",selectionRevision);
        response.put("assignments",assignments); response.put("publishers",result);
        return ResponseEntity.ok(response);
    }    private final Map<String, Tap> taps = new HashMap<>();
    private final ExecutorService work = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64), r -> daemon(r, "BotAudioTap"), new ThreadPoolExecutor.AbortPolicy());
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            r -> daemon(r, "BotAudioLease"));
    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name); t.setDaemon(true); return t;
    }
    public static class Request {
        public String sessionId, connectionId, streamId;
        public int port;
    }
    private static class Tap {
        final Request request;
        volatile String state = "pending";
        volatile RtpEndpoint endpoint;
        volatile int sourcePort;
        long expires;
        Tap(Request request) { this.request = request; renew(); }
        void renew() { expires = System.nanoTime() + TimeUnit.SECONDS.toNanos(20); }
    }
    @PostConstruct public void start() {
        enabled = environment.getProperty("MAFIA_BOT_AUDIO_ENABLED", Boolean.class, false);
        maxTaps = Math.max(1, Math.min(128, environment.getProperty("MAFIA_BOT_AUDIO_MAX_TAPS", Integer.class, 32)));
        timer.scheduleWithFixedDelay(this::expire, 2, 2, TimeUnit.SECONDS);
    }
    @PutMapping("/{leaseId}")
    public synchronized ResponseEntity<?> renew(@PathVariable String leaseId, @RequestBody Request request) {
        if (!enabled) return ResponseEntity.status(503).body("disabled");
        if (!leaseId.matches("[a-f0-9]{32}") || request.sessionId == null || request.connectionId == null
                || request.streamId == null || request.port < 58000 || request.port > 58998
                || (request.port & 1) != 0) return ResponseEntity.badRequest().body("invalid-request");
        if (!eligible(request.sessionId) || !selected(request.sessionId, request.connectionId)) return ResponseEntity.status(403).body("room-not-selected");
        Tap old = taps.get(leaseId);
        if (old != null) {
            Request r = old.request;
            if (!r.sessionId.equals(request.sessionId) || !r.connectionId.equals(request.connectionId)
                    || !r.streamId.equals(request.streamId) || r.port != request.port)
                return ResponseEntity.status(409).body("lease-conflict");
            old.renew();
            Map<String, Object> status = new HashMap<>();
            status.put("state", old.state); status.put("sourcePort", old.sourcePort);
            return ResponseEntity.ok(status);
        }
        if (taps.size() >= maxTaps) return ResponseEntity.status(429).body("capacity");
        Tap tap = new Tap(request);
        taps.put(leaseId, tap);
        try { work.execute(() -> attach(leaseId, tap)); }
        catch (RejectedExecutionException e) { taps.remove(leaseId); return ResponseEntity.status(503).body("busy"); }
        return ResponseEntity.accepted().body(Collections.singletonMap("state", "pending"));
    }
    @DeleteMapping("/{leaseId}")
    public synchronized ResponseEntity<?> remove(@PathVariable String leaseId) {
        Tap tap = taps.remove(leaseId);
        if (tap != null) release(tap.endpoint);
        return ResponseEntity.noContent().build();
    }
    private void attach(String id, Tap tap) {
        RtpEndpoint endpoint = null;
        try {
            synchronized (this) { if (taps.get(id) != tap || System.nanoTime() >= tap.expires || !eligible(tap.request.sessionId) || !selected(tap.request.sessionId,tap.request.connectionId)) return; } Request r = tap.request;
            Session session = findSession(r.sessionId);
            Participant p = session == null ? null : session.getParticipantByPublicId(r.connectionId);
            if (!(p instanceof KurentoParticipant) || !p.isStreaming()
                    || !r.streamId.equals(p.getPublisherStreamId())) throw new IllegalStateException("publisher-gone");
            PublisherEndpoint publisher = ((KurentoParticipant) p).getPublisher();
            endpoint = new RtpEndpoint.Builder(publisher.getPipeline()).build();
            // Fixed loopback: a request cannot forward audio to an arbitrary host.
            String sdp = "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=BotAudio\r\nc=IN IP4 127.0.0.1\r\nt=0 0\r\n"
                    + "m=audio " + r.port + " RTP/AVP 0\r\na=rtpmap:0 PCMU/8000\r\na=recvonly\r\n";
            String answer = endpoint.processOffer(sdp);
            if (!answer.contains("PCMU/8000") || answer.contains("m=audio 0 "))
                throw new IllegalStateException("codec-rejected");
            java.util.regex.Matcher media = java.util.regex.Pattern.compile("(?m)^m=audio ([0-9]+) ").matcher(answer);
            if (!media.find()) throw new IllegalStateException("missing-rtp-port");
            tap.sourcePort = Integer.parseInt(media.group(1));
            // Parallel sink, not inserted into the live player media chain.
            publisher.getEndpoint().connect(endpoint, MediaType.AUDIO);
            synchronized (this) {
                if (taps.get(id) != tap || System.nanoTime() >= tap.expires || !eligible(tap.request.sessionId) || !selected(tap.request.sessionId,tap.request.connectionId)) {
                    release(endpoint); return;
                }
                tap.endpoint = endpoint;
                tap.state = "active";
            }
        } catch (Exception e) {
            release(endpoint);
            tap.state = "failed";
        }
    }
    private synchronized void expire() {
        Iterator<Tap> it = taps.values().iterator();
        while (it.hasNext()) {
            Tap tap = it.next();
            if (System.nanoTime() >= tap.expires || !eligible(tap.request.sessionId) || !selected(tap.request.sessionId,tap.request.connectionId)) { it.remove(); release(tap.endpoint); }
        }
    }
    private void release(RtpEndpoint endpoint) {
        if (endpoint == null) return;
        try {
            endpoint.release(new Continuation<Void>() {
                public void onSuccess(Void result) {}
                public void onError(Throwable cause) {}
            });
        } catch (Exception ignored) { }
    }
    @PreDestroy public synchronized void stop() {
        bootstrap.shutdownNow();
        timer.shutdownNow();
        taps.values().forEach(t -> release(t.endpoint));
        taps.clear();
        work.shutdownNow();
    }
}