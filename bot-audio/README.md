# Bot audio observer

Local implementation; not deployed. Actual Kurento RTP, Linux container, microphone accuracy and uninterrupted live-media acceptance tests remain required on an idle node.

## Processes and communication

AS -> HTTPS authenticated OpenVidu control endpoint -> desired listening assignments in OpenVidu memory.
Kurento -> loopback AUDIO-only RTP tap -> separate bot-audio container (Silero VAD -> segmentation -> optional STT).
Observer -> HTTPS AS AudioResult callback -> existing bot notification pipe -> worker observations.

The original WebRTC publisher/subscriber path is unchanged. No inference or file operations run inside OpenVidu or Kurento. Kurento transcoding still adds CPU, so measure both processes. The observer may be stopped or updated independently; loss of observations must never be interpreted as player silence. This version does not promise zero-loss observer updates.

OpenVidu is off by default (`MAFIA_BOT_AUDIO_ENABLED=false`). A separate bounded executor creates AUDIO-only RTP endpoints. Source/destination are loopback, receiver ports 58000..58998, PCMU/8000; the SDP source port is checked. Tap leases expire after 20 seconds; only tap endpoints are released. Limit is 32 streams by default, at most 128.

## Assignments

AS sends one complete `{revision, assignments}` snapshot per affected node to `PUT /openvidu/api/bot-audio/rooms`. The existing route name is retained, but its payload now selects users, not entire rooms. Each assignment has:

- `sessionId`, `userId`, `analysisId` (currently `state-ID`), authoritative `connectionId` from dbo.Connections.
- `mode`: `explicit-end` or `pause-delimited`.
- `sttMode`: `vad-only`, `dry-run`, or `azure`.
- `language`: recognition locale; `pauseSeconds`: 3 by default.
- `expiresAt`: Unix milliseconds, a fail-closed deadline (state deadline plus 30 seconds).

Removing/replacing an assignment explicitly ends it and analyzes remaining audio. The deadline is an emergency bound if AS cannot deliver the end. It is not a normal heartbeat. A source loss flushes a partial segment with an interruption reason. A fresh publisher can continue the same analysis ID with a new result ID. Cancel/discard commands are not exposed separately in this version; removal means finish.

AS queues synchronization after committed state changes, game completion and connection creation, and on startup. There is no ReadRoom hook or periodic selection query. The existing maintenance timer retries failed work only. A newer event arriving during delivery queues an immediate fresh synchronization. ASP.NET additions use synchronous I/O on a hosted background job with bounded concurrency and network deadlines.

The current AS policy listens to non-bot players in games containing bots: the personal speaker in ThirtySec/SelfDefence/LastWord; living, non-muted humans in Discussion. Lobbies and other states yield no assignments. The observer itself has no game-phase or role logic. No hidden roles are sent to it.

OpenVidu acknowledges the exact revision. The room.BotAudioRevision SQL sequence makes ordering survive AS restarts. Startup recovery POSTs to `MAFIA_BOT_AUDIO_AS_URL=https://YOUR-API-HOST/Api/Bots/GetAudioRooms.ashx?serverNum=N`, using the node's existing OpenVidu secret as a Bearer credential. Failed startup fetches retry, but successful selection has no AS heartbeat. Do not reset the sequence on deployment.

The observer discovers publishers and renews tap leases through the local OpenVidu endpoint once per second. This is local media control, not AS polling. It introduces up to roughly one second of discovery delay, plus tap setup, which is marked partial coverage. Short assignments entirely between discoveries can be missed. This is acceptable for the initial observer, but must be measured before gameplay penalties use it.

## Segmentation and STT

Silero runs continuously in both modes. Explicit-end preserves pauses until assignment removal. Pause-delimited finalizes after three seconds of observed non-speech and continues listening. Packet loss is not a pause. A 180-second buffer ceiling produces a `duration-limit` continuation, rather than unbounded memory.

VAD-only does not retain PCM. Dry-run and Azure use the same prepared mono PCM16 8 kHz WAV: speech ranges with 320 ms padding, overlapping ranges merged, long internal pauses removed. The padding also covers VAD onset hysteresis. Returned spoken seconds are an estimate from VAD, not verified spoken words. Background speech/radio can still trigger VAD.

AS configuration defaults to `BotAudioSttMode=vad-only`. Set `BotAudioSttRoomModes` to a JSON object for room-specific testing, e.g. `{"66486":"dry-run"}`. Values are selected on AS and carried per assignment. Supported configured recognition locales are ru-RU, uk-UA, ka-GE, ar-SA, en-US and ko-KR; actual Azure model/region support must be verified before enabling a locale. Unsupported configured languages remain VAD-only.

Azure mode uses the synchronous Fast Transcription REST endpoint, API version 2025-10-15, with `audio` and `definition` multipart fields. Region/key are observer-only environment variables. It requires a resource/region/tier supporting Fast Transcription; do not assume the existing F0 TTS resource is sufficient. No real Azure transcription was called during local implementation. Provider failures return `sttStatus=failed` alongside VAD measurements; they never turn speech into silence. Delivery retries do not repeat transcription.

## Dry-run storage

Suggested deployment directory: `/opt/mafia-bot-audio`. Compose mounts `./data/stt-dry-run` at `/data/stt-dry-run` in the container. Prepare this host directory writable by the container app UID (1654 for the current .NET image), with access restricted to operators. Keep captures out of git and public web roots.

Each speech produces a timestamp/UUID `.wav` and `.json` with source IDs, VAD totals, original decoded-PCM sample ranges, padding and prepared duration. Offsets describe received PCM, not reconstructed missing RTP time. Dry-run writes the exact bytes Azure would receive and never makes an Azure call. No file is created when there is no detected speech.

Cleanup runs on each save: files older than 24 hours are removed, and oldest files are evicted to stay within a 256 MiB budget. Thus an idle observer does not actively expire old captures; clear them operationally after review. Raw untrimmed audio is not saved. Disk failure is reported as STT failure, independently of VAD/media processing.

## Results and worker

Results echo assignment IDs and add a UUID result ID, segment number, end reason, analyzed/spoken seconds, speech percentage of analyzed audio, coverage and STT status/text. Coverage is unavailable with no received PCM; startup, gaps and interrupted/end boundaries are conservatively partial. Never treat an unavailable result as silence or a transcript as a trusted command/check.

`BOT_AUDIO_RESULT_URL` points to HTTPS `Api/Bots/AudioResult.ashx?serverNum=N`. AS validates the node credential, body bounds, session/user/state association and recent game membership via room.GetBotAudioResultRecipients. Obsolete results are acknowledged/discarded. Valid observations go only to game bots, not player sockets. AS caches accepted result IDs; workers also deduplicate and keep at most 64 observations for ten minutes per bot. RecentAudioObservations exposes them for later decision work. No gameplay action or AFK penalty is added here.

The bounded processing queue holds 32 completed segments. Callback failures retry up to eight times; exhaustion/overload is counted in /status. The queue and dedup caches are in memory, not a durable outbox. Restart/overload can lose observations. Multiple observer instances are not supported for rolling overlap yet: UUID dedup handles retransmission, not two independent recordings of the same utterance.

## Verification and rollout

Local checks: `dotnet run -- --self-test` includes segmentation, trimming, WAV parity between dry-run and mocked Azure, no HTTP in dry-run, unavailable coverage, real ONNX silence/noise, RTP/UDP, gap recovery and stream isolation. Java selection/lease tests cover per-user selection, invalid modes, expiry, ordering, idempotency, capacity and setup failure isolation. These are not real-speech recognition-quality tests.

Apply the DB sequence/procedures and API changes together with the new control payload, then the OpenVidu controller and separate observer on an idle node. Initial controller installation requires an OpenVidu update; later observer updates do not. Use a separate compose project: `docker compose -p mafia-bot-audio up -d --build`. Do not restart the media stack as part of observer updates.

First enable one dry-run room. Verify personal speech boundaries, long pauses, overlapping discussion, microphone mute/unmute, reconnect, dropped packets and no-audio cases. Listen to the WAVs before enabling Azure. Kill/restart the observer during live speech and verify player WebRTC is uninterrupted. Measure CPU/RSS and extra Kurento transcoding at representative concurrency. Then verify HTTPS callbacks and worker receipt end-to-end. No production service or database was changed by this implementation.

Dry-run diagnostics: each segment now saves the STT-prepared `.wav`, an untrimmed `.original.wav` containing decoded PCMU before VAD framing, and `.json` metadata. Original audio includes received silence, but cannot reconstruct missing RTP packets. Metadata is also saved for no-speech/unavailable results; WAV files only exist when the corresponding samples exist. Transport counters and continuity gaps help distinguish capture failures from VAD decisions. No originals are retained in VAD-only or Azure mode.

Speech acceptance also requires at least -70 dBFS RMS per 32 ms frame. Silero receives every frame to preserve its context; below-threshold frames feed a zero probability to the speech gate. The Azure upload path independently rejects prepared audio with no frame above the same energy floor. Raw dry-run diagnostics remain available.

Set `BOT_AUDIO_SAVE_DIAGNOSTICS=1` to retain original decoded audio, prepared audio (when speech was detected), transport counters and final STT result JSON alongside Azure recognition. This includes no-speech results. Uses the same 24-hour/256-MB bounded capture directory as dry-run; disabled by default. Diagnostic save failures do not suppress result delivery.

## Observer resource metrics

The localhost-only `/metrics` endpoint returns process instanceId, uptimeSeconds, cumulative cpuSeconds, rssBytes and streamCount. The API health collector calculates CPU deltas per server/instance; a restart or missing sample resets the baseline. CPU 100% means one fully used core. These measurements are independent of media availability and use the existing server-health collection schedule. Stat/Health requires the matching database health columns/procedure and admin/API deployment.

Speech results include preparedSeconds (prepared WAV PCM duration, including padding) and sttElapsedMilliseconds for attempted Azure transcription. The worker stores these in the raw HumanSpeechLog result JSON. Production assignment synchronization is explicitly restricted by the AS BotAudioServers comma-separated allowlist; an absent/empty list disables production synchronization. Local debug selection retains its separate server setting.

AS `BotAudioLanguages` is a comma-separated allowlist of room language codes (pilot: `ru`). It filters assignments before recognition locale mapping, so excluded rooms are not listened to even in VAD-only/dry-run mode. Missing/empty configuration means no listening assignments. Per-room STT overrides do not bypass this filter. Changes take effect when AS sends the next selection snapshot.
