package io.openvidu.server.recording.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kurento.client.ErrorEvent;
import org.kurento.client.EventListener;
import org.kurento.client.ListenerSubscription;
import org.kurento.client.MediaFlowState;
import org.kurento.client.MediaPipeline;
import org.kurento.client.MediaProfileSpecType;
import org.kurento.client.MediaType;
import org.kurento.client.RecorderEndpoint;
import org.kurento.client.StoppedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import io.openvidu.server.core.Participant;
import io.openvidu.server.core.Session;
import io.openvidu.server.kurento.core.KurentoParticipant;
import io.openvidu.server.kurento.endpoint.PublisherEndpoint;

@Service
public class EvidenceRecordingService {

	private static final Logger log = LoggerFactory.getLogger(EvidenceRecordingService.class);

	private static final String BASE_PATH = "/opt/openvidu/evidence/";
	private static final Pattern FINAL_SEGMENT_PATTERN = Pattern.compile("segment_(\\d+)_(\\d+)\\.webm");
	private static final Pattern ACTIVE_SEGMENT_PATTERN = Pattern.compile("active_(\\d+)_.+\\.webm");
	private static final long SNAPSHOT_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(5);
	private static final int RETRY_AFTER_FINALIZE_GRACE_SECONDS = 2;
	private static final long MIN_CUT_DURATION_MS = 1000;
	private static final long MIN_CUT_FILE_BYTES = 1024;
	private static final int MAX_PLAYER_NUMBER = 99;
	private static final long RECORDER_STOP_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(15);
	private static final long RECORDER_STOP_DELAY_MS = TimeUnit.SECONDS.toMillis(30);
	private static final long EVIDENCE_KMS_BACKOFF_MS = TimeUnit.MINUTES.toMillis(1);
	private static final int EVIDENCE_QUEUE_CAPACITY = 100;
	private static final File EVIDENCE_LOG_FILE = new File(BASE_PATH + "evidence-recording.log");

	private final Map<String, RecorderEndpoint> activeRecorders = new ConcurrentHashMap<>();
	private final Map<String, ActiveEvidenceRecording> activeEvidenceRecordings = new ConcurrentHashMap<>();
	private final Map<String, PendingEvidenceStart> pendingEvidenceStarts = new ConcurrentHashMap<>();
	private final AtomicLong evidenceDisabledUntilMs = new AtomicLong(0);
	private final ExecutorService evidenceKurentoExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<>(EVIDENCE_QUEUE_CAPACITY), runnable -> newDaemonThread(runnable, "MafiaEvidenceKms"),
			new ThreadPoolExecutor.AbortPolicy());
	private final ScheduledExecutorService evidenceStopScheduler = Executors
			.newSingleThreadScheduledExecutor(runnable -> newDaemonThread(runnable, "MafiaEvidenceStopDelay"));

	public synchronized String start(Session session, Participant participant) {
		String connectionId = participant.getParticipantPublicId();
		String key = session.getSessionId() + ":" + connectionId;

		if (activeRecorders.containsKey(key)) {
			return "already-recording";
		}

		if (!participant.isStreaming()) {
			return "participant-not-streaming";
		}

		KurentoParticipant kp = (KurentoParticipant) participant;
		PublisherEndpoint publisher = kp.getPublisher();

		if (!publisher.getMediaOptions().hasAudio()) {
			return "publisher-has-no-audio";
		}

		try {
			File dir = new File(BASE_PATH + session.getSessionId());
			dir.mkdirs();

			String filePath = BASE_PATH + session.getSessionId() + "/" + connectionId + ".webm";

			MediaPipeline pipeline = publisher.getPipeline();

			RecorderEndpoint recorder = new RecorderEndpoint.Builder(pipeline, "file://" + filePath)
					.withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY).build();

			publisher.connect(recorder, MediaType.AUDIO, false);
			recorder.record();

			activeRecorders.put(key, recorder);

			log.info("MK evidence recording START session={} connectionId={} file={}", session.getSessionId(),
					connectionId, filePath);

			return filePath;

		} catch (Exception e) {
			log.error("MK evidence recording START failed session={} connectionId={}", session.getSessionId(),
					connectionId, e);
			return "error: " + e.getMessage();
		}
	}

	public synchronized String stop(String sessionId, String connectionId) {
		String key = sessionId + ":" + connectionId;
		RecorderEndpoint recorder = activeRecorders.remove(key);

		if (recorder == null) {
			return "not-recording";
		}

		try {
			recorder.stop();
			recorder.release();

			log.info("MK evidence recording STOP session={} connectionId={}", sessionId, connectionId);

			return BASE_PATH + sessionId + "/" + connectionId + ".webm";

		} catch (Exception e) {
			log.error("MK evidence recording STOP failed session={} connectionId={}", sessionId, connectionId, e);
			return "error: " + e.getMessage();
		}
	}

	public synchronized String startForPlayer(Session session, Participant participant, int playerNumber) {
		PlayerClientInfo info = parsePlayerClientInfo(participant);
		long clientCreated = info != null ? info.created : 0;
		cancelPendingEvidenceStart(session.getSessionId(), playerNumber);
		String result = startContinuous(session, participant, playerNumber, clientCreated, true);
		if (result == null) {
			return "player-excluded";
		}
		return result;
	}

	public synchronized String stopPlayer(String sessionId, int playerNumber) {
		cancelPendingEvidenceStart(sessionId, playerNumber);
		return stopContinuous(sessionId, playerNumber);
	}

	public void startContinuousOnAudioFlowIfEligible(Session session, Participant participant) {
		PlayerClientInfo info = parsePlayerClientInfo(participant);
		if (info == null || info.playerNumber > MAX_PLAYER_NUMBER) {
			return;
		}

		KurentoParticipant kp = (KurentoParticipant) participant;
		PublisherEndpoint publisher = kp.getPublisher();
		if (publisher == null || publisher.getEndpoint() == null || publisher.getMediaOptions() == null
				|| !publisher.getMediaOptions().hasAudio()) {
			return;
		}

		String key = evidenceKey(session.getSessionId(), info.playerNumber);
		PendingEvidenceStart pending = new PendingEvidenceStart(session, participant, publisher, key, info.playerNumber,
				info.created, participant.getParticipantPublicId());
		PendingEvidenceStart previous = pendingEvidenceStarts.put(key, pending);
		if (previous != null) {
			previous.dispose();
		}

		ListenerSubscription subscription = publisher.getEndpoint().addMediaFlowOutStateChangeListener(event -> {
			if (event.getState() == MediaFlowState.FLOWING && event.getMediaType() == MediaType.AUDIO) {
				executeEvidenceKurentoTask(() -> startPendingContinuous(pending, "audio-flow"),
						"start " + session.getSessionId() + " player " + info.playerNumber);
			}
		});
		pending.setSubscription(subscription);
	}

	public void stopContinuousIfParticipant(Participant participant) {
		PlayerClientInfo info = parsePlayerClientInfo(participant);
		if (info == null || info.playerNumber > MAX_PLAYER_NUMBER) {
			return;
		}
		cancelPendingEvidenceStart(participant.getSessionId(), info.playerNumber);
		scheduleStopContinuous(participant.getSessionId(), info.playerNumber, info.created,
				participant.getParticipantPublicId(), "participant-stop");
	}

	public synchronized SnapshotResult buildSnapshot(String roomId, int playerNumber, long startEpoch, long stopEpoch) {
		if (playerNumber > MAX_PLAYER_NUMBER) {
			return SnapshotResult.error("player-excluded", 400);
		}

		long startMs = normalizeEpoch(startEpoch);
		long stopMs = normalizeEpoch(stopEpoch);

		if (startMs <= 0 || stopMs <= startMs) {
			return SnapshotResult.error("invalid-time-range", 400);
		}

		String sessionId = resolveSessionId(roomId);
		File playerDir = getPlayerDir(sessionId, playerNumber);
		if (!playerDir.exists() || !playerDir.isDirectory()) {
			return SnapshotResult.error("recording-not-found", 404);
		}

		ActiveSegment active = findActiveSegment(sessionId, playerNumber);
		if (active != null && active.startMs < stopMs) {
			long nowMs = System.currentTimeMillis();
			if (stopMs > nowMs) {
				return SnapshotResult.retry("requested-range-is-still-recording",
						retryAfterSeconds(stopMs - nowMs));
			}
			scheduleRotateContinuousSegment(sessionId, playerNumber);
			return SnapshotResult.retry("requested-range-is-still-recording", RETRY_AFTER_FINALIZE_GRACE_SECONDS);
		}

		List<Segment> overlappingSegments = findOverlappingSegments(sessionId, playerNumber, playerDir, startMs, stopMs);
		if (overlappingSegments.isEmpty()) {
			return SnapshotResult.error("recording-not-found", 404);
		}

		File snapshotDir = new File(playerDir, "snapshots");
		snapshotDir.mkdirs();
		File outputFile = new File(snapshotDir, "snapshot_" + startMs + "_" + stopMs + ".webm");

		if (outputFile.exists() && outputFile.length() > 0) {
			return SnapshotResult.ready(outputFile);
		}

		File ffmpeg = new File("/usr/bin/ffmpeg");
		if (!ffmpeg.exists() || !ffmpeg.canExecute()) {
			return SnapshotResult.error("ffmpeg-not-installed", 500);
		}

		File tmpDir = null;
		try {
			tmpDir = Files.createTempDirectory(snapshotDir.toPath(), "build_").toFile();
			List<File> cutFiles = new ArrayList<>();

			for (int i = 0; i < overlappingSegments.size(); i++) {
				Segment segment = overlappingSegments.get(i);
				long cutStartMs = Math.max(startMs, segment.startMs);
				long cutStopMs = Math.min(stopMs, segment.stopMs);
				if (cutStopMs - cutStartMs < MIN_CUT_DURATION_MS) {
					continue;
				}
				File cutFile = new File(tmpDir, "part_" + i + ".webm");
				runFfmpegCut(segment.file, cutFile, cutStartMs - segment.startMs, cutStopMs - cutStartMs);
				if (cutFile.length() >= MIN_CUT_FILE_BYTES) {
					cutFiles.add(cutFile);
				}
			}

			if (cutFiles.isEmpty()) {
				return SnapshotResult.error("recording-not-found", 404);
			}

			if (cutFiles.size() == 1) {
				Files.move(cutFiles.get(0).toPath(), outputFile.toPath());
			} else {
				runFfmpegConcat(cutFiles, outputFile);
			}

			return SnapshotResult.ready(outputFile);
		} catch (Exception e) {
			log.error("MK evidence snapshot failed roomId={} playerNumber={} start={} stop={}", roomId, playerNumber,
					startMs, stopMs, e);
			outputFile.delete();
			return SnapshotResult.error("snapshot-build-failed: " + e.getMessage(), 500);
		} finally {
			deleteDirectory(tmpDir);
		}
	}

	private synchronized String startContinuous(Session session, Participant participant, int playerNumber, long clientCreated,
			boolean manual) {
		if (playerNumber > MAX_PLAYER_NUMBER) {
			return null;
		}
		if (isEvidenceBackoffActive()) {
			return "evidence-backoff";
		}

		String sessionId = session.getSessionId();
		String connectionId = participant.getParticipantPublicId();
		String key = evidenceKey(sessionId, playerNumber);

		ActiveEvidenceRecording current = activeEvidenceRecordings.get(key);
		if (current != null) {
			if (current.clientCreated > clientCreated) {
				log.info(
						"MK evidence continuous ignoring obsolete publisher session={} playerNumber={} connectionId={} clientCreated={} activeClientCreated={}",
						sessionId, playerNumber, connectionId, clientCreated, current.clientCreated);
				return "obsolete-player-connection";
			}
			cancelDelayedStop(current, "replace-active");
			activeEvidenceRecordings.remove(key);
			long stopMs = System.currentTimeMillis();
			File finalFile = new File(current.file.getParentFile(),
					"segment_" + current.startMs + "_" + stopMs + ".webm");
			stopRecorderDelayed(current, finalFile, "replace-active", false);
		}

		if (!participant.isStreaming()) {
			return "participant-not-streaming";
		}

		KurentoParticipant kp = (KurentoParticipant) participant;
		PublisherEndpoint publisher = kp.getPublisher();

		if (publisher == null || publisher.getMediaOptions() == null || !publisher.getMediaOptions().hasAudio()) {
			return "publisher-has-no-audio";
		}

		long startMs = System.currentTimeMillis();
		File dir = getPlayerDir(sessionId, playerNumber);
		dir.mkdirs();

		File activeFile = new File(dir, "active_" + startMs + "_" + sanitizeFilePart(connectionId) + ".webm");
		ActiveEvidenceRecording pendingLog = new ActiveEvidenceRecording(null, publisher, activeFile, startMs, sessionId,
				playerNumber, connectionId, clientCreated);

		try {
			logEvidence("START_BEGIN manual=" + manual, pendingLog, activeFile);
			MediaPipeline pipeline = publisher.getPipeline();

			RecorderEndpoint recorder = new RecorderEndpoint.Builder(pipeline, "file://" + activeFile.getAbsolutePath())
					.withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY).build();

			long connectStartMs = System.currentTimeMillis();
			publisher.connect(recorder, MediaType.AUDIO, false);
			logEvidence("START_CONNECTED durationMs=" + (System.currentTimeMillis() - connectStartMs), pendingLog,
					activeFile);
			long recordStartMs = System.currentTimeMillis();
			recorder.record();
			logEvidence("START_RECORDING durationMs=" + (System.currentTimeMillis() - recordStartMs), pendingLog,
					activeFile);

			activeEvidenceRecordings.put(key,
					new ActiveEvidenceRecording(recorder, publisher, activeFile, startMs, sessionId, playerNumber, connectionId,
							clientCreated));

			logEvidence("STARTED manual=" + manual, activeEvidenceRecordings.get(key), activeFile);

			log.info(
					"MK evidence continuous START session={} playerNumber={} connectionId={} clientCreated={} file={} manual={}",
					sessionId, playerNumber, connectionId, clientCreated, activeFile.getAbsolutePath(), manual);

			return activeFile.getAbsolutePath();
		} catch (Exception e) {
			log.error("MK evidence continuous START failed session={} playerNumber={} connectionId={}", sessionId,
					playerNumber, connectionId, e);
			markEvidenceBackoffIfKurentoTransportFailure(e);
			return "error: " + e.getMessage();
		}
	}

	private String rotateContinuousSegment(String sessionId, int playerNumber) {
		String key = evidenceKey(sessionId, playerNumber);
		ActiveEvidenceRecording active = activeEvidenceRecordings.remove(key);

		if (active == null) {
			return "not-recording-player";
		}

		long stopMs = System.currentTimeMillis();
		File finalFile = new File(active.file.getParentFile(), "segment_" + active.startMs + "_" + stopMs + ".webm");

		stopRecorderAsync(active, finalFile, "rotate", true);
		return finalFile.getAbsolutePath();
	}

	private synchronized String stopContinuous(String sessionId, int playerNumber) {
		return stopContinuous(sessionId, playerNumber, null, null, "manual-stop");
	}

	private synchronized String stopContinuous(String sessionId, int playerNumber, Long clientCreated, String connectionId,
			String reason) {
		String key = evidenceKey(sessionId, playerNumber);
		ActiveEvidenceRecording active = activeEvidenceRecordings.get(key);

		if (active == null) {
			return "not-recording-player";
		}

		if (clientCreated != null && active.clientCreated != clientCreated.longValue()) {
			log.info(
					"MK evidence continuous ignoring obsolete stop session={} playerNumber={} connectionId={} clientCreated={} activeConnectionId={} activeClientCreated={}",
					sessionId, playerNumber, connectionId, clientCreated, active.connectionId, active.clientCreated);
			return "obsolete-player-connection";
		}

		activeEvidenceRecordings.remove(key);
		cancelDelayedStop(active, reason);

		long stopMs = System.currentTimeMillis();
		File finalFile = new File(active.file.getParentFile(), "segment_" + active.startMs + "_" + stopMs + ".webm");

		stopRecorderAsync(active, finalFile, reason, false);
		return finalFile.getAbsolutePath();
	}

	private void scheduleStopContinuous(String sessionId, int playerNumber, Long clientCreated, String connectionId,
			String reason) {
		executeEvidenceKurentoTask(() -> scheduleStopContinuousDelayed(sessionId, playerNumber, clientCreated, connectionId, reason),
				"stop " + sessionId + " player " + playerNumber);
	}

	private void scheduleRotateContinuousSegment(String sessionId, int playerNumber) {
		executeEvidenceKurentoTask(() -> rotateContinuousSegment(sessionId, playerNumber),
				"rotate " + sessionId + " player " + playerNumber);
	}

	private void stopRecorderAsync(ActiveEvidenceRecording active, File finalFile, String reason, boolean restartAfterStop) {
		executeEvidenceKurentoTask(() -> stopRecorder(active, finalFile, reason, restartAfterStop),
				"stop-recorder " + active.sessionId + " player " + active.playerNumber + " reason " + reason);
	}

	private synchronized String scheduleStopContinuousDelayed(String sessionId, int playerNumber, Long clientCreated,
			String connectionId, String reason) {
		String key = evidenceKey(sessionId, playerNumber);
		ActiveEvidenceRecording active = activeEvidenceRecordings.get(key);

		if (active == null) {
			return "not-recording-player";
		}

		if (clientCreated != null && active.clientCreated != clientCreated.longValue()) {
			log.info(
					"MK evidence continuous ignoring obsolete delayed stop session={} playerNumber={} connectionId={} clientCreated={} activeConnectionId={} activeClientCreated={}",
					sessionId, playerNumber, connectionId, clientCreated, active.connectionId, active.clientCreated);
			return "obsolete-player-connection";
		}

		cancelDelayedStop(active, "reschedule-" + reason);
		long stopMs = System.currentTimeMillis();
		File finalFile = new File(active.file.getParentFile(), "segment_" + active.startMs + "_" + stopMs + ".webm");
		stopRecorderDelayed(active, finalFile, reason, false);
		return finalFile.getAbsolutePath();
	}

	private void stopRecorderDelayed(ActiveEvidenceRecording active, File finalFile, String reason,
			boolean restartAfterStop) {
		active.state = EvidenceRecordingState.STOP_PENDING;
		logEvidence("STOP_DELAYED reason=" + reason + " delayMs=" + RECORDER_STOP_DELAY_MS + " restartAfterStop="
				+ restartAfterStop, active, finalFile);

		ScheduledFuture<?> future = evidenceStopScheduler.schedule(() -> {
			synchronized (EvidenceRecordingService.this) {
				if (active.pendingStopFuture == null) {
					logEvidence("STOP_DELAY_CANCELLED_BEFORE_FIRE reason=" + reason, active, finalFile);
					return;
				}
				active.pendingStopFuture = null;
				activeEvidenceRecordings.remove(evidenceKey(active.sessionId, active.playerNumber), active);
			}

			logEvidence("STOP_DELAY_FIRED reason=" + reason, active, finalFile);
			stopRecorderAsync(active, finalFile, reason, restartAfterStop);
		}, RECORDER_STOP_DELAY_MS, TimeUnit.MILLISECONDS);

		synchronized (this) {
			active.pendingStopFuture = future;
		}
	}

	private void cancelDelayedStop(ActiveEvidenceRecording active, String reason) {
		ScheduledFuture<?> future = active.pendingStopFuture;
		if (future == null) {
			return;
		}

		active.pendingStopFuture = null;
		boolean cancelled = future.cancel(false);
		active.state = EvidenceRecordingState.RECORDING;
		logEvidence("STOP_DELAY_CANCELLED reason=" + reason + " cancelled=" + cancelled, active, null);
	}

	private void stopRecorder(ActiveEvidenceRecording active, File finalFile, String reason, boolean restartAfterStop) {
		CountDownLatch stopped = new CountDownLatch(1);
		AtomicBoolean finalized = new AtomicBoolean(false);
		AtomicReference<String> stoppedTimestamp = new AtomicReference<>("");
		active.state = EvidenceRecordingState.STOPPING;

		logEvidence("STOP_REQUEST reason=" + reason, active, finalFile);

		try {
			active.recorder.addErrorListener(new EventListener<ErrorEvent>() {
				@Override
				public void onEvent(ErrorEvent event) {
					logEvidence("ERROR code=" + event.getErrorCode() + " description=" + event.getDescription(), active,
							finalFile);
				}
			});

				active.recorder.addStoppedListener(new EventListener<StoppedEvent>() {
					@Override
					public void onEvent(StoppedEvent event) {
						if (finalized.compareAndSet(false, true)) {
							stoppedTimestamp.set(event.getTimestampMillis());
							stopped.countDown();
						}
				}
			});

			long stopStartMs = System.currentTimeMillis();
			active.recorder.stop();
			logEvidence("STOP_RETURNED reason=" + reason + " durationMs=" + (System.currentTimeMillis() - stopStartMs),
					active, finalFile);

			if (stopped.await(RECORDER_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
				finalizeStoppedRecorder(active, finalFile, reason, stoppedTimestamp.get());
				if (restartAfterStop) {
					startNextContinuousSegment(active);
				}
			} else if (finalized.compareAndSet(false, true)) {
				logEvidence("STOP_TIMEOUT reason=" + reason, active, finalFile);
				active.state = EvidenceRecordingState.FAILED;
				stopped.countDown();
				if (restartAfterStop) {
					startNextContinuousSegment(active);
				}
			}
		} catch (Exception e) {
			log.error("MK evidence continuous STOP failed session={} playerNumber={} connectionId={}", active.sessionId,
					active.playerNumber, active.connectionId, e);
			logEvidence("STOP_FAILED reason=" + reason + " error=" + e.getMessage(), active, finalFile);
			if (isKurentoMediaObjectNotFound(e)) {
				logEvidence("STOP_ALREADY_GONE reason=" + reason, active, finalFile);
				finalizeStoppedRecorder(active, finalFile, reason, "media-object-not-found");
				if (restartAfterStop) {
					startNextContinuousSegment(active);
				}
				return;
			}
			active.state = EvidenceRecordingState.FAILED;
			if (markEvidenceBackoffIfKurentoTransportFailure(e)) {
				logEvidence("RELEASE_SKIPPED transport-failure", active, null);
			} else {
				releaseRecorder(active);
			}
			if (restartAfterStop) {
				startNextContinuousSegment(active);
			}
		}
	}

	private void finalizeStoppedRecorder(ActiveEvidenceRecording active, File finalFile, String reason,
			String stoppedTimestamp) {
		try {
			releaseRecorder(active);
			long moveStartMs = System.currentTimeMillis();
			Files.move(active.file.toPath(), finalFile.toPath());
			logEvidence("FILE_MOVED durationMs=" + (System.currentTimeMillis() - moveStartMs), active, finalFile);
			active.state = EvidenceRecordingState.STOPPED;

			log.info("MK evidence continuous STOP session={} playerNumber={} connectionId={} file={}", active.sessionId,
					active.playerNumber, active.connectionId, finalFile.getAbsolutePath());
			logEvidence("STOPPED reason=" + reason + " stoppedTimestamp=" + stoppedTimestamp, active, finalFile);
		} catch (Exception e) {
			log.error("MK evidence continuous FINALIZE failed session={} playerNumber={} connectionId={}",
					active.sessionId, active.playerNumber, active.connectionId, e);
			logEvidence("FINALIZE_FAILED reason=" + reason + " error=" + e.getMessage(), active, finalFile);
			active.state = EvidenceRecordingState.FAILED;
		}
	}

	private void releaseRecorder(ActiveEvidenceRecording active) {
		try {
			long releaseStartMs = System.currentTimeMillis();
			logEvidence("RELEASE_BEGIN", active, null);
			active.recorder.release();
			logEvidence("RELEASED durationMs=" + (System.currentTimeMillis() - releaseStartMs), active, null);
		} catch (Exception e) {
			log.warn("MK evidence continuous RELEASE failed session={} playerNumber={} connectionId={}",
					active.sessionId, active.playerNumber, active.connectionId, e);
			logEvidence("RELEASE_FAILED error=" + e.getMessage(), active, null);
			markEvidenceBackoffIfKurentoTransportFailure(e);
		}
	}

	private void startNextContinuousSegment(ActiveEvidenceRecording previous) {
		String key = evidenceKey(previous.sessionId, previous.playerNumber);
		if (activeEvidenceRecordings.containsKey(key)) {
			logEvidence("RESTART_SKIPPED existing-active", previous, null);
			return;
		}

		long startMs = System.currentTimeMillis();
		File activeFile = new File(previous.file.getParentFile(),
				"active_" + startMs + "_" + sanitizeFilePart(previous.connectionId) + ".webm");

		try {
			if (isEvidenceBackoffActive()) {
				logEvidence("RESTART_SKIPPED evidence-backoff", previous, activeFile);
				return;
			}
			RecorderEndpoint recorder = new RecorderEndpoint.Builder(previous.publisher.getPipeline(),
					"file://" + activeFile.getAbsolutePath()).withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY)
							.build();
			previous.publisher.connect(recorder, MediaType.AUDIO, false);
			recorder.record();

			ActiveEvidenceRecording next = new ActiveEvidenceRecording(recorder, previous.publisher, activeFile, startMs,
					previous.sessionId, previous.playerNumber, previous.connectionId, previous.clientCreated);
			activeEvidenceRecordings.put(key, next);

			log.info("MK evidence continuous RESTART session={} playerNumber={} connectionId={} file={}",
					previous.sessionId, previous.playerNumber, previous.connectionId, activeFile.getAbsolutePath());
			logEvidence("RESTARTED", next, activeFile);
		} catch (Exception e) {
			log.error("MK evidence continuous RESTART failed session={} playerNumber={} connectionId={}",
					previous.sessionId, previous.playerNumber, previous.connectionId, e);
			logEvidence("RESTART_FAILED error=" + e.getMessage(), previous, activeFile);
			markEvidenceBackoffIfKurentoTransportFailure(e);
		}
	}

	private void startPendingContinuous(PendingEvidenceStart pending, String reason) {
		if (!pending.markCompleted()) {
			return;
		}

		PendingEvidenceStart current = pendingEvidenceStarts.get(pending.key);
		if (current != pending) {
			pending.dispose();
			return;
		}

		pendingEvidenceStarts.remove(pending.key, pending);
		pending.dispose();

		PlayerClientInfo currentInfo = parsePlayerClientInfo(pending.participant);
		if (currentInfo == null || currentInfo.playerNumber != pending.playerNumber
				|| currentInfo.created != pending.clientCreated) {
			log.info(
					"MK evidence continuous START skipped obsolete media event session={} playerNumber={} connectionId={} reason={}",
					pending.session.getSessionId(), pending.playerNumber, pending.connectionId, reason);
			return;
		}

		if (!pending.participant.isStreaming()) {
			log.info(
					"MK evidence continuous START skipped non-streaming participant session={} playerNumber={} connectionId={} reason={}",
					pending.session.getSessionId(), pending.playerNumber, pending.connectionId, reason);
			return;
		}

		KurentoParticipant kp = (KurentoParticipant) pending.participant;
		if (kp.getPublisher() != pending.publisher) {
			log.info(
					"MK evidence continuous START skipped replaced publisher session={} playerNumber={} connectionId={} reason={}",
					pending.session.getSessionId(), pending.playerNumber, pending.connectionId, reason);
			return;
		}

		startContinuous(pending.session, pending.participant, pending.playerNumber, pending.clientCreated, false);
	}

	private void cancelPendingEvidenceStart(String sessionId, int playerNumber) {
		PendingEvidenceStart pending = pendingEvidenceStarts.remove(evidenceKey(sessionId, playerNumber));
		if (pending != null) {
			pending.dispose();
		}
	}

	private PlayerClientInfo parsePlayerClientInfo(Participant participant) {
		try {
			JsonObject pJson = participant.toJson();
			String metadata = pJson.get("clientData").getAsString();
			JsonObject metadataJson = JsonParser.parseString(metadata).getAsJsonObject();
			String clientData = metadataJson.get("clientData").getAsString();
			String[] parts = clientData.split("\\.");

			PlayerClientInfo info = new PlayerClientInfo();
			info.playerNumber = Integer.parseInt(parts[0]);
			info.created = Long.parseLong(parts[1]);
			return info;
		} catch (Exception ignored) {
			return null;
		}
	}

	private String resolveSessionId(String roomId) {
		File exact = new File(BASE_PATH + roomId);
		if (exact.exists()) {
			return roomId;
		}
		if (roomId.startsWith("MafiaRoom")) {
			return roomId;
		}
		return "MafiaRoom" + roomId;
	}

	private File getPlayerDir(String sessionId, int playerNumber) {
		return new File(BASE_PATH + sessionId + "/player_" + playerNumber);
	}

	private String evidenceKey(String sessionId, int playerNumber) {
		return sessionId + ":player:" + playerNumber;
	}

	private String sanitizeFilePart(String value) {
		return value.replaceAll("[^A-Za-z0-9_.-]", "_");
	}

	private long normalizeEpoch(long epoch) {
		if (epoch > 0 && epoch < 100000000000L) {
			return epoch * 1000;
		}
		return epoch;
	}

	private List<Segment> findOverlappingSegments(String sessionId, int playerNumber, File playerDir, long startMs,
			long stopMs) {
		List<Segment> result = new ArrayList<>();
		File[] files = playerDir.listFiles();
		if (files == null) {
			return result;
		}
		ActiveEvidenceRecording current = activeEvidenceRecordings.get(evidenceKey(sessionId, playerNumber));
		for (File file : files) {
			Matcher matcher = FINAL_SEGMENT_PATTERN.matcher(file.getName());
			if (file.isFile() && matcher.matches()) {
				long segmentStart = Long.parseLong(matcher.group(1));
				long segmentStop = Long.parseLong(matcher.group(2));
				if (segmentStop > startMs && segmentStart < stopMs) {
					result.add(new Segment(file, segmentStart, segmentStop));
				}
				continue;
			}
			matcher = ACTIVE_SEGMENT_PATTERN.matcher(file.getName());
			if (file.isFile() && matcher.matches()
					&& (current == null || !file.getAbsolutePath().equals(current.file.getAbsolutePath()))) {
				long segmentStart = Long.parseLong(matcher.group(1));
				long segmentStop = file.lastModified();
				if (segmentStop > segmentStart && segmentStop > startMs && segmentStart < stopMs) {
					result.add(new Segment(file, segmentStart, segmentStop));
				}
			}
		}
		result.sort(Comparator.comparingLong(segment -> segment.startMs));
		return result;
	}

	private ActiveSegment findActiveSegment(String sessionId, int playerNumber) {
		ActiveEvidenceRecording active = activeEvidenceRecordings.get(evidenceKey(sessionId, playerNumber));
		return active == null ? null : new ActiveSegment(active.startMs);
	}

	private int retryAfterSeconds(long retryAfterMs) {
		long seconds = TimeUnit.MILLISECONDS.toSeconds(retryAfterMs) + RETRY_AFTER_FINALIZE_GRACE_SECONDS;
		return (int) Math.max(RETRY_AFTER_FINALIZE_GRACE_SECONDS, Math.min(Integer.MAX_VALUE, seconds));
	}

	private void runFfmpegCut(File inputFile, File outputFile, long offsetMs, long durationMs)
			throws IOException, InterruptedException {
		List<String> command = new ArrayList<>();
		command.add("/usr/bin/ffmpeg");
		command.add("-y");
		command.add("-hide_banner");
		command.add("-loglevel");
		command.add("error");
		command.add("-ss");
		command.add(formatSeconds(offsetMs));
		command.add("-i");
		command.add(inputFile.getAbsolutePath());
		command.add("-t");
		command.add(formatSeconds(durationMs));
		command.add("-vn");
		command.add("-c:a");
		command.add("libopus");
		command.add(outputFile.getAbsolutePath());
		runCommand(command);
	}

	private void runFfmpegConcat(List<File> inputFiles, File outputFile) throws IOException, InterruptedException {
		List<String> command = new ArrayList<>();
		command.add("/usr/bin/ffmpeg");
		command.add("-y");
		command.add("-hide_banner");
		command.add("-loglevel");
		command.add("error");
		for (File inputFile : inputFiles) {
			command.add("-i");
			command.add(inputFile.getAbsolutePath());
		}
		command.add("-filter_complex");
		command.add(buildConcatFilter(inputFiles.size()));
		command.add("-map");
		command.add("[a]");
		command.add("-vn");
		command.add("-c:a");
		command.add("libopus");
		command.add(outputFile.getAbsolutePath());
		runCommand(command);
	}

	private String buildConcatFilter(int inputCount) {
		StringBuilder filter = new StringBuilder();
		for (int i = 0; i < inputCount; i++) {
			filter.append("[").append(i).append(":a]");
		}
		filter.append("concat=n=").append(inputCount).append(":v=0:a=1[a]");
		return filter.toString();
	}

	private String formatSeconds(long ms) {
		return String.format(java.util.Locale.US, "%.3f", ms / 1000.0);
	}

	private void runCommand(List<String> command) throws IOException, InterruptedException {
		Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
		boolean finished = process.waitFor(SNAPSHOT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
		String output = new String(process.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
		if (!finished) {
			process.destroyForcibly();
			throw new IOException("ffmpeg-timeout: " + output);
		}
		if (process.exitValue() != 0) {
			throw new IOException("ffmpeg-exit-" + process.exitValue() + ": " + output);
		}
	}

	private void writeConcatList(File concatList, List<File> files) throws IOException {
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(concatList))) {
			for (File file : files) {
				writer.write("file '" + file.getAbsolutePath().replace("'", "'\\''") + "'");
				writer.newLine();
			}
		}
	}

	private void deleteDirectory(File dir) {
		if (dir == null || !dir.exists()) {
			return;
		}
		try {
			Files.walk(dir.toPath()).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
		} catch (IOException e) {
			log.warn("MK evidence failed to delete temp directory {}", dir.getAbsolutePath(), e);
		}
	}

	private void logEvidence(String action, ActiveEvidenceRecording active, File file) {
		String line = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date())
				+ " action=\"" + action + "\""
				+ " state=" + (active != null ? active.state : "null")
				+ " session=" + (active != null ? active.sessionId : "null")
				+ " player=" + (active != null ? active.playerNumber : 0)
				+ " connection=" + (active != null ? active.connectionId : "null")
				+ " clientCreated=" + (active != null ? active.clientCreated : 0)
				+ " activeFile=\"" + (active != null && active.file != null ? active.file.getAbsolutePath() : "") + "\""
				+ " file=\"" + (file != null ? file.getAbsolutePath() : "") + "\"";
		synchronized (EVIDENCE_LOG_FILE) {
			try {
				EVIDENCE_LOG_FILE.getParentFile().mkdirs();
				try (BufferedWriter writer = new BufferedWriter(new FileWriter(EVIDENCE_LOG_FILE, true))) {
					writer.write(line);
					writer.newLine();
				}
			} catch (IOException e) {
				log.warn("MK evidence failed to write evidence log", e);
			}
		}
	}

	private static Thread newDaemonThread(Runnable runnable, String name) {
		Thread thread = new Thread(runnable, name);
		thread.setDaemon(true);
		return thread;
	}

	private boolean isEvidenceBackoffActive() {
		return System.currentTimeMillis() < evidenceDisabledUntilMs.get();
	}

	private boolean markEvidenceBackoffIfKurentoTransportFailure(Throwable e) {
		if (!isKurentoTransportFailure(e)) {
			return false;
		}
		long until = System.currentTimeMillis() + EVIDENCE_KMS_BACKOFF_MS;
		evidenceDisabledUntilMs.updateAndGet(current -> Math.max(current, until));
		log.warn("MK evidence temporary backoff until={} due to Kurento transport failure", until);
		return true;
	}

	private boolean isKurentoTransportFailure(Throwable e) {
		for (Throwable current = e; current != null; current = current.getCause()) {
			String message = current.getMessage();
			if (message == null) {
				continue;
			}
			if (message.contains("Timeout trying to connect to websocket server")
					|| message.contains("Connection with server have been closed")
					|| message.contains("Timeout waiting")
					|| message.contains("waiting from response to request")) {
				return true;
			}
		}
		return false;
	}

	private boolean isKurentoMediaObjectNotFound(Throwable e) {
		for (Throwable current = e; current != null; current = current.getCause()) {
			String message = current.getMessage();
			if (message != null && message.contains("MEDIA_OBJECT_NOT_FOUND")) {
				return true;
			}
		}
		return false;
	}

	private void executeEvidenceKurentoTask(Runnable task, String description) {
		try {
			evidenceKurentoExecutor.execute(task);
		} catch (RuntimeException e) {
			log.warn("MK evidence dropped KMS task {} because queue is full or executor is unavailable", description, e);
		}
	}

	private enum EvidenceRecordingState {
		RECORDING,
		STOP_PENDING,
		STOPPING,
		STOPPED,
		FAILED
	}

	private static class ActiveEvidenceRecording {
		RecorderEndpoint recorder;
		PublisherEndpoint publisher;
		File file;
		long startMs;
		String sessionId;
		int playerNumber;
		String connectionId;
		long clientCreated;
		volatile EvidenceRecordingState state = EvidenceRecordingState.RECORDING;
		volatile ScheduledFuture<?> pendingStopFuture;

		ActiveEvidenceRecording(RecorderEndpoint recorder, PublisherEndpoint publisher, File file, long startMs,
				String sessionId, int playerNumber, String connectionId, long clientCreated) {
			this.recorder = recorder;
			this.publisher = publisher;
			this.file = file;
			this.startMs = startMs;
			this.sessionId = sessionId;
			this.playerNumber = playerNumber;
			this.connectionId = connectionId;
			this.clientCreated = clientCreated;
		}
	}

	private static class PendingEvidenceStart {
		Session session;
		Participant participant;
		PublisherEndpoint publisher;
		String key;
		int playerNumber;
		long clientCreated;
		String connectionId;
		private final AtomicBoolean completed = new AtomicBoolean(false);
		private volatile ListenerSubscription subscription;

		PendingEvidenceStart(Session session, Participant participant, PublisherEndpoint publisher, String key,
				int playerNumber, long clientCreated, String connectionId) {
			this.session = session;
			this.participant = participant;
			this.publisher = publisher;
			this.key = key;
			this.playerNumber = playerNumber;
			this.clientCreated = clientCreated;
			this.connectionId = connectionId;
		}

		boolean markCompleted() {
			return completed.compareAndSet(false, true);
		}

		void setSubscription(ListenerSubscription subscription) {
			this.subscription = subscription;
			if (completed.get()) {
				dispose();
			}
		}

		void dispose() {
			completed.set(true);
			ListenerSubscription currentSubscription = subscription;
			subscription = null;
			if (currentSubscription != null && publisher != null && publisher.getEndpoint() != null) {
				try {
					publisher.getEndpoint().removeMediaFlowOutStateChangeListener(currentSubscription);
				} catch (Exception ignored) {
					// Stale publishers can already be gone during reconnect/unpublish cleanup.
				}
			}
		}
	}

	private static class PlayerClientInfo {
		int playerNumber;
		long created;
	}

	private static class Segment {
		File file;
		long startMs;
		long stopMs;

		Segment(File file, long startMs, long stopMs) {
			this.file = file;
			this.startMs = startMs;
			this.stopMs = stopMs;
		}
	}

	private static class ActiveSegment {
		long startMs;

		ActiveSegment(long startMs) {
			this.startMs = startMs;
		}
	}

	public static class SnapshotResult {
		public boolean ready;
		public boolean retry;
		public int httpStatus;
		public int retryAfterSeconds;
		public String error;
		public File file;
		public Resource resource;

		static SnapshotResult ready(File file) {
			SnapshotResult result = new SnapshotResult();
			result.ready = true;
			result.httpStatus = 200;
			result.file = file;
			result.resource = new FileSystemResource(file);
			return result;
		}

		static SnapshotResult retry(String error, int retryAfterSeconds) {
			SnapshotResult result = new SnapshotResult();
			result.retry = true;
			result.httpStatus = 425;
			result.retryAfterSeconds = retryAfterSeconds;
			result.error = error;
			return result;
		}

		static SnapshotResult error(String error, int httpStatus) {
			SnapshotResult result = new SnapshotResult();
			result.httpStatus = httpStatus;
			result.error = error;
			return result;
		}
	}
}
