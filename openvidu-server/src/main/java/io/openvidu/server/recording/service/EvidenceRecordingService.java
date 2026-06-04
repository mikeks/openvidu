package io.openvidu.server.recording.service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kurento.client.MediaPipeline;
import org.kurento.client.MediaProfileSpecType;
import org.kurento.client.MediaType;
import org.kurento.client.RecorderEndpoint;
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

	private final Map<String, RecorderEndpoint> activeRecorders = new ConcurrentHashMap<>();
	private final Map<String, ActiveEvidenceRecording> activeEvidenceRecordings = new ConcurrentHashMap<>();

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
		String result = startContinuous(session, participant, playerNumber, clientCreated, true);
		if (result == null) {
			return "player-excluded";
		}
		return result;
	}

	public synchronized String stopPlayer(String sessionId, int playerNumber) {
		return stopContinuous(sessionId, playerNumber);
	}

	public synchronized void startContinuousIfEligible(Session session, Participant participant) {
		PlayerClientInfo info = parsePlayerClientInfo(participant);
		if (info == null || info.playerNumber > MAX_PLAYER_NUMBER) {
			return;
		}
		startContinuous(session, participant, info.playerNumber, info.created, false);
	}

	public synchronized void stopContinuousIfParticipant(Participant participant) {
		PlayerClientInfo info = parsePlayerClientInfo(participant);
		if (info == null || info.playerNumber > MAX_PLAYER_NUMBER) {
			return;
		}
		stopContinuous(participant.getSessionId(), info.playerNumber, info.created, participant.getParticipantPublicId());
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
			String rotateResult = rotateContinuousSegment(sessionId, playerNumber);
			if (rotateResult != null && rotateResult.startsWith("error:")) {
				return SnapshotResult.retry("requested-range-is-still-recording", RETRY_AFTER_FINALIZE_GRACE_SECONDS);
			}
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

	private String startContinuous(Session session, Participant participant, int playerNumber, long clientCreated,
			boolean manual) {
		if (playerNumber > MAX_PLAYER_NUMBER) {
			return null;
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
			stopContinuous(sessionId, playerNumber);
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

		try {
			MediaPipeline pipeline = publisher.getPipeline();

			RecorderEndpoint recorder = new RecorderEndpoint.Builder(pipeline, "file://" + activeFile.getAbsolutePath())
					.withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY).build();

			publisher.connect(recorder, MediaType.AUDIO, false);
			recorder.record();

			activeEvidenceRecordings.put(key,
					new ActiveEvidenceRecording(recorder, publisher, activeFile, startMs, sessionId, playerNumber, connectionId,
							clientCreated));

			log.info(
					"MK evidence continuous START session={} playerNumber={} connectionId={} clientCreated={} file={} manual={}",
					sessionId, playerNumber, connectionId, clientCreated, activeFile.getAbsolutePath(), manual);

			return activeFile.getAbsolutePath();
		} catch (Exception e) {
			log.error("MK evidence continuous START failed session={} playerNumber={} connectionId={}", sessionId,
					playerNumber, connectionId, e);
			return "error: " + e.getMessage();
		}
	}

	private String rotateContinuousSegment(String sessionId, int playerNumber) {
		String key = evidenceKey(sessionId, playerNumber);
		ActiveEvidenceRecording active = activeEvidenceRecordings.get(key);

		if (active == null) {
			return "not-recording-player";
		}

		long stopMs = System.currentTimeMillis();
		File finalFile = new File(active.file.getParentFile(), "segment_" + active.startMs + "_" + stopMs + ".webm");

		try {
			active.recorder.stop();
			active.recorder.release();
			Files.move(active.file.toPath(), finalFile.toPath());

			long newStartMs = System.currentTimeMillis();
			File newActiveFile = new File(active.file.getParentFile(),
					"active_" + newStartMs + "_" + sanitizeFilePart(active.connectionId) + ".webm");
			RecorderEndpoint recorder = new RecorderEndpoint.Builder(active.publisher.getPipeline(),
					"file://" + newActiveFile.getAbsolutePath()).withMediaProfile(MediaProfileSpecType.WEBM_AUDIO_ONLY)
							.build();
			active.publisher.connect(recorder, MediaType.AUDIO, false);
			recorder.record();

			activeEvidenceRecordings.put(key,
					new ActiveEvidenceRecording(recorder, active.publisher, newActiveFile, newStartMs, sessionId,
							playerNumber, active.connectionId, active.clientCreated));

			log.info("MK evidence continuous ROLL session={} playerNumber={} connectionId={} file={} newFile={}",
					sessionId, playerNumber, active.connectionId, finalFile.getAbsolutePath(),
					newActiveFile.getAbsolutePath());

			return finalFile.getAbsolutePath();
		} catch (Exception e) {
			log.error("MK evidence continuous ROLL failed session={} playerNumber={} connectionId={}", sessionId,
					playerNumber, active.connectionId, e);
			return "error: " + e.getMessage();
		}
	}

	private String stopContinuous(String sessionId, int playerNumber) {
		return stopContinuous(sessionId, playerNumber, null, null);
	}

	private String stopContinuous(String sessionId, int playerNumber, Long clientCreated, String connectionId) {
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

		long stopMs = System.currentTimeMillis();
		File finalFile = new File(active.file.getParentFile(), "segment_" + active.startMs + "_" + stopMs + ".webm");

		try {
			active.recorder.stop();
			active.recorder.release();
			Files.move(active.file.toPath(), finalFile.toPath());

			log.info("MK evidence continuous STOP session={} playerNumber={} connectionId={} file={}", sessionId,
					playerNumber, active.connectionId, finalFile.getAbsolutePath());

			return finalFile.getAbsolutePath();
		} catch (Exception e) {
			log.error("MK evidence continuous STOP failed session={} playerNumber={} connectionId={}", sessionId,
					playerNumber, active.connectionId, e);
			return "error: " + e.getMessage();
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

	private static class ActiveEvidenceRecording {
		RecorderEndpoint recorder;
		PublisherEndpoint publisher;
		File file;
		long startMs;
		String sessionId;
		int playerNumber;
		String connectionId;
		long clientCreated;

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
