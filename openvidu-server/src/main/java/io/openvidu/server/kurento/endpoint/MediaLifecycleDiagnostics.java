package io.openvidu.server.kurento.endpoint;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Fixed-size cumulative counters, sampled once per minute; no participant data or SDP. */
public final class MediaLifecycleDiagnostics {

	public enum Event {
		CREATE_STARTED, CREATE_SUCCEEDED, CREATE_FAILED, INITIALIZATION_FAILED, DISPOSED, LATE_CREATE,
		DUPLICATE_SUBSCRIPTION, PUBLISHER_LOCK_TIMEOUT, RELEASE_REQUESTED, RELEASE_SUCCEEDED,
		RELEASE_FAILED, RELEASE_TIMEOUT, RELEASE_RETRY, RELEASE_RETRY_SUCCEEDED,
		RELEASE_RETRY_FAILED, RELEASE_RETRY_TIMEOUT
	}

	private static final Logger log = LoggerFactory.getLogger(MediaLifecycleDiagnostics.class);
	private static final Map<Event, LongAdder> counters = new EnumMap<>(Event.class);

	static {
		for (Event event : Event.values()) counters.put(event, new LongAdder());
		Executors.newSingleThreadScheduledExecutor(r -> {
			Thread thread = new Thread(r, "media-lifecycle-diagnostics");
			thread.setDaemon(true);
			return thread;
		}).scheduleWithFixedDelay(() -> {
			StringBuilder line = new StringBuilder("MEDIA_LIFECYCLE cumulative");
			counters.forEach((event, value) -> line.append(' ').append(event).append('=').append(value.sum()));
			log.info(line.toString());
		}, 60, 60, TimeUnit.SECONDS);
	}

	private MediaLifecycleDiagnostics() { }

	public static void record(Event event) {
		counters.get(event).increment();
	}
}
