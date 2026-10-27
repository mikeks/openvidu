package io.openvidu.server.kurento.endpoint;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import org.junit.Before;
import org.junit.Test;
import org.kurento.client.WebRtcEndpoint;
import io.openvidu.client.OpenViduException;
import io.openvidu.server.config.OpenviduConfig;
import io.openvidu.server.core.Token;
import io.openvidu.server.kurento.core.KurentoParticipant;
import io.openvidu.server.kurento.core.KurentoSession;
import io.openvidu.server.kurento.core.KurentoParticipantEndpointConfig;
import io.openvidu.server.core.Participant;
import io.openvidu.server.core.EndReason;
import io.openvidu.server.cdr.CallDetailRecord;
import io.openvidu.java.client.OpenViduRole;
import org.kurento.client.MediaElement;
import org.kurento.client.Continuation;
import org.springframework.test.util.ReflectionTestUtils;

public class MediaEndpointLifecycleTest {

	private KurentoParticipant owner;
	private DelayedEndpoint endpoint;

	private static class DelayedEndpoint extends SubscriberEndpoint {
		int builds;
		DelayedEndpoint(KurentoParticipant owner) {
			super(EndpointType.WEBRTC_ENDPOINT, owner, "test-endpoint", null, mock(OpenviduConfig.class));
		}
		@Override protected void internalEndpointInitialization(CountDownLatch latch) { builds++; }
	}

	@Before public void setup() {
		owner = mock(KurentoParticipant.class);
		when(owner.getToken()).thenReturn(mock(Token.class));
		endpoint = new DelayedEndpoint(owner);
	}

	@Test public void pendingCreationCannotStartAnotherBuild() {
		endpoint.createEndpoint(new CountDownLatch(1));
		assertThrows(OpenViduException.class, () -> endpoint.createEndpoint(new CountDownLatch(1)));
		assertEquals(1, endpoint.builds);
		assertFalse(endpoint.isDisposed());
	}

	@Test public void synchronousCreationFailureCancelsOwnership() {
		DelayedEndpoint failing = new DelayedEndpoint(owner) {
			@Override protected void internalEndpointInitialization(CountDownLatch latch) {
				throw new IllegalStateException("build failed");
			}
		};
		CountDownLatch latch = new CountDownLatch(1);
		assertThrows(IllegalStateException.class, () -> failing.createEndpoint(latch));
		assertEquals(0, latch.getCount());
		assertTrue(failing.isDisposed());
	}

	@Test public void closeBeforeCallbackReleasesLateEndpoint() {
		CountDownLatch latch = new CountDownLatch(1);
		endpoint.createEndpoint(latch);
		endpoint.disposeEndpoint();
		assertEquals(0, latch.getCount());
		WebRtcEndpoint media = mock(WebRtcEndpoint.class);
		assertFalse(endpoint.acceptCreatedEndpoint(media));
		assertNull(endpoint.getEndpoint());
		verify(owner).releaseElement("test-endpoint", media);
	}

	@Test public void repeatedDisposalReleasesOnlyOnce() {
		WebRtcEndpoint media = mock(WebRtcEndpoint.class);
		assertTrue(endpoint.acceptCreatedEndpoint(media));
		endpoint.disposeEndpoint();
		endpoint.disposeEndpoint();
		verify(owner, times(1)).releaseElement("test-endpoint", media);
		assertThrows(OpenViduException.class, () -> endpoint.createEndpoint(new CountDownLatch(1)));
	}

	@Test public void duplicateInitializationDoesNotDisposeExistingSubscription() {
		assertTrue(endpoint.tryStartInitialization());
		assertFalse(endpoint.tryStartInitialization());
		assertFalse(endpoint.isDisposed());
		verify(owner, never()).releaseElement(anyString(), any());
	}

	@Test public void simultaneousInitializationHasSingleOwner() throws Exception {
		CountDownLatch start = new CountDownLatch(1);
		AtomicInteger winners = new AtomicInteger();
		Runnable claim = () -> {
			try { start.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
			if (endpoint.tryStartInitialization()) winners.incrementAndGet();
		};
		Thread first = new Thread(claim);
		Thread second = new Thread(claim);
		first.start(); second.start(); start.countDown();
		first.join(2000); second.join(2000);
		assertFalse(first.isAlive()); assertFalse(second.isAlive());
		assertEquals(1, winners.get());
	}

	@Test public void creationRacingCloseReleasesExactlyOnce() throws Exception {
		for (int i = 0; i < 50; i++) {
			DelayedEndpoint racing = new DelayedEndpoint(owner);
			WebRtcEndpoint media = mock(WebRtcEndpoint.class);
			CountDownLatch start = new CountDownLatch(1);
			Thread callback = new Thread(() -> {
				try { start.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
				racing.acceptCreatedEndpoint(media);
			});
			Thread close = new Thread(() -> {
				try { start.await(); } catch (InterruptedException e) { throw new AssertionError(e); }
				racing.disposeEndpoint();
			});
			callback.start(); close.start(); start.countDown();
			callback.join(2000); close.join(2000);
			assertFalse(callback.isAlive()); assertFalse(close.isAlive());
			verify(owner, times(1)).releaseElement("test-endpoint", media);
		}
	}

	private KurentoParticipant participant() {
		Token token = mock(Token.class);
		when(token.getRole()).thenReturn(OpenViduRole.SUBSCRIBER);
		Participant base = new Participant("user", "private", "public", "room", "room-unique",
				token, "", null, "test", EndpointType.WEBRTC_ENDPOINT, null);
		KurentoSession session = mock(KurentoSession.class);
		when(session.getSessionId()).thenReturn("room");
		KurentoParticipantEndpointConfig config = mock(KurentoParticipantEndpointConfig.class);
		when(config.getCdr()).thenReturn(mock(CallDetailRecord.class));
		return new KurentoParticipant(base, session, config, mock(OpenviduConfig.class), null);
	}

	@Test public void duplicateRequestPreservesMappedEndpoint() {
		KurentoParticipant participant = participant();
		SubscriberEndpoint existing = participant.getNewOrExistingSubscriber("sender");
		assertTrue(existing.tryStartInitialization());
		Participant sender = mock(Participant.class);
		when(sender.getParticipantPublicId()).thenReturn("sender");
		assertThrows(OpenViduException.class, () -> ReflectionTestUtils.invokeMethod(participant,
				"initializeSubscriberEndpoint", sender));
		assertSame(existing, participant.getSubscriber("sender"));
		assertFalse(existing.isDisposed());
	}

	@Test public void missingPreparedSubscriptionReturnsProtocolErrorNotNullPointer() {
		KurentoParticipant participant = participant();
		KurentoParticipant sender = mock(KurentoParticipant.class);
		when(sender.getParticipantPublicId()).thenReturn("sender");
		PublisherEndpoint publisher = mock(PublisherEndpoint.class);
		publisher.closingLock = new java.util.concurrent.locks.ReentrantReadWriteLock();
		when(sender.getPublisher()).thenReturn(publisher);
		ReflectionTestUtils.setField(sender, "streaming", true);
		assertThrows(OpenViduException.class, () -> participant.receiveMedia(sender, "unused", true, true));
	}

	@Test public void closingParticipantDisposesUncreatedSubscribers() {
		KurentoParticipant participant = participant();
		SubscriberEndpoint pending = participant.getNewOrExistingSubscriber("sender");
		participant.close(null, true, null);
		assertTrue(pending.isDisposed());
		assertNull(participant.getSubscriber("sender"));
	}

	@Test public void listenerFailureDoesNotSkipRelease() {
		KurentoParticipant participant = participant();
		SubscriberEndpoint subscriber = mock(SubscriberEndpoint.class);
		doThrow(new IllegalStateException("RPC unavailable")).when(subscriber).unregisterErrorListeners();
		@SuppressWarnings("unchecked")
		ConcurrentMap<String, SubscriberEndpoint> subscribers = (ConcurrentMap<String, SubscriberEndpoint>)
				ReflectionTestUtils.getField(participant, "subscribers");
		subscribers.put("sender", subscriber);
		assertTrue(participant.cleanupStaleSubscriberEndpoint("sender", null));
		verify(subscriber).disposeEndpoint();
	}

	@Test public void webRtcListenerRemovalIsAsynchronous() {
		WebRtcEndpoint media = mock(WebRtcEndpoint.class);
		org.kurento.client.ListenerSubscription subscription = mock(org.kurento.client.ListenerSubscription.class);
		endpoint.acceptCreatedEndpoint(media);
		ReflectionTestUtils.setField(endpoint, "endpointSubscription", subscription);
		endpoint.unregisterErrorListeners();
		verify(media).removeErrorListener(eq(subscription), any(Continuation.class));
		verify(media, never()).removeErrorListener(subscription);
	}

	@Test public void publisherLockTimeoutStillReleases() throws Exception {
		KurentoParticipant participant = participant();
		PublisherEndpoint publisher = mock(PublisherEndpoint.class);
		publisher.closingLock = mock(ReadWriteLock.class);
		publisher.cancelStatsLoop = new java.util.concurrent.atomic.AtomicBoolean();
		Lock lock = mock(Lock.class);
		when(publisher.closingLock.writeLock()).thenReturn(lock);
		when(lock.tryLock(15, TimeUnit.SECONDS)).thenReturn(false);
		when(publisher.getEndpoint()).thenReturn(mock(WebRtcEndpoint.class));
		when(publisher.getStreamId()).thenReturn("stream");
		ReflectionTestUtils.setField(participant, "publisher", publisher);
		KurentoSession session = participant.getSession();
		ReflectionTestUtils.setField(session, "publishedStreamIds", new java.util.concurrent.ConcurrentHashMap<>());
		ReflectionTestUtils.invokeMethod(participant, "releasePublisherEndpoint", (EndReason) null, (Long) null);
		verify(publisher).disposeEndpoint();
		verify(lock, never()).unlock();
	}

	@Test public void synchronousReleaseFailureIsRetried() {
		KurentoParticipant participant = participant();
		MediaElement element = mock(MediaElement.class);
		when(element.getId()).thenReturn("media-id");
		doThrow(new IllegalStateException("transport failed")).doNothing().when(element).release(any(Continuation.class));
		participant.releaseElement("sender", element);
		verify(element, timeout(3000).times(2)).release(any(Continuation.class));
	}

	@Test public void repeatedErrorCallbackDoesNotLaunchMultipleRetries() throws Exception {
		KurentoParticipant participant = participant();
		MediaElement element = mock(MediaElement.class);
		when(element.getId()).thenReturn("media-id");
		participant.releaseElement("sender", element);
		@SuppressWarnings("unchecked")
		org.mockito.ArgumentCaptor<Continuation<Void>> captor = org.mockito.ArgumentCaptor.forClass(Continuation.class);
		verify(element).release(captor.capture());
		captor.getValue().onError(new IllegalStateException("failed"));
		captor.getValue().onError(new IllegalStateException("late duplicate callback"));
		verify(element, times(2)).release(any(Continuation.class));
	}
}
