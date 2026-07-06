/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.ErrorCode;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.ArchiveEvent;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthorisationService;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.concurrent.TimeUnit;

import static io.aeron.archive.ControlSession.State.DONE;
import static io.aeron.archive.ControlSession.State.INIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ControlSessionTest
{
    private static final long CONTROL_PUBLICATION_ID = 777;
    private static final long COUNTER_REGISTRATION_ID = 333;
    private static final long CONNECT_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(5);
    private static final long SESSION_LIVENESS_CHECK_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(100);
    private static final int CONTROL_SESSION_ID = 42;
    private static final int CORRELATION_ID = 2;
    private static final int CONTROL_PUBLICATION_STREAM_ID = 5555;
    private static final String CONTROL_PUBLICATION_CHANNEL = "aeron:ipc?alias=test-control-response";

    private final ControlSessionAdapter mockDemuxer = mock(ControlSessionAdapter.class);
    private ArchiveConductor archiveConductor;
    private final CountedErrorHandler mockErrorHandler = mock(CountedErrorHandler.class);
    private final Aeron mockAeron = mock(Aeron.class);
    private final ExclusivePublication mockControlPublication = mock(ExclusivePublication.class);
    private final ControlResponseProxy mockProxy = mock(ControlResponseProxy.class);
    private final ControlSessionProxy mockSessionProxy = mock(ControlSessionProxy.class);
    private final Authenticator mockAuthenticator = mock(Authenticator.class);
    private final AuthorisationService mockAuthorisationService = mock(AuthorisationService.class);
    private final CachedEpochClock cachedEpochClock = new CachedEpochClock();
    private ControlSession session;

    @BeforeEach
    void before()
    {
        archiveConductor = new SharedModeArchiveConductor(new Archive.Context()
            .countedErrorHandler(mockErrorHandler)
            .nanoClock(SystemNanoClock.INSTANCE)
            .epochClock(SystemEpochClock.INSTANCE)
            .aeron(mockAeron)
            .authenticatorSupplier(() -> mockAuthenticator)
            .authorisationServiceSupplier(() -> mockAuthorisationService)
            .controlChannelEnabled(false));

        session = new ControlSession(
            CONTROL_SESSION_ID,
            CORRELATION_ID,
            CONNECT_TIMEOUT_MS,
            SESSION_LIVENESS_CHECK_INTERVAL_NS,
            COUNTER_REGISTRATION_ID,
            CONTROL_PUBLICATION_CHANNEL,
            CONTROL_PUBLICATION_STREAM_ID,
            null,
            mockDemuxer,
            mockAeron,
            archiveConductor,
            cachedEpochClock,
            mockProxy,
            mockAuthenticator,
            mockSessionProxy);

        when(mockAeron.asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID))
            .thenReturn(CONTROL_PUBLICATION_ID);
        when(mockAeron.getExclusivePublication(CONTROL_PUBLICATION_ID)).thenReturn(mockControlPublication);
    }

    @Test
    void shouldTimeoutIfConnectSentButPublicationNotConnected()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(false);

        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }

    @Test
    void shouldTimeoutIfConnectSentButPublicationFailsToSend()
    {
        when(mockControlPublication.isClosed()).thenReturn(false);
        when(mockControlPublication.isConnected()).thenReturn(true);

        session.doWork();
        session.sendOkResponse(1L);
        session.doWork();

        cachedEpochClock.update(CONNECT_TIMEOUT_MS + 1L);
        session.doWork();
        assertEquals(DONE, session.state());
        assertTrue(session.isDone());
    }

    @Test
    void shouldRetryAddingControlPublicationWhenResourceTemporarilyUnavailable()
    {
        final RegistrationException resourceUnavailable = new RegistrationException(
            1,
            ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE.value(),
            ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE,
            "WARN - resource temporarily unavailable, please retry");

        when(mockAeron.asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID))
            .thenReturn(CONTROL_PUBLICATION_ID, CONTROL_PUBLICATION_ID + 1);

        when(mockAeron.getExclusivePublication(CONTROL_PUBLICATION_ID))
            .thenReturn(null)
            .thenThrow(resourceUnavailable);

        when(mockAeron.getExclusivePublication(CONTROL_PUBLICATION_ID + 1))
            .thenReturn(null)
            .thenReturn(mockControlPublication);

        final Counter counter = mock(Counter.class);
        when(mockAeron.getCounter(COUNTER_REGISTRATION_ID)).thenReturn(null, counter);

        assertEquals(ControlSession.State.INIT, session.state());

        session.doWork();
        verify(mockAeron, times(1))
            .asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID);
        verify(mockAeron, times(1)).getExclusivePublication(CONTROL_PUBLICATION_ID);
        assertEquals(ControlSession.State.INIT, session.state());

        session.doWork();
        verify(mockAeron, times(1))
            .asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID);
        verify(mockAeron, times(2)).getExclusivePublication(CONTROL_PUBLICATION_ID);
        assertEquals(ControlSession.State.INIT, session.state());

        session.doWork();
        verify(mockAeron, times(2))
            .asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID);
        verify(mockAeron, times(1)).getExclusivePublication(CONTROL_PUBLICATION_ID + 1);
        assertEquals(ControlSession.State.INIT, session.state());

        try (MockedStatic<AeronCounters> mockedCountersUtil = mockStatic(AeronCounters.class))
        {
            final int counterId = 444;
            when(counter.id()).thenReturn(counterId);
            when(mockAeron.context()).thenReturn(mock(Aeron.Context.class));
            when(mockControlPublication.registrationId()).thenReturn(CONTROL_PUBLICATION_ID + 2);

            session.doWork();
            verify(mockAeron, times(2))
                .asyncAddExclusivePublication(CONTROL_PUBLICATION_CHANNEL, CONTROL_PUBLICATION_STREAM_ID);
            verify(mockAeron, times(2)).getExclusivePublication(CONTROL_PUBLICATION_ID + 1);
            assertEquals(ControlSession.State.CONNECTING, session.state());

            mockedCountersUtil.verify(() ->
                AeronCounters.setReferenceId(null, null, counterId, CONTROL_PUBLICATION_ID + 2), times(1));
        }
    }

    @Test
    void shouldChangeStateAndCaptureAbortReason()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());

        final String reason = "stop execution";
        session.abort(reason);

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertSame(reason, session.abortReason());
    }

    @Test
    void shouldChangeStateCaptureAbortReasonAndAbortActiveListing()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());
        final Session listingSession = mock(Session.class);
        session.activeListing(listingSession);

        final String reason = "stop execution";
        session.abort(reason);

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertSame(reason, session.abortReason());
        verify(listingSession).abort(reason);
    }

    @Test
    void abortIsANoOpIfSessionIsDone()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());
        final Session listingSession = mock(Session.class);
        session.activeListing(listingSession);

        session.state(ControlSession.State.DONE, "test");

        session.abort(ControlSession.SESSION_CLOSED_MSG);

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertNull(session.abortReason());
        verifyNoInteractions(listingSession);
    }

    @Test
    void abortShouldIgnoreAdditionalCalls()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());
        final Session listingSession = mock(Session.class);
        session.activeListing(listingSession);

        final String reason1 = "call1";
        session.abort(reason1);
        session.abort("call2");
        session.abort("call3");

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertEquals(reason1, session.abortReason());
        verify(listingSession).abort(reason1);
    }

    @Test
    void cleanTerminationCannotBeAborted()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());
        final Session listingSession = mock(Session.class);
        session.activeListing(listingSession);

        session.abort(ControlSession.SESSION_CLOSED_MSG);
        session.abort("call2");
        session.abort("call3");

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertEquals(ControlSession.SESSION_CLOSED_MSG, session.abortReason());
        verify(listingSession).abort(ControlSession.SESSION_CLOSED_MSG);
    }

    @Test
    void cleanTerminationCanOverwritePreviousAbortMessage()
    {
        assertFalse(session.isDone());
        assertFalse(session.hasActiveListing());
        final Session listingSession = mock(Session.class);
        session.activeListing(listingSession);

        session.abort("call1");
        session.abort(ControlSession.SESSION_CLOSED_MSG);
        session.abort("call3");

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertEquals(ControlSession.SESSION_CLOSED_MSG, session.abortReason());
        verify(listingSession).abort(ControlSession.SESSION_CLOSED_MSG);
        verify(listingSession).abort("call1");
    }

    @Test
    void shouldLogWarningIfSessionIsAborted()
    {
        final String abortReason = "test abort reason";
        session.abort(abortReason);

        session.close();

        final ArgumentCaptor<Throwable> error = ArgumentCaptor.forClass(Throwable.class);
        verify(mockErrorHandler).onError(error.capture());
        final Throwable throwable = error.getValue();
        final ArchiveEvent event = assertInstanceOf(ArchiveEvent.class, throwable);
        assertEquals(AeronException.Category.WARN, event.category());
        assertEquals("WARN - controlSessionId=" + CONTROL_SESSION_ID + " (controlResponseStreamId=" +
            CONTROL_PUBLICATION_STREAM_ID + " controlResponseChannel=" + CONTROL_PUBLICATION_CHANNEL +
            ") terminated: " + abortReason, event.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        ControlSession.SESSION_CLOSED_MSG,
        ControlSession.RESPONSE_NOT_CONNECTED_MSG,
        ControlSession.REQUEST_IMAGE_NOT_AVAILABLE_MSG + " : abc" })
    void shouldNotLogWarningIfDisconnectedOrClosed(final String reason)
    {
        session.abort(reason);

        session.close();

        verifyNoInteractions(mockErrorHandler);
    }

    @Test
    void shouldAbortSessionIfPublicationCreationFails()
    {
        final IllegalStateException expected = new IllegalStateException("Haha");
        when(mockAeron.getExclusivePublication(CONTROL_PUBLICATION_ID)).thenThrow(expected);
        assertEquals(INIT, session.state());

        final IllegalStateException actual = assertThrowsExactly(IllegalStateException.class, session::doWork);
        assertSame(expected, actual);

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertEquals("failed with exception: Haha", session.abortReason());
    }

    @Test
    void shouldAbortSessionIfCounterCreationFails()
    {
        final NumberFormatException expected = new NumberFormatException("xxx");
        when(mockAeron.getCounter(COUNTER_REGISTRATION_ID)).thenThrow(expected);
        assertEquals(INIT, session.state());

        final NumberFormatException actual = assertThrowsExactly(NumberFormatException.class, session::doWork);
        assertSame(expected, actual);

        assertTrue(session.isDone());
        assertEquals(DONE, session.state());
        assertEquals("failed with exception: xxx", session.abortReason());
    }
}
