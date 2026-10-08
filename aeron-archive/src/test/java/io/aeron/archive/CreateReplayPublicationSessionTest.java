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
import io.aeron.Counter;
import io.aeron.ErrorCode;
import io.aeron.ExclusivePublication;
import io.aeron.exceptions.RegistrationException;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.UncheckedIOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class CreateReplayPublicationSessionTest
{
    public static final int FILE_IO_MAX_LENGTH = 1024 * 1024;
    public static final int SEGMENT_FILE_LENGTH = 128 * 1024;
    public static final int TERM_BUFFER_LENGTH = 64 * 1024;
    private final Counter limitPositionCounter = mock(Counter.class);
    private final Aeron aeron = mock(Aeron.class);
    private final ControlSession controlSession = mock(ControlSession.class);
    private final ArchiveConductor conductor = mock(ArchiveConductor.class);

    @Test
    void shouldRetryPublicationCreationIfResourceTemporaryUnavailable()
    {
        final long correlationId = 42;
        final long recordingId = 1;
        final long replayPosition = 0;
        final long replayLength = 1024;
        final long startPosition = 0;
        final long stopPosition = Long.MAX_VALUE;
        final int streamId = 444;
        final String replayChannel = "aeron:udp?endpoint=localhost:5555";
        final int replayStreamId = 777;
        final long pubRegId1 = 111111111111111L, pubRegId2 = 2;
        when(aeron.asyncAddExclusivePublication(replayChannel, replayStreamId)).thenReturn(pubRegId1, pubRegId2);
        final ExclusivePublication publication = mock(ExclusivePublication.class);
        final ErrorCode code = ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE;
        final RegistrationException exception =
            new RegistrationException(correlationId, code.value(), code, "try again");
        when(aeron.getExclusivePublication(pubRegId1)).thenThrow(exception);
        when(aeron.getExclusivePublication(pubRegId2)).thenReturn(null, publication);

        final CreateReplayPublicationSession session = new CreateReplayPublicationSession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            streamId,
            replayChannel,
            replayStreamId,
            FILE_IO_MAX_LENGTH,
            limitPositionCounter,
            aeron,
            controlSession,
            conductor);

        assertEquals(0, session.doWork());
        assertEquals(1, session.doWork());
        assertEquals(1, session.doWork());
        assertEquals(0, session.doWork());

        session.close();

        final InOrder inOrder = inOrder(conductor, controlSession, aeron);
        inOrder.verify(aeron).asyncAddExclusivePublication(replayChannel, replayStreamId);
        inOrder.verify(aeron).getExclusivePublication(pubRegId1);
        inOrder.verify(aeron).asyncAddExclusivePublication(replayChannel, replayStreamId);
        inOrder.verify(aeron, times(2)).getExclusivePublication(pubRegId2);
        inOrder.verify(conductor).newReplaySession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            streamId,
            FILE_IO_MAX_LENGTH,
            controlSession,
            limitPositionCounter,
            publication);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldAbortOnAnyOtherException()
    {
        final long correlationId = 1;
        final long recordingId = 7;
        final long replayPosition = 1024 * 1024L * 1024L;
        final long replayLength = Long.MAX_VALUE;
        final long startPosition = 0;
        final long stopPosition = Long.MAX_VALUE;
        final int streamId = 3333;
        final String replayChannel = "aeron:udp?endpoint=localhost:1717";
        final int replayStreamId = -765;
        final long registrationId = 42;
        when(aeron.asyncAddExclusivePublication(replayChannel, replayStreamId)).thenReturn(registrationId);
        final UncheckedIOException exception = new UncheckedIOException(new IOException("haha"));
        when(aeron.getExclusivePublication(registrationId)).thenThrow(exception);

        final CreateReplayPublicationSession session = new CreateReplayPublicationSession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            streamId,
            replayChannel,
            replayStreamId,
            FILE_IO_MAX_LENGTH,
            limitPositionCounter,
            aeron,
            controlSession,
            conductor);

        final UncheckedIOException error = assertThrowsExactly(UncheckedIOException.class, session::doWork);
        assertSame(exception, error);

        assertEquals(0, session.doWork());
        session.close();

        final InOrder inOrder = inOrder(conductor, controlSession, aeron);
        inOrder.verify(aeron).asyncAddExclusivePublication(replayChannel, replayStreamId);
        inOrder.verify(aeron).getExclusivePublication(registrationId);
        inOrder.verify(conductor).onReplayEnd();
        inOrder.verify(controlSession).sendErrorResponse(
            correlationId,
            "failed to create replay publication: " + exception);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldDoNoWorkAfterAbortIsCalled()
    {
        final CreateReplayPublicationSession session = new CreateReplayPublicationSession(
            1,
            2,
            3,
            4,
            5,
            6,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            7,
            null,
            8,
            FILE_IO_MAX_LENGTH,
            limitPositionCounter,
            aeron,
            controlSession,
            conductor);

        assertFalse(session.isDone());

        session.abort("test");

        assertTrue(session.isDone());
        assertEquals(0, session.doWork());
    }

    @Test
    void closeShouldRemovePublicationIfCreationIsStillInProgress()
    {
        final long correlationId = 1;
        final long recordingId = 7;
        final long replayPosition = 1024 * 1024L * 1024L;
        final long replayLength = Long.MAX_VALUE;
        final long startPosition = 0;
        final long stopPosition = Long.MAX_VALUE;
        final int streamId = 3333;
        final String replayChannel = "aeron:udp?endpoint=localhost:1717";
        final int replayStreamId = -765;
        final long registrationId = 42;
        when(aeron.asyncAddExclusivePublication(replayChannel, replayStreamId)).thenReturn(registrationId);

        final CreateReplayPublicationSession session = new CreateReplayPublicationSession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            streamId,
            replayChannel,
            replayStreamId,
            FILE_IO_MAX_LENGTH,
            limitPositionCounter,
            aeron,
            controlSession,
            conductor);

        assertEquals(1, session.doWork());

        final InOrder inOrder = inOrder(conductor, controlSession, aeron);
        inOrder.verify(aeron).asyncAddExclusivePublication(replayChannel, replayStreamId);
        inOrder.verify(aeron).getExclusivePublication(registrationId);

        session.close();
        inOrder.verify(aeron).asyncRemovePublication(registrationId);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closeIsANoOpIfPublicationWasNotCreated()
    {
        final long correlationId = 1;
        final long recordingId = 7;
        final long replayPosition = 1024 * 1024L * 1024L;
        final long replayLength = Long.MAX_VALUE;
        final long startPosition = 0;
        final long stopPosition = Long.MAX_VALUE;
        final int streamId = 3333;
        final String replayChannel = "aeron:udp?endpoint=localhost:1717";
        final int replayStreamId = -765;
        final long registrationId = 42;

        final CreateReplayPublicationSession session = new CreateReplayPublicationSession(
            correlationId,
            recordingId,
            replayPosition,
            replayLength,
            startPosition,
            stopPosition,
            SEGMENT_FILE_LENGTH,
            TERM_BUFFER_LENGTH,
            streamId,
            replayChannel,
            replayStreamId,
            FILE_IO_MAX_LENGTH,
            limitPositionCounter,
            aeron,
            controlSession,
            conductor);

        session.close();

        verifyNoInteractions(aeron, controlSession, conductor);
    }
}
