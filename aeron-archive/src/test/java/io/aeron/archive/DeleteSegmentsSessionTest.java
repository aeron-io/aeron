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

import org.agrona.ErrorHandler;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.io.File;
import java.util.ArrayDeque;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.codecs.RecordingSignal.DELETE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class DeleteSegmentsSessionTest
{
    private final ControlSession controlSession = mock(ControlSession.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    @Test
    void shouldComputeMaxDeletePosition()
    {
        final long recordingId = 18;
        final long correlationId = 42;
        final ArrayDeque<File> files = new ArrayDeque<>(List.of(
            new File(segmentFileName(recordingId, 1024 * 1024)),
            new File(segmentFileName(recordingId, 0) + ".del"),
            new File(segmentFileName(recordingId, correlationId)),
            new File(segmentFileName(recordingId, -9999999999L)),
            new File(segmentFileName(recordingId, 12345000000L) + ".del"),
            new File(segmentFileName(recordingId, 56678))));

        final DeleteSegmentsSession deleteSegmentsSession = new DeleteSegmentsSession(
            recordingId, correlationId, files, controlSession, errorHandler, false);

        assertEquals(12345000000L, deleteSegmentsSession.maxDeletePosition());
    }

    @Test
    void shouldRemoveSessionUponClose()
    {
        final long recordingId = 0;
        final long correlationId = -4732948723L;
        final ArrayDeque<File> files = new ArrayDeque<>(List.of());
        final ArchiveConductor conductor = mock(ArchiveConductor.class);
        when(controlSession.archiveConductor()).thenReturn(conductor);

        final DeleteSegmentsSession deleteSegmentsSession = new DeleteSegmentsSession(
            recordingId, correlationId, files, controlSession, errorHandler, false);

        deleteSegmentsSession.close();

        final InOrder inOrder = inOrder(controlSession, conductor);
        inOrder.verify(controlSession).archiveConductor();
        inOrder.verify(conductor).removeDeleteSegmentsSession(deleteSegmentsSession);
        inOrder.verify(controlSession).sendSignal(correlationId, recordingId, NULL_VALUE, NULL_VALUE, DELETE);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldStayInAwaitReplaysStopWhileReplaysAreInProgress()
    {
        final long recordingId = 5;
        final long correlationId = 100;
        final ArrayDeque<File> files = new ArrayDeque<>(List.of(
            new File(segmentFileName(recordingId, 0))));
        final ArchiveConductor conductor = mock(ArchiveConductor.class);
        when(controlSession.archiveConductor()).thenReturn(conductor);
        when(conductor.hasInProgressReplays(recordingId)).thenReturn(true);

        final DeleteSegmentsSession session = new DeleteSegmentsSession(
            recordingId, correlationId, files, controlSession, errorHandler, true);

        assertEquals(0, session.doWork());
        assertEquals(0, session.doWork());
        assertEquals(1, files.size());
        verify(conductor, times(2)).hasInProgressReplays(recordingId);
    }

    @Test
    void shouldTransitionToDeleteFilesStateWhenReplaysStop()
    {
        final long recordingId = 5;
        final long correlationId = 100;
        final ArrayDeque<File> files = new ArrayDeque<>(List.of(
            new File(segmentFileName(recordingId, 0))));
        final ArchiveConductor conductor = mock(ArchiveConductor.class);
        when(controlSession.archiveConductor()).thenReturn(conductor);
        when(conductor.hasInProgressReplays(recordingId)).thenReturn(true, false);

        final DeleteSegmentsSession session = new DeleteSegmentsSession(
            recordingId, correlationId, files, controlSession, errorHandler, true);

        assertEquals(0, session.doWork()); // replays in progress, no work
        assertEquals(1, session.doWork()); // replays done, transitions to DELETE_FILES
        assertEquals(1, session.doWork()); // DELETE_FILES, deletes file
        assertEquals(0, files.size());
    }
}
