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

import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.IoUtil;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.archive.Archive.segmentFileName;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;
import static java.nio.file.StandardOpenOption.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RecordingSessionTest
{
    private static final int RECORDED_BLOCK_LENGTH = 100;
    private static final long RECORDING_ID = 12345;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int SEGMENT_LENGTH = TERM_BUFFER_LENGTH;

    private static final String CHANNEL = "channel";
    private static final String SOURCE_IDENTITY = "sourceIdentity";
    private static final int STREAM_ID = 54321;

    private static final int SESSION_ID = 12345;
    private static final int TERM_OFFSET = 1024;
    private static final int MTU_LENGTH = 1024;
    private static final long START_POSITION = TERM_OFFSET;
    private static final int INITIAL_TERM_ID = 0;
    private static final ControlSession CONTROL_SESSION = mock(ControlSession.class);

    private final RecordingEventsProxy recordingEventsProxy = mock(RecordingEventsProxy.class);
    private final Counter mockPosition = mock(Counter.class);
    private final Image image = mockImage(mockSubscription());
    private final File archiveDir = ArchiveTests.makeTestDirectory();
    private FileChannel mockLogBufferChannel;
    private UnsafeBuffer mockLogBufferMapped;
    private Path termFilePath;
    private final EpochClock epochClock = mock(EpochClock.class);
    private final NanoClock nanoClock = new NanoClock()
    {
        private long timeNs = 1;

        public long nanoTime()
        {
            timeNs = timeNs * 2;
            return timeNs;
        }
    };
    private Archive.Context context;
    private long positionLong;

    @BeforeEach
    void before() throws Exception
    {
        when(mockPosition.getPlain()).then((invocation) -> positionLong);
        when(mockPosition.get()).then((invocation) -> positionLong);
        doAnswer(
            (invocation) ->
            {
                positionLong = invocation.getArgument(0);
                return null;
            })
            .when(mockPosition).setRelease(anyLong());

        termFilePath = Files.createTempFile("test.rec", "sourceIdentity");

        mockLogBufferChannel = FileChannel.open(termFilePath, CREATE, READ, WRITE);
        mockLogBufferMapped = new UnsafeBuffer(
            mockLogBufferChannel.map(FileChannel.MapMode.READ_WRITE, 0, TERM_BUFFER_LENGTH));

        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();
        headerFlyweight.wrap(mockLogBufferMapped, TERM_OFFSET, DataHeaderFlyweight.HEADER_LENGTH);
        headerFlyweight
            .termOffset(TERM_OFFSET)
            .sessionId(SESSION_ID)
            .streamId(STREAM_ID)
            .headerType(DataHeaderFlyweight.HDR_TYPE_DATA)
            .frameLength(RECORDED_BLOCK_LENGTH);

        context = new Archive.Context()
            .segmentFileLength(SEGMENT_LENGTH)
            .archiveDir(archiveDir)
            .epochClock(epochClock)
            .nanoClock(nanoClock);

        when(CONTROL_SESSION.sessionId()).thenReturn(123L);
    }

    @AfterEach
    void after()
    {
        IoUtil.unmap(mockLogBufferMapped.byteBuffer());
        CloseHelper.close(mockLogBufferChannel);
        IoUtil.delete(archiveDir, false);
        IoUtil.delete(termFilePath.toFile(), false);
    }

    @Test
    void shouldRecordFragmentsFromImage()
    {
        final RecordingSession session = new RecordingSession(
            NULL_VALUE,
            RECORDING_ID,
            START_POSITION,
            SEGMENT_LENGTH,
            CHANNEL,
            recordingEventsProxy,
            image,
            mockPosition,
            context,
            CONTROL_SESSION,
            false,
            mock(ArchiveConductor.Recorder.class));

        assertEquals(RECORDING_ID, session.sessionId());

        session.doWork();

        when(image.blockPoll(any(), anyInt())).thenAnswer(
            (invocation) ->
            {
                final BlockHandler handle = invocation.getArgument(0);
                if (null == handle)
                {
                    return 0;
                }

                handle.onBlock(
                    mockLogBufferMapped,
                    TERM_OFFSET,
                    RECORDED_BLOCK_LENGTH,
                    SESSION_ID,
                    0);

                return RECORDED_BLOCK_LENGTH;
            });

        assertNotEquals(0, session.doWork(), "Expect some work");
        assertNotEquals(0, session.doWork(), "Expect some work");

        final File segmentFile = new File(archiveDir, segmentFileName(RECORDING_ID, 0));
        assertTrue(segmentFile.exists());

        final RecordingSummary recordingSummary = new RecordingSummary();
        recordingSummary.recordingId = RECORDING_ID;
        recordingSummary.startPosition = START_POSITION;
        recordingSummary.segmentFileLength = context.segmentFileLength();
        recordingSummary.initialTermId = INITIAL_TERM_ID;
        recordingSummary.termBufferLength = TERM_BUFFER_LENGTH;
        recordingSummary.streamId = STREAM_ID;
        recordingSummary.sessionId = SESSION_ID;
        recordingSummary.stopPosition = START_POSITION + RECORDED_BLOCK_LENGTH;

        try (RecordingReader reader = new RecordingReader(
            recordingSummary, archiveDir, NULL_POSITION, AeronArchive.NULL_LENGTH))
        {
            final int fragments = reader.poll(
                (buffer, offset, length, frameType, flags, reservedValue) ->
                {
                    final int frameOffset = offset - DataHeaderFlyweight.HEADER_LENGTH;
                    assertEquals(TERM_OFFSET, frameOffset);
                    assertEquals(RECORDED_BLOCK_LENGTH, FrameDescriptor.frameLength(buffer, frameOffset));
                    assertEquals(RECORDED_BLOCK_LENGTH - DataHeaderFlyweight.HEADER_LENGTH, length);
                },
                1);

            assertEquals(1, fragments);
        }

        when(image.blockPoll(any(), anyInt())).thenReturn(0);
        assertEquals(0, session.doWork(), "Expect no work");

        when(image.isClosed()).thenReturn(true);
        session.doWork();
        session.doWork();
        assertTrue(session.isDone());
        session.close();
    }

    private static Subscription mockSubscription()
    {
        final Subscription subscription = mock(Subscription.class);

        when(subscription.channel()).thenReturn(CHANNEL);
        when(subscription.streamId()).thenReturn(STREAM_ID);

        return subscription;
    }

    private static Image mockImage(final Subscription subscription)
    {
        final Image image = mock(Image.class);

        when(image.sessionId()).thenReturn(SESSION_ID);
        when(image.initialTermId()).thenReturn(INITIAL_TERM_ID);
        when(image.sourceIdentity()).thenReturn(SOURCE_IDENTITY);
        when(image.termBufferLength()).thenReturn(TERM_BUFFER_LENGTH);
        when(image.mtuLength()).thenReturn(MTU_LENGTH);
        when(image.joinPosition()).thenReturn(START_POSITION);
        when(image.subscription()).thenReturn(subscription);

        return image;
    }
}
