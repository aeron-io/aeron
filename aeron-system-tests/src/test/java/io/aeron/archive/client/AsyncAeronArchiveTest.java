/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.codecs.ControlResponseCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.TimeoutException;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.TestContexts;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.SystemUtil;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.StringDescription;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_FOLLOW;
import static io.aeron.archive.client.AeronArchive.REPLAY_ALL_AND_STOP;
import static io.aeron.archive.client.ArchiveException.UNKNOWN_RECORDING;
import static io.aeron.archive.codecs.ControlResponseCode.ERROR;
import static io.aeron.archive.codecs.ControlResponseCode.OK;
import static io.aeron.archive.codecs.ControlResponseCode.RECORDING_UNKNOWN;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(InterruptingTestCallback.class)
class AsyncAeronArchiveTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private File archiveDir;
    private Archive archive;
    private AeronArchive.Context aeronArchiveCtx;
    private AeronArchive aeronArchive;
    private Archive.Context archiveCtx;
    private AsyncAeronArchive asyncAeronArchive;

    @BeforeEach
    void setUp()
    {
        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(TERM_MIN_LENGTH)
            .ipcTermBufferLength(TERM_MIN_LENGTH)
            .dirDeleteOnShutdown(true)
            .aeronDirectoryName(aeronDirectoryName);

        archiveDir = new File(SystemUtil.tmpDirName(), "archive");

        archiveCtx = TestContexts.localhostArchive()
            .catalogCapacity(128 * 1024)
            .segmentFileLength(2 * TERM_MIN_LENGTH)
            .aeronDirectoryName(aeronDirectoryName)
            .deleteArchiveOnStart(true)
            .archiveDir(archiveDir)
            .threadingMode(ArchiveThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driverCtx.aeronDirectory());
        archive = Archive.launch(archiveCtx.clone());
        systemTestWatcher.dataCollector().add(archiveCtx.archiveDir());

        aeronArchiveCtx = TestContexts.localhostAeronArchive()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .messageTimeoutNs(TimeUnit.SECONDS.toNanos(1));
        aeronArchive = AeronArchive.connect(aeronArchiveCtx.clone());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(
            asyncAeronArchive,
            aeronArchive,
            archive,
            driver,
            () -> IoUtil.delete(archiveDir, true));
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("methodlength")
    void test()
    {
        final TestListener listener = new TestListener();
        asyncAeronArchive = new AsyncAeronArchive(aeronArchiveCtx, listener);

        assertFalse(asyncAeronArchive.isConnected());
        pollUntil(listener::onConnectedCount, equalTo(1));
        assertTrue(asyncAeronArchive.isConnected());

        assertTrue(asyncAeronArchive.trySendListRecordingRequest(1, 10));
        pollUntil(listener::controlResponses, hasItem(
            new ControlResponse(1, 10, RECORDING_UNKNOWN, "")));

        assertTrue(asyncAeronArchive.trySendMaxRecordedPositionRequest(2, 11));
        pollUntil(listener::controlResponses, hasItem(
            new ControlResponse(2, UNKNOWN_RECORDING, ERROR, "unknown recording id: 11")));

        final ReplayParams replayParams1 = new ReplayParams().position(0).length(REPLAY_ALL_AND_STOP);
        assertTrue(asyncAeronArchive.trySendReplayRequest(
            3, 12, 2000, IPC_CHANNEL, replayParams1
        ));
        pollUntil(listener::controlResponses, hasItem(
            new ControlResponse(3, UNKNOWN_RECORDING, ERROR, "unknown recording id: 12")));

        final String channel = "aeron:ipc?alias=foo";
        final int streamId = 1000;
        final int sessionId;
        final long position;
        try (ExclusivePublication publication = aeronArchive.addRecordedExclusivePublication(channel, streamId))
        {
            sessionId = publication.sessionId();
            final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
            final int length = buffer.putStringWithoutLengthAscii(0, "Hello, World!");
            position = pollUntil(() -> publication.offer(buffer, 0, length), greaterThan(0L));
        }

        final long recordingId = 0;
        pollUntil(() -> getStopPositionOrNullValue(recordingId), equalTo(position));

        assertTrue(asyncAeronArchive.trySendListRecordingRequest(4, recordingId));
        pollUntil(listener::recordingDescriptors, not(empty()));
        final RecordingDescriptor descriptor = listener.lastRecordingDescriptor();
        assertEquals(4, descriptor.correlationId());
        assertEquals(recordingId, descriptor.recordingId());
        assertEquals(streamId, descriptor.streamId());
        assertEquals(0, descriptor.startPosition());
        assertEquals(position, descriptor.stopPosition());
        assertEquals(sessionId, descriptor.sessionId());
        assertEquals(driver.context().ipcTermBufferLength(), descriptor.termBufferLength());
        assertEquals(driver.context().ipcMtuLength(), descriptor.mtuLength());
        assertEquals(archive.context().segmentFileLength(), descriptor.segmentFileLength());
        assertEquals(ChannelUri.addSessionId(channel, sessionId), descriptor.originalChannel());

        assertTrue(asyncAeronArchive.trySendMaxRecordedPositionRequest(5, recordingId));
        pollUntil(listener::controlResponses, hasItem(new ControlResponse(5, position, OK, "")));

        final int replayStreamId = 2000;
        final String replayChannel = IPC_CHANNEL;
        try (Subscription subscription = aeronArchive.context().aeron().addSubscription(replayChannel, replayStreamId))
        {
            final ReplayParams replayParams2 = new ReplayParams().position(0).length(REPLAY_ALL_AND_STOP);
            assertTrue(asyncAeronArchive.trySendReplayRequest(
                6, recordingId, replayStreamId, replayChannel, replayParams2)
            );
            final ControlResponse replayResponse = pollUntil(
                () -> listener.controlResponseFor(6), notNullValue(ControlResponse.class));
            assertEquals(OK, replayResponse.code());

            final int replaySessionId = (int)replayResponse.relevantId();
            final Image image = pollUntil(
                () -> subscription.imageBySessionId(replaySessionId), notNullValue(Image.class));

            final List<String> messages = new ArrayList<>();
            while (!image.isEndOfStream())
            {
                int workCount = image.poll((buffer, offset, length, header) ->
                    messages.add(buffer.getStringWithoutLengthAscii(offset, length)), 1);

                workCount += asyncAeronArchive.poll();

                if (Thread.interrupted())
                {
                    fail("timed out waiting for EOS: " + image + " " + messages);
                }

                if (workCount == 0)
                {
                    Thread.yield();
                }
            }
            assertEquals(List.of("Hello, World!"), messages);
        }

        archive.close();
        pollUntil(listener::onDisconnectedCount, equalTo(1));

        assertFalse(asyncAeronArchive.isConnected());
        assertFalse(asyncAeronArchive.trySendMaxRecordedPositionRequest(7, recordingId));
        assertFalse(asyncAeronArchive.trySendReplayTokenRequest(8, recordingId));

        final int errorCount = listener.errors().size();
        pollUntil(listener::errors, hasSize(errorCount + 1));
        assertInstanceOf(TimeoutException.class, listener.lastError());

        archive = Archive.launch(archiveCtx.clone().deleteArchiveOnStart(false));
        pollUntil(listener::onConnectedCount, equalTo(2));
        assertTrue(asyncAeronArchive.isConnected());

        assertTrue(asyncAeronArchive.trySendMaxRecordedPositionRequest(9, recordingId));
        pollUntil(listener::controlResponses, hasItem(new ControlResponse(9, position, OK, "")));
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldAllowToStopReplay()
    {
        final TestListener listener = new TestListener();
        asyncAeronArchive = new AsyncAeronArchive(aeronArchiveCtx, listener);

        pollUntil(asyncAeronArchive::isConnected, equalTo(true));

        try (ExclusivePublication ignored = aeronArchive.addRecordedExclusivePublication(IPC_CHANNEL, 5000);
            Subscription subscription = aeronArchive.context().aeron().addSubscription(IPC_CHANNEL, 6000))
        {
            final ReplayParams replayParams = new ReplayParams().position(0).length(REPLAY_ALL_AND_FOLLOW);
            assertTrue(asyncAeronArchive.trySendReplayRequest(
                1, 0, subscription.streamId(), subscription.channel(), replayParams)
            );
            final ControlResponse replayResponse = pollUntil(
                () -> listener.controlResponseFor(1), notNullValue(ControlResponse.class));
            assertEquals(OK, replayResponse.code());

            final int sessionId = (int)replayResponse.relevantId;
            final Image image = pollUntil(() -> subscription.imageBySessionId(sessionId), notNullValue(Image.class));

            assertTrue(asyncAeronArchive.trySendStopReplayRequest(2, replayResponse.relevantId));
            final ControlResponse stopReplayResponse = pollUntil(
                () -> listener.controlResponseFor(2), notNullValue(ControlResponse.class));
            assertEquals(OK, stopReplayResponse.code());

            pollUntil(image::isEndOfStream, equalTo(true));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldCloseItselfIfErrorIsTerminal()
    {
        final TestListener listener = new TestListener();
        asyncAeronArchive = new AsyncAeronArchive(new AeronArchive.Context(), listener);

        pollUntil(listener::errors, not(empty()));
        assertInstanceOf(ConfigurationException.class, listener.lastError());

        assertTrue(asyncAeronArchive.isClosed());
    }

    private long getStopPositionOrNullValue(final long recordingId)
    {
        try
        {
            return aeronArchive.getStopPosition(recordingId);
        }
        catch (final ArchiveException e)
        {
            if (e.errorCode() == UNKNOWN_RECORDING)
            {
                return Aeron.NULL_VALUE;
            }
            throw e;
        }
    }

    private <T> T pollUntil(final Supplier<T> supplier, final Matcher<T> matcher)
    {
        while (true)
        {
            final int workCount = asyncAeronArchive.poll();

            final T value = supplier.get();

            if (matcher.matches(value))
            {
                return value;
            }

            if (Thread.interrupted())
            {
                final Description description = new StringDescription();
                description.appendText("Timed out waiting for condition to become true")
                    .appendText(System.lineSeparator())
                    .appendText("Expected: ")
                    .appendDescriptionOf(matcher)
                    .appendText(System.lineSeparator())
                    .appendText("     but: ");
                matcher.describeMismatch(value, description);
                fail(description.toString());
            }

            if (workCount == 0)
            {
                Thread.yield();
            }
        }
    }

    private record ControlResponse(long correlationId, long relevantId, ControlResponseCode code, String errorMessage)
    {
    }

    private record RecordingDescriptor(
        long controlSessionId,
        long correlationId,
        long recordingId,
        long startTimestamp,
        long stopTimestamp,
        long startPosition,
        long stopPosition,
        int initialTermId,
        int segmentFileLength,
        int termBufferLength,
        int mtuLength,
        int sessionId,
        int streamId,
        String strippedChannel,
        String originalChannel,
        String sourceIdentity)
    {
    }

    private static final class TestListener implements AsyncAeronArchiveListener
    {
        private final List<ControlResponse> controlResponses = new ArrayList<>();
        private final List<RecordingDescriptor> recordingDescriptors = new ArrayList<>();
        private final List<Exception> errors = new ArrayList<>();
        private int onConnectedCount;
        private int onDisconnectedCount;

        public void onConnected()
        {
            onConnectedCount++;
        }

        public void onDisconnected()
        {
            onDisconnectedCount++;
        }

        public void onControlResponse(
            final long correlationId,
            final long relevantId,
            final ControlResponseCode code,
            final String errorMessage)
        {
            controlResponses.add(new ControlResponse(correlationId, relevantId, code, errorMessage));
        }

        public void onError(final Exception error)
        {
            errors.add(error);
        }

        public void onRecordingDescriptor(
            final long controlSessionId,
            final long correlationId,
            final long recordingId,
            final long startTimestamp,
            final long stopTimestamp,
            final long startPosition,
            final long stopPosition,
            final int initialTermId,
            final int segmentFileLength,
            final int termBufferLength,
            final int mtuLength,
            final int sessionId,
            final int streamId,
            final String strippedChannel,
            final String originalChannel,
            final String sourceIdentity)
        {
            recordingDescriptors.add(new RecordingDescriptor(
                controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity));
        }

        public int onConnectedCount()
        {
            return onConnectedCount;
        }

        public int onDisconnectedCount()
        {
            return onDisconnectedCount;
        }

        public List<ControlResponse> controlResponses()
        {
            return controlResponses;
        }

        public ControlResponse controlResponseFor(final long correlationId)
        {
            for (final ControlResponse controlResponse : controlResponses)
            {
                if (controlResponse.correlationId() == correlationId)
                {
                    return controlResponse;
                }
            }

            return null;
        }

        public List<RecordingDescriptor> recordingDescriptors()
        {
            return recordingDescriptors;
        }

        public RecordingDescriptor lastRecordingDescriptor()
        {
            return recordingDescriptors.isEmpty() ? null : recordingDescriptors.get(recordingDescriptors.size() - 1);
        }

        public List<Exception> errors()
        {
            return errors;
        }

        public Exception lastError()
        {
            return errors.isEmpty() ? null : errors.get(errors.size() - 1);
        }
    }
}
