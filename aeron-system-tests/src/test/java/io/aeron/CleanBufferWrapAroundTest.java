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
package io.aeron;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.status.RecordingPos;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.protocol.DataHeaderFlyweight.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class CleanBufferWrapAroundTest
{
    private static final int TERM_LENGTH = 64 * 1024;
    private static final int PUBLICATION_WINDOW_LENGTH = TERM_LENGTH / 2;
    private static final int FRAME_LENGTH = 64;
    private static final int NUM_MESSAGES = TERM_LENGTH / FRAME_LENGTH;

    @InterruptAfter(10)
    @Test
    void shouldNotAllowReadingFromDirtyBufferUdp(final @TempDir Path tempDir)
    {
        final FragmentHandler fragmentHandler = new FragmentHandler()
        {
            private long sequenceNr;

            public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
            {
                if (sequenceNr != buffer.getLong(offset))
                {
                    fail("reading from dirty term: expectedSequenceNr=" + sequenceNr + ", actualSequenceNr=" +
                        buffer.getLong(offset) + ": header.termId=" + header.termId() + ", header.termOffset=" +
                        header.termOffset() + ", nextPosition=" + header.position());
                }

                sequenceNr++;
            }
        };

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .spiesSimulateConnection(true)
            .threadingMode(ThreadingMode.DEDICATED);
        final Archive.Context archiveCtx = new Archive.Context()
            .aeronDirectoryName(driverCtx.aeronDirectoryName())
            .archiveDir(tempDir.resolve("archive").toFile())
            .controlChannel("aeron:udp?endpoint=localhost:8010")
            .replicationChannel("aeron:udp?endpoint=localhost:0")
            .threadingMode(ArchiveThreadingMode.SHARED);
        try (ArchivingMediaDriver driver = ArchivingMediaDriver.launch(driverCtx, archiveCtx);
            AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context()
                .aeronDirectoryName(driver.mediaDriver().aeronDirectoryName())
                .controlRequestChannel(archiveCtx.controlChannel())
                .controlResponseChannel(AeronArchive.Configuration.localControlChannel())))
        {
            final long recordingId, initialPosition, finalRecordedPosition;
            final ChannelUri uri = prepareChannelUri("aeron:ipc", 4, 1024, 324);
            try (ExclusivePublication recordedPub =
                aeronArchive.addRecordedExclusivePublication(uri.toString(), 131313))
            {
                initialPosition = recordedPub.position();
                final long targetPosition = initialPosition + 19L * TERM_LENGTH;

                final CountersReader counters = aeronArchive.context().aeron().countersReader();
                final int recordingCounterId = Tests.awaitRecordingCounterId(
                    counters, recordedPub.sessionId(), aeronArchive.archiveId());
                recordingId = RecordingPos.getRecordingId(counters, recordingCounterId);

                long sequenceNr = 0;
                final BufferClaim bufferClaim = new BufferClaim();
                while (recordedPub.position() < targetPosition)
                {
                    final int length = ThreadLocalRandom.current().nextBoolean() ? 32 : 80;
                    while (recordedPub.tryClaim(length, bufferClaim) < 0)
                    {
                        Tests.yield();
                    }

                    bufferClaim.buffer().putLong(bufferClaim.offset(), sequenceNr++);
                    bufferClaim.commit();
                }

                finalRecordedPosition = recordedPub.position();
                Tests.await(() -> counters.getCounterValue(recordingCounterId) == finalRecordedPosition);
            }

            Tests.await(() -> aeronArchive.getStopPosition(recordingId) == finalRecordedPosition);

            final Subscription subscription = aeronArchive.replay(
                recordingId, initialPosition, Long.MAX_VALUE, "aeron:udp?endpoint=localhost:5656", 555);
            while (0 == subscription.imageCount())
            {
                Tests.yield();
            }

            final Image image = subscription.imageAtIndex(0);
            pollSubscription(image, finalRecordedPosition, fragmentHandler);

            assertEquals(0, image.poll(fragmentHandler, 64), "polling in the dirty term");
        }
    }

    @InterruptAfter(10)
    @Test
    void shouldNotAllowReadingFromDirtyBufferIpc()
    {
        final int termId = 9;
        final int sessionId = 42;
        final String channel = "aeron:ipc";
        final ChannelUri uri = prepareChannelUri(channel, termId, 0, sessionId);
        final int streamId = 888;
        final UnsafeBuffer data = new UnsafeBuffer(new byte[TERM_LENGTH]);
        ThreadLocalRandom.current().nextBytes(data.byteArray());
        final DataHeaderFlyweight headerFlyweight = new DataHeaderFlyweight();

        final FragmentHandler fragmentHandler = new FragmentHandler()
        {
            private int expectedTermId = termId;

            public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
            {
                if (expectedTermId != header.termId())
                {
                    fail("reading from dirty term: expectedTermId=" + expectedTermId + ", header.termId=" +
                        header.termId() + ", header.termOffset=" + header.termOffset());
                }

                if (TERM_LENGTH == offset + length)
                {
                    expectedTermId++;
                }
            }
        };

        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.INVOKER));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final long publicationRegistrationId = aeron.asyncAddExclusivePublication(uri.toString(), streamId);
            final long subscriptionRegistrationId = aeron.asyncAddSubscription(channel, streamId);

            final ExclusivePublication publication = awaitPublication(driver, aeron, publicationRegistrationId);
            final Image image = awaitSubscription(driver, aeron, subscriptionRegistrationId);

            final long initialPosition = publication.position();

            long pubLimit = awaitPublicationLimit(driver, publication, initialPosition + PUBLICATION_WINDOW_LENGTH);
            long pubPos = offerBlock(publication, headerFlyweight, data, sessionId, streamId, termId);
            assertEquals(initialPosition + TERM_LENGTH, pubPos);
            assertEquals(pubLimit, publication.positionLimit());

            long subPos = pollSubscription(image, initialPosition + TERM_LENGTH * 3 / 4, fragmentHandler);

            pubLimit = awaitPublicationLimit(driver, publication, subPos + PUBLICATION_WINDOW_LENGTH);
            pubPos = offerBlock(publication, headerFlyweight, data, sessionId, streamId, termId + 1);
            assertEquals(initialPosition + 2 * TERM_LENGTH, pubPos);
            assertEquals(pubLimit, publication.positionLimit());

            subPos = pollSubscription(image, subPos + TERM_LENGTH, fragmentHandler);

            pubLimit = awaitPublicationLimit(driver, publication, subPos + PUBLICATION_WINDOW_LENGTH);
            pubPos = offerBlock(publication, headerFlyweight, data, sessionId, streamId, termId + 2);
            assertEquals(initialPosition + 3 * TERM_LENGTH, pubPos);
            assertEquals(pubLimit, publication.positionLimit());

            subPos = pollSubscription(image, subPos + TERM_LENGTH, fragmentHandler);

            pubLimit = awaitPublicationLimit(driver, publication, subPos + PUBLICATION_WINDOW_LENGTH);
            pubPos = offerBlock(publication, headerFlyweight, data, sessionId, streamId, termId + 3);
            assertEquals(initialPosition + 4 * TERM_LENGTH, pubPos);
            assertEquals(pubLimit, publication.positionLimit());

            subPos = pollSubscription(image, pubPos, fragmentHandler);
            assertEquals(pubPos, subPos);

            assertEquals(0, image.poll(fragmentHandler, 64), "polling in the dirty term");
        }
    }

    private static ChannelUri prepareChannelUri(
        final String channel, final int termId, final int termOffset, final int sessionId)
    {
        final ChannelUri uri = ChannelUri.parse(channel);
        uri.put(CommonContext.TERM_LENGTH_PARAM_NAME, Integer.toString(TERM_LENGTH));
        uri.put(CommonContext.INITIAL_TERM_ID_PARAM_NAME, Integer.toString(termId));
        uri.put(CommonContext.TERM_ID_PARAM_NAME, Integer.toString(termId));
        uri.put(CommonContext.TERM_OFFSET_PARAM_NAME, Integer.toString(termOffset));
        uri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));
        uri.put(CommonContext.PUBLICATION_WINDOW_LENGTH_PARAM_NAME, Integer.toString(PUBLICATION_WINDOW_LENGTH));
        uri.put(CommonContext.RECEIVER_WINDOW_LENGTH_PARAM_NAME, Integer.toString(TERM_LENGTH / 2));
        return uri;
    }

    private static long pollSubscription(
        final Image image, final long targetPosition, final FragmentHandler fragmentHandler)
    {
        while (image.position() < targetPosition)
        {
            if (0 == image.poll(fragmentHandler, 64))
            {
                Tests.yield();
            }
        }
        return image.position();
    }

    private static void runInvoker(final MediaDriver driver)
    {
        if (null != driver.sharedAgentInvoker())
        {
            driver.sharedAgentInvoker().invoke();
        }
    }

    private static long offerBlock(
        final ExclusivePublication publication,
        final DataHeaderFlyweight headerFlyweight,
        final UnsafeBuffer data,
        final int sessionId,
        final int streamId,
        final int termId)
    {
        for (int i = 0; i < NUM_MESSAGES; i++)
        {
            final int offset = i * FRAME_LENGTH;
            headerFlyweight.wrap(data, offset, HEADER_LENGTH);
            headerFlyweight
                .frameLength(FRAME_LENGTH)
                .version(CURRENT_VERSION)
                .flags(BEGIN_AND_END_FLAGS)
                .headerType(HDR_TYPE_DATA);
            headerFlyweight
                .termOffset(offset)
                .sessionId(sessionId)
                .streamId(streamId)
                .termId(termId);
        }

        while (publication.offerBlock(data, 0, data.capacity()) < 0)
        {
            Tests.yield();
        }

        return publication.position();
    }

    private static Image awaitSubscription(
        final MediaDriver driver, final Aeron aeron, final long subscriptionRegistrationId)
    {
        Subscription subscription;
        while (null == (subscription = aeron.getSubscription(subscriptionRegistrationId)) ||
            0 == subscription.imageCount())
        {
            runInvoker(driver);
        }
        return subscription.imageAtIndex(0);
    }

    private static ExclusivePublication awaitPublication(
        final MediaDriver driver, final Aeron aeron, final long publicationRegistrationId)
    {
        ExclusivePublication publication;
        while (null == (publication = aeron.getExclusivePublication(publicationRegistrationId)))
        {
            runInvoker(driver);
        }
        return publication;
    }

    private static long awaitPublicationLimit(
        final MediaDriver driver, final ExclusivePublication publication, final long expectedLimit)
    {
        while (publication.positionLimit() < expectedLimit)
        {
            runInvoker(driver);
        }
        return publication.positionLimit();
    }
}
