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

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.RandomWatcher;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.function.Function;

import static io.aeron.AeronCounters.DRIVER_SENDER_LIMIT_TYPE_ID;
import static io.aeron.AeronCounters.DRIVER_SENDER_POSITION_TYPE_ID;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.logbuffer.LogBufferDescriptor.positionBitsToShift;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith({ InterruptingTestCallback.class, EventLogExtension.class })
class SpecifiedPositionPublicationTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();
    @RegisterExtension
    final RandomWatcher randomWatcher = new RandomWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .dirDeleteOnStart(true)
        .dirDeleteOnShutdown(true)
        .aeronDirectoryName(CommonContext.generateRandomDirName())
        .publicationTermBufferLength(TERM_MIN_LENGTH)
        .ipcTermBufferLength(TERM_MIN_LENGTH)
        .threadingMode(ThreadingMode.SHARED)
        .publicationLingerTimeoutNs(0);
    private final DirectBuffer msg = new UnsafeBuffer(new byte[64]);

    @AfterEach
    void after()
    {
        context.deleteDirectory();
    }

    @InterruptAfter(5)
    @ParameterizedTest
    @CsvSource({
        CommonContext.IPC_CHANNEL + ",true",
        "aeron:udp?endpoint=localhost:24325,true",
        CommonContext.IPC_CHANNEL + ",false",
        "aeron:udp?endpoint=localhost:24325,false"
    })
    void shouldStartAtSpecifiedPositionForPublications(final String initialUri, final boolean exclusive)
    {
        final int termLength = 1 << 16;
        final int initialTermId = randomWatcher.random().nextInt();
        final int activeTermId = initialTermId + randomWatcher.random().nextInt(Integer.MAX_VALUE);
        final int positionBitsToShift = positionBitsToShift(termLength);
        final int termOffset = randomWatcher.random().nextInt(termLength) & -FrameDescriptor.FRAME_ALIGNMENT;
        final long startPosition = computePosition(
            activeTermId,
            termOffset,
            positionBitsToShift,
            initialTermId);
        final PositionCalculator positionCalculator = new PositionCalculator(startPosition, termLength, termOffset);
        final long nextPosition = positionCalculator.addMessage(DataHeaderFlyweight.HEADER_LENGTH + msg.capacity());

        final String channel = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength)
            .build();

        final int streamId = 1001;
        final Function<Aeron, Publication> publicationSupplier = exclusive ?
            (a) -> a.addExclusivePublication(channel, streamId) : (a) -> a.addPublication(channel, streamId);

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = publicationSupplier.apply(aeron))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            assertEquals(startPosition, publication.position());

            Tests.await(() -> publication.offer(msg) > 0);
            assertEquals(nextPosition, publication.position());

            final FragmentHandler fragmentHandler =
                (buffer, offset, length, header) -> assertEquals(nextPosition, header.position());

            Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
        }
    }

    @InterruptAfter(5)
    @ParameterizedTest
    @ValueSource(strings = {
        CommonContext.IPC_CHANNEL,
        "aeron:udp?endpoint=localhost:24325"
    })
    void shouldValidateSpecifiedPositionForConcurrentPublications(final String initialUri)
    {
        final int termLength = TERM_MIN_LENGTH;
        final int initialTermId = randomWatcher.random().nextInt();
        final int activeTermId = initialTermId + randomWatcher.random().nextInt(Integer.MAX_VALUE);
        final int positionBitsToShift = positionBitsToShift(termLength);
        final int termOffset = randomWatcher.random().nextInt(termLength) & -FrameDescriptor.FRAME_ALIGNMENT;
        final long startPosition = computePosition(
            activeTermId,
            termOffset,
            positionBitsToShift,
            initialTermId);
        final int totalMessageLength = DataHeaderFlyweight.HEADER_LENGTH + msg.capacity();
        final PositionCalculator positionCalculator = new PositionCalculator(startPosition, termLength, termOffset);
        final long positionMsg1 = positionCalculator.addMessage(totalMessageLength);
        final long positionMsg2 = positionCalculator.addMessage(totalMessageLength);
        final long positionMsg3 = positionCalculator.addMessage(totalMessageLength);

        final String channel = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength)
            .build();

        final String invalidPositionUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition + FrameDescriptor.FRAME_ALIGNMENT, initialTermId, termLength)
            .build();
        final String invalidInitialTermIdUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId + 1, termLength)
            .build();
        final String invalidTermLengthUri = new ChannelUriStringBuilder(initialUri)
            .initialPosition(startPosition, initialTermId, termLength << 1)
            .build();

        final int streamId = 1001;

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = aeron.addPublication(channel, streamId))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            assertEquals(startPosition, publication.position());

            Tests.await(() -> publication.offer(msg) > 0);
            assertEquals(positionMsg1, publication.position());

            Tests.await(() -> subscription.poll((buffer, offset, length, header) -> {}, 1) == 1);

            try (Publication publication2 = aeron.addPublication(channel, streamId))
            {
                assertEquals(positionMsg1, publication2.position());
                Tests.await(() -> publication.offer(msg) > 0);
                assertEquals(positionMsg2, publication2.position());
                final FragmentHandler fragmentHandler =
                    (buffer, offset, length, header) -> assertEquals(positionMsg2, header.position());
                Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
            }

            try (Publication publication3 = aeron.addPublication(initialUri, streamId))
            {
                assertEquals(positionMsg2, publication3.position());
                Tests.await(() -> publication.offer(msg) > 0);
                assertEquals(positionMsg3, publication3.position());
                final FragmentHandler fragmentHandler =
                    (buffer, offset, length, header) -> assertEquals(positionMsg3, header.position());
                Tests.await(() -> subscription.poll(fragmentHandler, 1) == 1);
            }

            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidPositionUri, streamId));
            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidInitialTermIdUri, streamId));
            assertThrows(RegistrationException.class, () -> aeron.addPublication(invalidTermLengthUri, streamId));
        }
    }

    @InterruptAfter(5)
    @ParameterizedTest
    @ValueSource(strings = {
        CommonContext.IPC_CHANNEL,
        "aeron:udp?endpoint=localhost:24325"
    })
    void shouldValidateSpecifiedPositionForConcurrentPublicationsInitiallyUnspecified(final String initialUri)
    {
        final int streamId = 1001;

        try (
            TestMediaDriver mediaDriver = TestMediaDriver.launch(context, testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            Subscription subscription = aeron.addSubscription(initialUri, streamId);
            Publication publication = aeron.addPublication(initialUri, streamId))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            final String channel = new ChannelUriStringBuilder(initialUri)
                .initialPosition(publication.position(), publication.initialTermId(), publication.termBufferLength())
                .build();

            try (Publication publication2 = aeron.addPublication(channel, streamId))
            {
                assertEquals(publication.position(), publication2.position());
                assertEquals(publication.initialTermId(), publication2.initialTermId());
            }
        }
    }

    @InterruptAfter(10)
    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void shouldNotAllowPublicationToConnectIfSubscriberPositionIsMoreThatOneTermOff(final boolean newestFirst)
    {
        final int streamId = 876;
        final int termLength = 1024 * 1024;
        final int positionBitsToShift = positionBitsToShift(termLength);
        final int initialTermId = -199;
        final int termId = initialTermId + (int)(4_000_000_000L / termLength);
        final long highPos = computePosition(termId, 1408, positionBitsToShift, initialTermId);
        final long lowPos =
            computePosition(initialTermId + 3, 128, positionBitsToShift, initialTermId);
        final long pub1Pos, pub2Pos;
        if (newestFirst)
        {
            pub1Pos = highPos;
            pub2Pos = lowPos;
        }
        else
        {
            pub2Pos = highPos;
            pub1Pos = lowPos;
        }

        final String uri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost";
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder(uri);
        builder.termLength(termLength).mtu(2048).sessionId(42).linger(0L);
        final String pub1Uri = builder.initialPosition(pub1Pos, initialTermId, termLength).build();
        final String pub2Uri = builder.initialPosition(pub2Pos, initialTermId, termLength).build();

        final String aeronDir = context.aeronDirectoryName();
        try (
            TestMediaDriver driver1 = TestMediaDriver.launch(
                context.clone().aeronDirectoryName(aeronDir + "-A"), testWatcher);
            TestMediaDriver driver2 = TestMediaDriver.launch(
                context.clone().aeronDirectoryName(aeronDir + "-B"), testWatcher);
            Aeron aeron1 = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(driver1.aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            Aeron aeron2 = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(driver2.aeronDirectoryName())
                .subscriberErrorHandler(RethrowingErrorHandler.INSTANCE));
            Subscription subscription = aeron1.addSubscription(uri, streamId);
            Publication pub1 = aeron1.addExclusivePublication(pub1Uri, streamId))
        {
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(pub1);

            while (pub1.offer(msg, 0, msg.capacity()) < 0)
            {
                Tests.yield();
            }

            final FragmentHandler fragmentHandler = (buffer, offset, length, header) ->
                assertEquals(pub1.position(), header.position());
            final Image image = subscription.imageBySessionId(pub1.sessionId());
            while (image.position() < pub1.position())
            {
                image.poll(fragmentHandler, 1);
            }

            // sends setup eliciting SM
            final Subscription subscription2 = aeron2.addSubscription(uri, streamId);
            Tests.awaitConnected(subscription2);
            assertEquals(pub1.position(), subscription2.imageBySessionId(pub1.sessionId()).position());

            final CountersReader countersReader = aeron2.countersReader();
            assertEquals(
                0,
                countersReader.getCounterValue(SystemCounterDescriptor.STATUS_MESSAGES_REJECTED.id()));

            final Publication pub2 = aeron2.addExclusivePublication(pub2Uri, streamId);
            final int sndPosCounterId = countersReader.findByTypeIdAndRegistrationId(
                DRIVER_SENDER_POSITION_TYPE_ID, pub2.registrationId());
            final int sndLmtCounterId = countersReader.findByTypeIdAndRegistrationId(
                DRIVER_SENDER_LIMIT_TYPE_ID, pub2.registrationId());

            Tests.await(
                () -> 0 != countersReader.getCounterValue(SystemCounterDescriptor.STATUS_MESSAGES_REJECTED.id()));

            // this publisher should not connect
            assertFalse(pub2.isConnected());
            assertEquals(pub2Pos, pub2.position());
            assertEquals(pub2Pos, pub2.positionLimit());
            assertEquals(pub2Pos, countersReader.getCounterValue(sndPosCounterId));
            assertEquals(pub2Pos, countersReader.getCounterValue(sndLmtCounterId));

            // ensure such publisher can be closed
            final int channelStatusId = pub2.channelStatusId();
            pub2.close();
            Tests.await(() -> RECORD_ALLOCATED != countersReader.getCounterState(channelStatusId));
        }
    }

    static final class PositionCalculator
    {
        final int termLength;
        long position;
        int termRemaining;

        PositionCalculator(final long startingPosition, final int termLength, final int termOffset)
        {
            this.position = startingPosition;
            this.termLength = termLength;
            this.termRemaining = termLength - termOffset;
        }

        long addMessage(final int totalMessageLength)
        {
            if (termRemaining < totalMessageLength)
            {
                position += termRemaining;
                termRemaining = termLength;
            }

            position += totalMessageLength;
            termRemaining -= totalMessageLength;

            return position;
        }
    }
}
