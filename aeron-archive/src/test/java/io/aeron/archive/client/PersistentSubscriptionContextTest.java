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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.CountersAnswer;
import io.aeron.test.Tests;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.stream.Stream;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PersistentSubscriptionContextTest
{
    private PersistentSubscription.Context context;

    @BeforeEach
    void setup()
    {
        final Aeron aeron = mock(Aeron.class);
        final CountersManager countersManager = Tests.newCountersManager(64 * 1024);

        when(aeron.addCounter(anyInt(), anyString())).then(CountersAnswer.mapTo(countersManager));

        context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(1)
            .replayChannel(IPC_CHANNEL)
            .replayStreamId(2)
            .aeron(aeron)
            .aeronArchiveContext(new AeronArchive.Context());
    }

    @Test
    void canOnlyConcludeOnce()
    {
        context.conclude();

        assertThrows(ConcurrentConcludeException.class, context::conclude);
    }

    @Test
    void contextMustHaveAnArchiveContext()
    {
        context.aeronArchiveContext(null);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveRecordingId()
    {
        context.recordingId(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void contextMustHaveLiveChannel(final String channel)
    {
        context.liveChannel(channel);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveLiveStreamId()
    {
        context.liveStreamId(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void contextMustHaveReplayChannel(final String channel)
    {
        context.replayChannel(channel);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void mustNotRejoinOnReplayChannels()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder("aeron:udp?endpoint=localhost:0");
        final String configuredReplayChannel = builder.rejoin(true).build();

        context.replayChannel(configuredReplayChannel);
        context.conclude();
        final String actualReplayChannel = context.replayChannel();
        final ChannelUriStringBuilder channelUri = new ChannelUriStringBuilder(actualReplayChannel);
        assertFalse(channelUri.rejoin());
    }

    @Test
    void contextMustHaveReplayStreamId()
    {
        context.replayStreamId(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextThrowsIfStartPositionIsInvalid()
    {
        context.startPosition(-3);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextThrowsIfRecordingIdIsInvalid()
    {
        context.recordingId(-2);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextCanBeCloned()
    {
        final PersistentSubscription.Context clonedCtx = context.clone();

        assertNotSame(context, clonedCtx);

        assertEquals(context.startPosition(), clonedCtx.startPosition());
        assertEquals(context.recordingId(), clonedCtx.recordingId());
        assertEquals(context.liveChannel(), clonedCtx.liveChannel());
        assertEquals(context.liveStreamId(), clonedCtx.liveStreamId());

        assertSame(context.listener(), clonedCtx.listener());
        assertSame(context.aeronArchiveContext(), clonedCtx.aeronArchiveContext());
    }

    @Test
    void contextShouldCreateListenerIfNoneProvided()
    {
        context.listener(null);
        context.conclude();

        assertNotNull(context.listener());
    }

    @Test
    void contextShouldCreateStateCounterIfNoneProvided()
    {
        context.stateCounter(null);
        context.conclude();

        assertNotNull(context.stateCounter());
    }

    @Test
    void contextShouldCreateJoinDifferenceCounterIfNoneProvided()
    {
        context.joinDifferenceCounter(null);
        context.conclude();

        assertNotNull(context.joinDifferenceCounter());
    }

    @Test
    void contextShouldCreateLiveLeftCounterIfNoneProvided()
    {
        context.liveLeftCounter(null);
        context.conclude();

        assertNotNull(context.liveLeftCounter());
    }

    @Test
    void contextShouldCreateLiveJoinedCounterIfNoneProvided()
    {
        context.liveJoinedCounter(null);
        context.conclude();

        assertNotNull(context.liveJoinedCounter());
    }

    @ParameterizedTest
    @MethodSource("replayAndControlChannels")
    void replayAndControlChannelMediaTypesMustMatchWhenUsingResponseChannels(
        final boolean expectSuccess,
        final String replayChannel,
        final String archiveControlRequestChannel,
        final String archiveControlResponseChannel)
    {
        context.replayChannel(replayChannel).aeronArchiveContext()
            .controlRequestChannel(archiveControlRequestChannel)
            .controlResponseChannel(archiveControlResponseChannel);
        if (expectSuccess)
        {
            context.conclude();
        }
        else
        {
            assertThrows(ConfigurationException.class, () -> context.conclude());
        }
    }

    private static Stream<Arguments> replayAndControlChannels()
    {
        return Stream.of(
            arguments(true, "aeron:udp?endpoint=localhost:0", null, null),
            arguments(
              true,
              "aeron:udp?endpoint=localhost:0",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?endpoint=localhost:0"
            ),
            arguments(
              true,
              "aeron:udp?endpoint=localhost:0",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(true, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc"),
            arguments(true, "aeron:udp?endpoint=localhost:0", "aeron:ipc", "aeron:ipc?control-mode=response"),
            arguments(true, "aeron:udp?control=localhost:10001|control-mode=response", null, null),
            arguments(
              true,
              "aeron:udp?control=localhost:10001|control-mode=response",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?endpoint=localhost:0"
            ),
            arguments(
              true,
              "aeron:udp?control=localhost:10001|control-mode=response",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(false, "aeron:udp?control=localhost:10001|control-mode=response", "aeron:ipc", "aeron:ipc"),
            arguments(
              false,
              "aeron:udp?control=localhost:10001|control-mode=response",
              "aeron:ipc",
              "aeron:ipc?control-mode=response"
            ),
            arguments(true, "aeron:ipc", null, null),
            arguments(true, "aeron:ipc", "aeron:udp?endpoint=localhost:8010", "aeron:udp?endpoint=localhost:0"),
            arguments(
              true,
              "aeron:ipc",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(true, "aeron:ipc", "aeron:ipc", "aeron:ipc"),
            arguments(true, "aeron:ipc", "aeron:ipc", "aeron:ipc?control-mode=response"),
            arguments(true, "aeron:ipc?control-mode=response", null, null),
            arguments(
              false,
              "aeron:ipc?control-mode=response",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?endpoint=localhost:0"
            ),
            arguments(
              false,
              "aeron:ipc?control-mode=response",
              "aeron:udp?endpoint=localhost:8010",
              "aeron:udp?control-mode=response|control=localhost:10002"
            ),
            arguments(true, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc"),
            arguments(true, "aeron:ipc?control-mode=response", "aeron:ipc", "aeron:ipc?control-mode=response")
        );
    }
}
