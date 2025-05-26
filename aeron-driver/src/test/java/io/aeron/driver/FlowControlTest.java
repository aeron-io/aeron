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
package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import io.aeron.test.Tests;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

import java.util.stream.Stream;

import static io.aeron.driver.FlowControl.calculateRetransmissionLength;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

class FlowControlTest
{
    private static final int MTU_LENGTH = 1024;
    private static final int COUNTERS_BUFFER_LENGTH = 16 * 1024;
    private static final int TERM_BUFFER_LENGTH = 64 * 1024;

    private static Stream<Arguments> multicastFlowControlStrategies()
    {
        return Stream.of(
            Arguments.of(
                new MaxMulticastFlowControl(), "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max"
            ),
            Arguments.of(
                new MinMulticastFlowControl(), "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min"
            ),
            Arguments.of(
                new TaggedMulticastFlowControl(), "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged"
            )
        );
    }

    @Test
    void shouldUseResendLengthIfSmallestValue()
    {
        final int resendLength = 1024;

        assertEquals(resendLength, calculateRetransmissionLength(resendLength, 64 * 1024, 0, 16));
    }

    @Test
    void shouldClampToTheEndOfTheBuffer()
    {
        final int expectedLength = 512;
        final int termLength = 64 * 1024;
        final int termOffset = termLength - expectedLength;

        assertEquals(expectedLength, calculateRetransmissionLength(1024, termLength, termOffset, 16));
    }

    @Test
    void shouldClampToReceiverWindow()
    {
        final int multiplier = 16;
        final int expectedLength = Configuration.INITIAL_WINDOW_LENGTH_DEFAULT * multiplier;

        assertEquals(expectedLength, calculateRetransmissionLength(4 * 1024 * 1024, 8 * 1024 * 1024, 0, 16));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost"
    })
    void shouldUseDefaultRetransmitReceiverWindowMultipleWhenNotSetInUri(final String uri)
    {
        final MediaDriver.Context context = new MediaDriver.Context();

        final int rrwm = FlowControl.retransmitReceiverWindowMultiple(
            UdpChannel.parse(uri),
            context.multicastFlowControlRetransmitReceiverWindowMultiple()
        );
        final int defaultRrwm = Configuration.multicastFlowControlRetransmitReceiverWindowMultiple();
        assertEquals(defaultRrwm, rrwm);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:8",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,foo:bar,rrwm:8,a:b",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,rrwm:8",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=tagged,g:100/10,t:100ms,rrwm:8"
    })
    void shouldUseRetransmitReceiverWindowMultipleFromUri(final String uri)
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .multicastFlowControlRetransmitReceiverWindowMultiple(2);

        final int rrwm = FlowControl.retransmitReceiverWindowMultiple(
            UdpChannel.parse(uri),
            context.multicastFlowControlRetransmitReceiverWindowMultiple()
        );
        assertEquals(8, rrwm);
    }

    @ParameterizedTest
    @ValueSource(strings = { "a", "", " ", "foo", "0", "-1" })
    void shouldRejectInvalidRetransmitReceiverWindowMultiple(final String rrwmValue)
    {
        final String uri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:" + rrwmValue;

        assertThrows(
            IllegalArgumentException.class,
            () -> FlowControl.retransmitReceiverWindowMultiple(UdpChannel.parse(uri), 2)
        );
    }

    @ParameterizedTest
    @MethodSource("multicastFlowControlStrategies")
    void usesDefaultMulticastRetransmitReceiverWindowMultipleWhenNotSet(final FlowControl flowControl, final String uri)
    {
        final int expectedRrwm = Configuration.multicastFlowControlRetransmitReceiverWindowMultiple();
        final CountersManager countersManager = Tests.newCountersManager(COUNTERS_BUFFER_LENGTH);

        try (MockedStatic<FlowControl> mockedFlowControl = mockStatic(FlowControl.class))
        {
            mockedFlowControl.when(
                () -> FlowControl.retransmitReceiverWindowMultiple(any(), anyInt())
            ).thenCallRealMethod();

            mockedFlowControl.when(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), anyInt())
            ).thenCallRealMethod();

            final MediaDriver.Context context = new MediaDriver.Context()
                .tempBuffer(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]));
            flowControl.initialize(context, countersManager, UdpChannel.parse(uri), 0, 0, 0, 0, 0);

            flowControl.maxRetransmissionLength(0, 32, TERM_BUFFER_LENGTH, MTU_LENGTH);

            mockedFlowControl.verify(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), eq(expectedRrwm))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("multicastFlowControlStrategies")
    void usesMulticastRetransmitReceiverWindowMultipleFromContextWhenNotSetInUri(
        final FlowControl flowControl, final String uri)
    {
        final int expectedRrwm = 2;
        final CountersManager countersManager = Tests.newCountersManager(COUNTERS_BUFFER_LENGTH);

        try (MockedStatic<FlowControl> mockedFlowControl = mockStatic(FlowControl.class))
        {
            mockedFlowControl.when(
                () -> FlowControl.retransmitReceiverWindowMultiple(any(), anyInt())
            ).thenCallRealMethod();

            mockedFlowControl.when(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), anyInt())
            ).thenCallRealMethod();

            final MediaDriver.Context context = new MediaDriver.Context()
                .tempBuffer(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]))
                .multicastFlowControlRetransmitReceiverWindowMultiple(expectedRrwm);
            flowControl.initialize(context, countersManager, UdpChannel.parse(uri), 0, 0, 0, 0, 0);

            flowControl.maxRetransmissionLength(0, 32, TERM_BUFFER_LENGTH, MTU_LENGTH);

            mockedFlowControl.verify(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), eq(expectedRrwm))
            );
        }
    }

    @ParameterizedTest
    @MethodSource("multicastFlowControlStrategies")
    void usesMulticastRetransmitReceiverWindowMultipleFromUriWhenSet(final FlowControl flowControl, final String uri)
    {
        final int expectedRrwm = 8;
        final String fullUri = uri + ",rrwm:" + expectedRrwm;
        final CountersManager countersManager = Tests.newCountersManager(COUNTERS_BUFFER_LENGTH);

        try (MockedStatic<FlowControl> mockedFlowControl = mockStatic(FlowControl.class))
        {
            mockedFlowControl.when(
                () -> FlowControl.retransmitReceiverWindowMultiple(any(), anyInt())
            ).thenCallRealMethod();

            mockedFlowControl.when(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), anyInt())
            ).thenCallRealMethod();

            final MediaDriver.Context context = new MediaDriver.Context()
                .tempBuffer(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]))
                .multicastFlowControlRetransmitReceiverWindowMultiple(expectedRrwm / 2);
            flowControl.initialize(context, countersManager, UdpChannel.parse(fullUri), 0, 0, 0, 0, 0);

            flowControl.maxRetransmissionLength(0, 32, TERM_BUFFER_LENGTH, MTU_LENGTH);

            mockedFlowControl.verify(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), eq(expectedRrwm))
            );
        }
    }

    @Test
    void usesDefaultUnicastRetransmitReceiverWindowMultipleWhenNotSetInContext()
    {
        final String uri = "aeron:udp?endpoint=localhost:0";
        final int expectedRrwm = Configuration.unicastFlowControlRetransmitReceiverWindowMultiple();
        final CountersManager countersManager = Tests.newCountersManager(COUNTERS_BUFFER_LENGTH);

        try (MockedStatic<FlowControl> mockedFlowControl = mockStatic(FlowControl.class))
        {
            mockedFlowControl.when(
                () -> FlowControl.retransmitReceiverWindowMultiple(any(), anyInt())
            ).thenCallRealMethod();

            mockedFlowControl.when(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), anyInt())
            ).thenCallRealMethod();

            final MediaDriver.Context context = new MediaDriver.Context()
                .tempBuffer(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]));
            final UnicastFlowControl flowControl = new UnicastFlowControl();
            flowControl.initialize(context, countersManager, UdpChannel.parse(uri), 0, 0, 0, 0, 0);

            flowControl.maxRetransmissionLength(0, 32, TERM_BUFFER_LENGTH, MTU_LENGTH);

            mockedFlowControl.verify(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), eq(expectedRrwm))
            );
        }
    }

    @Test
    void usesUnicastRetransmitReceiverWindowMultipleFromContextWhenSet()
    {
        final String uri = "aeron:udp?endpoint=localhost:0";
        final int expectedRrwm = 2;
        final CountersManager countersManager = Tests.newCountersManager(COUNTERS_BUFFER_LENGTH);

        try (MockedStatic<FlowControl> mockedFlowControl = mockStatic(FlowControl.class))
        {
            mockedFlowControl.when(
                () -> FlowControl.retransmitReceiverWindowMultiple(any(), anyInt())
            ).thenCallRealMethod();

            mockedFlowControl.when(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), anyInt())
            ).thenCallRealMethod();

            final MediaDriver.Context context = new MediaDriver.Context()
                .tempBuffer(new UnsafeBuffer(new byte[TERM_BUFFER_LENGTH]))
                .unicastFlowControlRetransmitReceiverWindowMultiple(expectedRrwm);
            final UnicastFlowControl flowControl = new UnicastFlowControl();

            flowControl.initialize(context, countersManager, UdpChannel.parse(uri), 0, 0, 0, 0, 0);

            flowControl.maxRetransmissionLength(0, 32, TERM_BUFFER_LENGTH, MTU_LENGTH);

            mockedFlowControl.verify(
                () -> FlowControl.calculateRetransmissionLength(anyInt(), anyInt(), anyInt(), eq(expectedRrwm))
            );
        }
    }
}
