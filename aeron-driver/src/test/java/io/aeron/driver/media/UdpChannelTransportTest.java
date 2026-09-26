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
package io.aeron.driver.media;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.driver.status.SystemCounters;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.ErrorFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.NakFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import io.aeron.protocol.ResponseSetupFlyweight;
import io.aeron.protocol.RttMeasurementFlyweight;
import io.aeron.protocol.SetupFlyweight;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static io.aeron.protocol.HeaderFlyweight.CURRENT_VERSION;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ATS_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ATS_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ATS_SM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_DATA;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_ERR;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_EXT;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_NAK;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_PAD;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RES;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RSP_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_RTTM;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SETUP;
import static io.aeron.protocol.HeaderFlyweight.HDR_TYPE_SM;
import static io.aeron.protocol.HeaderFlyweight.MIN_HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class UdpChannelTransportTest
{
    private final AtomicCounter invalidPacketsCounter = mock(AtomicCounter.class);
    private final MediaDriver.Context context = new MediaDriver.Context();
    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight(new UnsafeBuffer(new byte[128]));
    private TestTransport transport;

    @BeforeEach
    void before()
    {
        final SystemCounters systemCounters = mock(SystemCounters.class);
        when(systemCounters.get(SystemCounterDescriptor.INVALID_PACKETS)).thenReturn(invalidPacketsCounter);
        context.systemCounters(systemCounters);

        transport = new TestTransport(context);
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, MIN_HEADER_LENGTH - 1 })
    void shouldDiscardFrameIfLengthIsInsufficient(final int length)
    {
        assertFalse(transport.isValidFrame(headerFlyweight, length));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -4, -1 })
    void shouldDiscardFrameIfFrameLengthIsNegative(final int frameLength)
    {
        headerFlyweight.frameLength(frameLength);

        assertFalse(transport.isValidFrame(headerFlyweight, MIN_HEADER_LENGTH));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @ValueSource(bytes = { 7, (byte)0xFF, -3 })
    void shouldDiscardFrameIfVersionIsWrong(final byte version)
    {
        headerFlyweight.frameLength(32).version(version);

        assertFalse(transport.isValidFrame(headerFlyweight, MIN_HEADER_LENGTH));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @ValueSource(ints = { HDR_TYPE_ATS_DATA, HDR_TYPE_ATS_SETUP, HDR_TYPE_ATS_SM })
    void shouldDiscardAtsFrames(final int type)
    {
        headerFlyweight.frameLength(32).version(CURRENT_VERSION).headerType(type);

        assertFalse(transport.isValidFrame(headerFlyweight, MIN_HEADER_LENGTH));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @ValueSource(ints = { HDR_TYPE_RSP_SETUP + 1, HDR_TYPE_EXT, Short.MAX_VALUE, Short.MIN_VALUE, -1 })
    void shouldDiscardFrameWithInvalidType(final int type)
    {
        headerFlyweight.frameLength(32).version(CURRENT_VERSION).headerType(type);

        assertFalse(transport.isValidFrame(headerFlyweight, MIN_HEADER_LENGTH));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @MethodSource("invalidFrameLength")
    void shouldDiscardFrameIfFrameLengthIsTooSmallForAGivenFrameType(final int type, final int frameLength)
    {
        headerFlyweight
            .version(CURRENT_VERSION)
            .headerType(type)
            .frameLength(frameLength);

        assertFalse(transport.isValidFrame(headerFlyweight, frameLength));
        verify(invalidPacketsCounter).increment();
    }

    @ParameterizedTest
    @MethodSource("validFrameLength")
    void shouldAcceptFrameIfFrameLengthIsCorrectForAGivenFrameType(final int type, final int frameLength)
    {
        headerFlyweight
            .version(CURRENT_VERSION)
            .headerType(type)
            .frameLength(frameLength);

        assertTrue(transport.isValidFrame(headerFlyweight, frameLength));
        verifyNoInteractions(invalidPacketsCounter);
    }

    @ParameterizedTest
    @MethodSource("validFrameLength")
    void shouldDiscardFrameIfFrameLengthIsGreaterThanBufferLength(final int type, final int frameLength)
    {
        assumeFalse(HDR_TYPE_PAD == type);

        headerFlyweight
            .version(CURRENT_VERSION)
            .headerType(type)
            .frameLength(frameLength);

        assertFalse(transport.isValidFrame(headerFlyweight, frameLength - 1));
        verify(invalidPacketsCounter).increment();
    }

    @Test
    void shouldAcceptPaddingFrameLongerThanBufferCapacity()
    {
        headerFlyweight
            .version(CURRENT_VERSION)
            .headerType(HDR_TYPE_PAD)
            .frameLength(1024);

        assertTrue(transport.isValidFrame(headerFlyweight, DataHeaderFlyweight.HEADER_LENGTH));
        verifyNoInteractions(invalidPacketsCounter);
    }

    private static List<Arguments> invalidFrameLength()
    {
        return List.of(
            Arguments.arguments(HDR_TYPE_PAD, DataHeaderFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_DATA, DataHeaderFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_NAK, NakFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_SM, StatusMessageFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_ERR, ErrorFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_SETUP, SetupFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_RTTM, RttMeasurementFlyweight.HEADER_LENGTH - 1),
            Arguments.arguments(HDR_TYPE_RES, ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH),
            Arguments.arguments(HDR_TYPE_RSP_SETUP, ResponseSetupFlyweight.HEADER_LENGTH - 1));
    }

    private static List<Arguments> validFrameLength()
    {
        return List.of(
            Arguments.arguments(HDR_TYPE_PAD, DataHeaderFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_DATA, DataHeaderFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_NAK, NakFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_SM, StatusMessageFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_ERR, ErrorFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_SETUP, SetupFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_RTTM, RttMeasurementFlyweight.HEADER_LENGTH),
            Arguments.arguments(HDR_TYPE_RES,
                HeaderFlyweight.MIN_HEADER_LENGTH + ResolutionEntryFlyweight.MIN_IPV4_FRAME_LENGTH),
            Arguments.arguments(HDR_TYPE_RSP_SETUP, ResponseSetupFlyweight.HEADER_LENGTH));
    }

    private static final class TestTransport extends UdpChannelTransport
    {
        TestTransport(final MediaDriver.Context context)
        {
            super(null, null, null, null, null, context, -1, -1, -1);
        }
    }
}
