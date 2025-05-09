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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.driver.FlowControl.calculateRetransmissionLength;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlowControlTest
{
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

    @Test
    void shouldUseDefaultRetransmitReceiverWindowMultipleWhenNotSet()
    {
        final String uri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max";
        final MediaDriver.Context context = new MediaDriver.Context();

        final int rrwm = FlowControl.retransmitReceiverWindowMultiple(context, UdpChannel.parse(uri));
        final int defaultRrwm = Configuration.flowControlRetransmitReceiverWindowMultiple();
        assertEquals(defaultRrwm, rrwm);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:8",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,foo:bar,rrwm:8,a:b",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,rrwm:8",
    })
    void shouldUseRetransmitReceiverWindowMultipleFromUri(final String uri)
    {
        final MediaDriver.Context context = new MediaDriver.Context().flowControlRetransmitReceiverWindowMultiple(2);

        final int rrwm = FlowControl.retransmitReceiverWindowMultiple(context, UdpChannel.parse(uri));
        assertEquals(8, rrwm);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max",
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost"
    })
    void shouldUseRetransmitReceiverWindowMultipleFromContextWhenNotSetInUri(final String uri)
    {
        final MediaDriver.Context context = new MediaDriver.Context().flowControlRetransmitReceiverWindowMultiple(2);

        final int rrwm = FlowControl.retransmitReceiverWindowMultiple(context, UdpChannel.parse(uri));
        assertEquals(2, rrwm);
    }

    @ParameterizedTest
    @ValueSource(strings = { "a", "", " ", "foo", "0", "-1" })
    void shouldRejectInvalidRetransmitReceiverWindowMultiple(final String rrwmValue)
    {
        final String uri = "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=max,rrwm:" + rrwmValue;
        final MediaDriver.Context context = new MediaDriver.Context().flowControlRetransmitReceiverWindowMultiple(2);

        assertThrows(
            IllegalArgumentException.class,
            () -> FlowControl.retransmitReceiverWindowMultiple(context, UdpChannel.parse(uri))
        );
    }
}
