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

import io.aeron.ChannelUri;
import io.aeron.ChannelUriStringBuilder;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArchiveConductorTest
{
    @Test
    void strippedChannelBuilderShouldCopyKeyParameters() throws ReflectiveOperationException
    {
        final ChannelUri uri = ChannelUri.parse("""
            aeron:udp?term-length=1g|mtu=2048|session-id=tag:424242424242|rcv-wnd=32k|so-sndbuf=2m|so-rcvbuf=1m|\
            endpoint=some-host:7777|interface=192.168.0.1/24|control-mode=dynamic|control=localhost:5555|\
            tether=false|reliable=false|linger=1321ms|sparse=false|cc=cubic|alias=test-stripped-channel|\
            fc=tagged,g:1111/3,t:4s|gtag=2222|tags=17,432|tag=5|ttl=3|eos=true|rejoin=false|ssc=true|group=true|\
            response-correlation-id=88888888881|response-endpoint=publisher:10101|\
            media-rcv-ts-offset=reserved|channel-rcv-ts-offset=136|channel-snd-ts-offset=144|\
            init-term-id=300|term-id=341|term-offset=4096|nak-delay=100ns|\
            untethered-window-limit-timeout=3s|untethered-linger-timeout=2s|untethered-resting-timeout=1s|\
            max-resend=13|stream-id=-654|pub-wnd=32m
            """.stripIndent());

        final ChannelUriStringBuilder builder = ArchiveConductor.strippedChannelBuilder(uri);
        assertNotNull(builder);
        assertEquals("udp", builder.media());
        assertEquals("some-host:7777", builder.endpoint());
        assertEquals("192.168.0.1/24", builder.networkInterface());
        assertEquals("localhost:5555", builder.controlEndpoint());
        assertEquals("dynamic", builder.controlMode());
        assertEquals("17,432", builder.tags());
        assertEquals(false, builder.rejoin());
        assertEquals(true, builder.group());
        assertEquals(false, builder.tether());
        assertEquals("tagged,g:1111/3,t:4s", builder.flowControl());
        assertEquals(2222, builder.groupTag());
        assertEquals("cubic", builder.congestionControl());
        assertEquals(1024 * 1024, builder.socketRcvbufLength());
        assertEquals(2 * 1024 * 1024, builder.socketSndbufLength());
        assertEquals(32 * 1024, builder.receiverWindowLength());
        assertEquals("144", builder.channelSendTimestampOffset());
        assertEquals("136", builder.channelReceiveTimestampOffset());
        assertEquals("reserved", builder.mediaReceiveTimestampOffset());
        assertTrue(builder.isSessionIdTagged());
        final Field sessionIdField = builder.getClass().getDeclaredField("sessionId");
        sessionIdField.trySetAccessible();
        assertEquals(424242424242L, sessionIdField.get(builder));
        assertEquals("test-stripped-channel", builder.alias());
        assertEquals("88888888881", builder.responseCorrelationId());
        assertEquals("publisher:10101", builder.responseEndpoint());
        assertEquals(3, builder.ttl());
        assertEquals(-654, builder.streamId());
        assertNull(builder.reliable());
        assertNull(builder.linger());
        assertNull(builder.sparse());
        assertNull(builder.eos());
        assertNull(builder.spiesSimulateConnection());
        assertNull(builder.initialTermId());
        assertNull(builder.termId());
        assertNull(builder.termOffset());
        assertNull(builder.nakDelay());
        assertNull(builder.untetheredWindowLimitTimeoutNs());
        assertNull(builder.untetheredLingerTimeoutNs());
        assertNull(builder.untetheredRestingTimeoutNs());
        assertNull(builder.maxResend());
        assertNull(builder.publicationWindowLength());
    }
}
