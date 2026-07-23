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
package io.aeron.agent;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.CommonEventEncoder.internalEncodeLogHeader;
import static io.aeron.agent.EventConfiguration.MAX_EVENT_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CommonEventDissectorTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[MAX_EVENT_LENGTH]);
    private final StringBuilder builder = new StringBuilder();

    @Test
    void dissectLogStartMessage()
    {
        final long timestampNs = 10_000_001_955L;
        final long timestampMs = 10_000_001L;

        CommonEventDissector.dissectLogStartMessage(timestampNs, timestampMs, ZoneId.of("UTC"), builder);
        assertThat(builder.toString(), equalTo("[10.000001955] log started 1970-01-01 02:46:40.001+0000"));

        builder.delete(0, builder.length());

        CommonEventDissector.dissectLogStartMessage(timestampNs, timestampMs, ZoneId.of("America/New_York"), builder);
        assertThat(builder.toString(), equalTo("[10.000001955] log started 1969-12-31 21:46:40.001-0500"));
    }

    @Test
    void dissectLogHeader()
    {
        internalEncodeLogHeader(buffer, 0, 100, 222, () -> 1234567890);

        final int decodedLength = CommonEventDissector
            .dissectLogHeader("test ctx", EventCodeType.ARCHIVE, buffer, 0, builder);

        assertEquals(LOG_HEADER_LENGTH, decodedLength);
        assertEquals("[1.234567890] test ctx: ARCHIVE [100/222]", builder.toString());
    }

    @Test
    void dissectSocketAddressIpv4()
    {
        final int offset = 16;
        buffer.putInt(offset, 12121, LITTLE_ENDIAN);
        buffer.putInt(offset + SIZE_OF_INT, 4, LITTLE_ENDIAN);
        buffer.putBytes(offset + SIZE_OF_INT * 2, new byte[]{ 127, 0, 0, 1 });

        final int decodedLength = CommonEventDissector.dissectSocketAddress(buffer, offset, builder);

        assertEquals(12, decodedLength);
        assertEquals("127.0.0.1:12121", builder.toString());
    }

    @Test
    void dissectSocketAddressIpv6()
    {
        final int offset = 16;
        buffer.putInt(offset, 7777, LITTLE_ENDIAN);
        buffer.putInt(offset + SIZE_OF_INT, 16, LITTLE_ENDIAN);
        buffer.putBytes(offset + (SIZE_OF_INT * 2),
            new byte[]{ -100, 124, 0, 18, 120, -128, 44, 44, 10, -80, 80, 22, 122, 5, 5, -99 });

        final int decodedLength = CommonEventDissector.dissectSocketAddress(buffer, offset, builder);

        assertEquals(24, decodedLength);
        assertEquals("[9c7c:12:7880:2c2c:ab0:5016:7a05:59d]:7777", builder.toString());
    }

    @Test
    void dissectSocketAddressInvalidLength()
    {
        final int offset = 16;
        buffer.putInt(offset, 555, LITTLE_ENDIAN);
        buffer.putInt(offset + SIZE_OF_INT, 7, LITTLE_ENDIAN);

        final int decodedLength = CommonEventDissector.dissectSocketAddress(buffer, offset, builder);

        assertEquals(15, decodedLength);
        assertEquals("unknown-address:555", builder.toString());
    }
}
