/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.driver.logging;

import io.aeron.protocol.*;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.protocol.HeaderFlyweight.*;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.agrona.PrintBufferUtil.prettyHexDump;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DriverProtocolBinaryRendererTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[512]);
    private final StringBuilder sb = new StringBuilder();
    private final DriverProtocolBinaryRenderer renderer = new DriverProtocolBinaryRenderer();

    @Test
    void renderFrameTypePad()
    {
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_PAD);
        flyweight.flags((short)13);
        flyweight.frameLength(100);
        flyweight.sessionId(42);
        flyweight.streamId(5);
        flyweight.termId(16);
        flyweight.termOffset(1045);

        renderer.append(sb, DriverEventCode.FRAME_IN.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=PAD flags=00001101 frameLength=100 sessionId=42 streamId=5 termId=16 termOffset=1045",
            sb.toString());
    }

    @Test
    void renderFrameTypeData()
    {
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_DATA);
        flyweight.flags((short)23);
        flyweight.frameLength(77);
        flyweight.sessionId(12);
        flyweight.streamId(51);
        flyweight.termId(6);
        flyweight.termOffset(444);

        renderer.append(sb, DriverEventCode.FRAME_IN.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=DATA flags=00010111 frameLength=77 sessionId=12 streamId=51 termId=6 termOffset=444",
            sb.toString());
    }

    @Test
    void renderFrameTypeDataWithExtraBytes()
    {
        final DriverProtocolBinaryRenderer driverProtocolRenderer = new DriverProtocolBinaryRenderer(true);
        final byte[] payload = "TestPayload".getBytes(UTF_8);

        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_DATA);
        flyweight.flags((short)23);
        flyweight.frameLength(DataHeaderFlyweight.HEADER_LENGTH + payload.length);
        flyweight.sessionId(12);
        flyweight.streamId(51);
        flyweight.termId(6);
        flyweight.termOffset(444);
        buffer.putBytes(DataHeaderFlyweight.HEADER_LENGTH, payload);

        driverProtocolRenderer.append(sb, DriverEventCode.FRAME_IN.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=DATA flags=00010111 frameLength=" + (DataHeaderFlyweight.HEADER_LENGTH + payload.length) +
            " sessionId=12 streamId=51 termId=6 termOffset=444 payload=" +
            prettyHexDump(new UnsafeBuffer(payload)),
            sb.toString());
    }

    @Test
    void renderFrameTypeStatusMessage()
    {
        final StatusMessageFlyweight flyweight = new StatusMessageFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_SM);
        flyweight.flags((short)7);
        flyweight.frameLength(121);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.consumptionTermId(4);
        flyweight.consumptionTermOffset(18);
        flyweight.receiverWindowLength(2048);
        flyweight.receiverId(11);

        renderer.append(sb, DriverEventCode.FRAME_OUT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=SM flags=00000111 frameLength=121 sessionId=5 streamId=8 termId=4 termOffset=18 " +
            "receiverWindowLength=2048 receiverId=11",
            sb.toString());
    }

    @Test
    void renderFrameTypeNak()
    {
        final NakFlyweight flyweight = new NakFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_NAK);
        flyweight.flags((short)2);
        flyweight.frameLength(54);
        flyweight.sessionId(5);
        flyweight.streamId(8);
        flyweight.termId(20);
        flyweight.termOffset(0);
        flyweight.length(999999);

        renderer.append(sb, DriverEventCode.FRAME_OUT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=NAK flags=00000010 frameLength=54 sessionId=5 streamId=8 termId=20 termOffset=0 length=999999",
            sb.toString());
    }

    @Test
    void renderFrameTypeSetup()
    {
        final SetupFlyweight flyweight = new SetupFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_SETUP);
        flyweight.flags((short)200);
        flyweight.frameLength(1);
        flyweight.sessionId(15);
        flyweight.streamId(18);
        flyweight.activeTermId(81);
        flyweight.initialTermId(69);
        flyweight.termOffset(10);
        flyweight.termLength(444);
        flyweight.mtuLength(8096);
        flyweight.ttl(20_000);

        renderer.append(sb, DriverEventCode.FRAME_IN.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=SETUP flags=11001000 frameLength=1 sessionId=15 streamId=18 activeTermId=81 initialTermId=69 " +
            "termOffset=10 termLength=444 mtu=8096 ttl=20000",
            sb.toString());
    }

    @Test
    void renderFrameTypeRtt()
    {
        final RttMeasurementFlyweight flyweight = new RttMeasurementFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_RTTM);
        flyweight.flags((short)20);
        flyweight.frameLength(100);
        flyweight.sessionId(0);
        flyweight.streamId(1);
        flyweight.echoTimestampNs(123456789);
        flyweight.receptionDelta(354);
        flyweight.receiverId(22);

        renderer.append(sb, DriverEventCode.FRAME_OUT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=RTT flags=00010100 frameLength=100 sessionId=0 streamId=1 echoTimestampNs=123456789 " +
            "receptionDelta=354 receiverId=22",
            sb.toString());
    }

    @Test
    void renderFrameTypeError()
    {
        final ErrorFlyweight flyweight = new ErrorFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HDR_TYPE_ERR);
        flyweight.flags((short)ErrorFlyweight.HAS_GROUP_ID_FLAG);
        flyweight.frameLength(876);
        flyweight.sessionId(42);
        flyweight.streamId(999);
        flyweight.receiverId(-4723947284689L);
        flyweight.groupTag(1_000_000_000_000_1L);
        flyweight.errorCode(1959);
        flyweight.errorMessage("test err msg string");

        renderer.append(sb, DriverEventCode.FRAME_OUT.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals(
            "type=ERR flags=00001000 frameLength=59 sessionId=42 streamId=999 receiverId=-4723947284689 " +
            "groupTag=10000000000001 errorCode=1959 errorMessage=\"test err msg string\"",
            sb.toString());
    }

    @Test
    void renderFrameTypeUnknown()
    {
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(Integer.MAX_VALUE);

        renderer.append(sb, DriverEventCode.FRAME_IN.toEventCodeId(), buffer, 0, buffer.capacity());

        assertEquals("type=UNKNOWN(65535)", sb.toString());
    }
}
