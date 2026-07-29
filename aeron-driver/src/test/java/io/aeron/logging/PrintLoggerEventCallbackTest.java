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
package io.aeron.logging;

import io.aeron.driver.logging.DriverEventCode;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.test.CapturingPrintStream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import static io.aeron.logging.CborUtils.NO_TAG;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PrintLoggerEventCallbackTest
{
    @Test
    void shouldRenderProtocolFrameUsingDriverBinaryRenderer()
    {
        final CapturingPrintStream capturingPrintStream = new CapturingPrintStream();
        final PrintLoggerEventCallback printLoggerEventCallback = new PrintLoggerEventCallback(
            capturingPrintStream.resetAndGetPrintStream());

        final int typeCode = EventCodeType.DRIVER.getTypeCode();
        final int eventCode = DriverEventCode.FRAME_IN.toEventCodeId();
        final String eventName = DriverEventCode.FRAME_IN.name();
        final long timestamp = 2827937432L;

        final String key = "frame";
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[300]);
        final DataHeaderFlyweight flyweight = new DataHeaderFlyweight();
        flyweight.wrap(buffer, 0, 300);
        flyweight.headerType(HeaderFlyweight.HDR_TYPE_DATA);
        flyweight.flags((short)23);
        flyweight.frameLength(77);
        flyweight.sessionId(12);
        flyweight.streamId(51);
        flyweight.termId(6);
        flyweight.termOffset(444);

        printLoggerEventCallback.onHeader(typeCode, eventCode, eventName, timestamp);
        printLoggerEventCallback.onValue(key, NO_TAG, buffer);
        printLoggerEventCallback.onFooter(false);

        final StringBuilder sb = new StringBuilder();
        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(EventCodeType.DRIVER.name()).append(": ");
        sb.append(eventName).append(" ");
        sb.append(key).append("=");
        sb.append("type=DATA flags=00010111 frameLength=77 sessionId=12 streamId=51 termId=6 termOffset=444");
        sb.append("\n");

        assertEquals(capturingPrintStream.flushAndGetContent(), sb.toString());
    }
}
