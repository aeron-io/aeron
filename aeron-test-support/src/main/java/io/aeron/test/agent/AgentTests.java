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
package io.aeron.test.agent;

import io.aeron.test.InterruptingTestCallback;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.logging.CommonEventEncoder.LOG_HEADER_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.HEADER_LENGTH;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.encodedMsgOffset;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.lengthOffset;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.typeOffset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@ExtendWith(InterruptingTestCallback.class)
public final class AgentTests
{
    static void startLogging(final Map<String, String> configOptions)
    {
//        EventLogAgent.agentmain(buildAgentArgs(configOptions), ByteBuddyAgent.install());
    }

    static void stopLogging()
    {
//        EventLogAgent.stopLogging();
    }

    public static void verifyLogHeader(
        final UnsafeBuffer logBuffer,
        final int recordOffset,
        final int eventCodeId,
        final int captureLength,
        final int length)
    {
        assertEquals(
            HEADER_LENGTH + LOG_HEADER_LENGTH + captureLength,
            logBuffer.getInt(lengthOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(eventCodeId, logBuffer.getInt(typeOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(captureLength, logBuffer.getInt(encodedMsgOffset(recordOffset), LITTLE_ENDIAN));
        assertEquals(length, logBuffer.getInt(encodedMsgOffset(recordOffset + SIZE_OF_INT), LITTLE_ENDIAN));
        assertNotEquals(0, logBuffer.getLong(encodedMsgOffset(recordOffset + SIZE_OF_INT * 2), LITTLE_ENDIAN));
    }

    private static class TestLoggingAgent implements Agent
    {
        static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();
        static volatile boolean isOnStartCalled = false;
        static volatile boolean isOnStopCalled = false;

        TestLoggingAgent()
        {
            INSTANCE_COUNT.getAndIncrement();
        }

        public int doWork()
        {
            return 0;
        }

        public String roleName()
        {
            return "test-logging-agent";
        }

        public void onStart()
        {
            isOnStartCalled = true;
        }

        public void onClose()
        {
            isOnStopCalled = true;
        }
    }

    private static class TestLoggingAgentWithFileName implements Agent
    {
        static final AtomicReference<String> LOG_FILE_NAME = new AtomicReference<>();

        TestLoggingAgentWithFileName(final String logFileName)
        {
            LOG_FILE_NAME.set(logFileName);
        }

        public int doWork()
        {
            return 0;
        }

        public String roleName()
        {
            return "test-logging-agent-with-file-name";
        }
    }
}
