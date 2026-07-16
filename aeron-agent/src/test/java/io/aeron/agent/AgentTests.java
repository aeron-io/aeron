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

import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.agent.ArchiveEventCode.*;
import static io.aeron.agent.ClusterEventCode.ROLE_CHANGE;
import static io.aeron.agent.CommonEventEncoder.LOG_HEADER_LENGTH;
import static io.aeron.agent.ConfigOption.*;
import static io.aeron.agent.DriverEventCode.*;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.ringbuffer.RecordDescriptor.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(InterruptingTestCallback.class)
@Disabled
final class AgentTests
{
    static void startLogging(final Map<String, String> configOptions)
    {
        // EventLogAgent.agentmain(buildAgentArgs(configOptions), ByteBuddyAgent.install());
    }

    static void stopLogging()
    {
        // EventLogAgent.stopLogging();
    }

    static void verifyLogHeader(
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

    @AfterEach
    void afterEach()
    {
        stopLogging();
        TestLoggingAgent.INSTANCE_COUNT.set(0);
        TestLoggingAgent.isOnStartCalled = false;
        TestLoggingAgent.isOnStopCalled = false;
        TestLoggingAgentWithFileName.LOG_FILE_NAME.set(null);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void agentmainShouldUseSystemPropertiesWhenAgentsArgsIsEmpty(final String agentArgs)
    {
        System.setProperty(ENABLED_DRIVER_EVENT_CODES, "admin");
        System.setProperty(
            ENABLED_ARCHIVE_EVENT_CODES,
            "CMD_IN_EXTEND_RECORDING,REPLICATION_SESSION_STATE_CHANGE,CATALOG_RESIZE");
        System.setProperty(DISABLED_ARCHIVE_EVENT_CODES, "REPLICATION_SESSION_STATE_CHANGE");
        System.setProperty(ENABLED_CLUSTER_EVENT_CODES, "all");
        System.setProperty(DISABLED_CLUSTER_EVENT_CODES, "ROLE_CHANGE");
        System.setProperty(READER_CLASSNAME, TestLoggingAgent.class.getName());
        final int instanceCount = TestLoggingAgent.INSTANCE_COUNT.get();
        try
        {
            // EventLogAgent.agentmain(agentArgs, ByteBuddyAgent.install());

            assertEquals(instanceCount + 1, TestLoggingAgent.INSTANCE_COUNT.get());
            assertEquals(
                EnumSet.complementOf(EnumSet.of(
                FRAME_IN,
                FRAME_OUT)),
                DriverComponentLogger.ENABLED_EVENTS);
            assertEquals(
                EnumSet.of(CMD_IN_EXTEND_RECORDING, CATALOG_RESIZE),
                ArchiveComponentLogger.ENABLED_EVENTS);
            assertEquals(
                EnumSet.complementOf(EnumSet.of(ROLE_CHANGE)),
                ClusterComponentLogger.ENABLED_EVENTS);
        }
        finally
        {
            System.clearProperty(ENABLED_DRIVER_EVENT_CODES);
            System.clearProperty(ENABLED_ARCHIVE_EVENT_CODES);
            System.clearProperty(DISABLED_ARCHIVE_EVENT_CODES);
            System.clearProperty(ENABLED_CLUSTER_EVENT_CODES);
            System.clearProperty(DISABLED_CLUSTER_EVENT_CODES);
            System.clearProperty(READER_CLASSNAME);
        }
    }

    @Test
    void shouldFailToStartLoggingIfAgentAlreadyRunning()
    {
        final int instanceCount = TestLoggingAgent.INSTANCE_COUNT.get();
        final Map<String, String> configOptions = new HashMap<>();
        configOptions.put(READER_CLASSNAME, TestLoggingAgent.class.getName());

        startLogging(configOptions); // logging is not started as no events are configured
        assertEquals(instanceCount, TestLoggingAgent.INSTANCE_COUNT.get());

        configOptions.put(ENABLED_DRIVER_EVENT_CODES, "admin");
        startLogging(configOptions); // logging is running now
        assertEquals(instanceCount + 1, TestLoggingAgent.INSTANCE_COUNT.get());

        final IllegalStateException exception =
            assertThrows(IllegalStateException.class, () -> startLogging(configOptions));
        assertEquals("agent already instrumented", exception.getMessage());
    }

    @Test
    void shouldStartLoggingAfterItWasStopped()
    {
        final int instanceCount = TestLoggingAgent.INSTANCE_COUNT.get();
        final Map<String, String> configOptions = new HashMap<>();
        configOptions.put(READER_CLASSNAME, TestLoggingAgent.class.getName());
        configOptions.put(ENABLED_ARCHIVE_EVENT_CODES, CMD_IN_AUTH_CONNECT.name());

        startLogging(configOptions);
        assertEquals(instanceCount + 1, TestLoggingAgent.INSTANCE_COUNT.get());

        stopLogging();
        assertEquals(EnumSet.noneOf(DriverEventCode.class), DriverComponentLogger.ENABLED_EVENTS);
        assertEquals(EnumSet.noneOf(ArchiveEventCode.class), ArchiveComponentLogger.ENABLED_EVENTS);
        assertEquals(EnumSet.noneOf(ClusterEventCode.class), ClusterComponentLogger.ENABLED_EVENTS);

        startLogging(configOptions);
        assertEquals(instanceCount + 2, TestLoggingAgent.INSTANCE_COUNT.get());
    }

    @Test
    void shouldInitializeLoggingAgentWithAFileName()
    {
        assertNull(TestLoggingAgentWithFileName.LOG_FILE_NAME.get());
        final String logFileName = "/dev/shm/my-file.txt";
        final Map<String, String> configOptions = new HashMap<>();
        configOptions.put(READER_CLASSNAME, TestLoggingAgentWithFileName.class.getName());
        configOptions.put(LOG_FILENAME, logFileName);
        configOptions.put(ENABLED_ARCHIVE_EVENT_CODES, "all");

        startLogging(configOptions);
        assertEquals(logFileName, TestLoggingAgentWithFileName.LOG_FILE_NAME.get());
    }

    @Test
    @InterruptAfter(10)
    void shouldStartAndStopLoggingAgent()
    {
        final int instanceCount = TestLoggingAgent.INSTANCE_COUNT.get();
        final Map<String, String> configOptions = new HashMap<>();
        configOptions.put(READER_CLASSNAME, TestLoggingAgent.class.getName());
        configOptions.put(ENABLED_ARCHIVE_EVENT_CODES, "all");

        startLogging(configOptions);
        assertEquals(instanceCount + 1, TestLoggingAgent.INSTANCE_COUNT.get());
        Tests.await(() -> TestLoggingAgent.isOnStartCalled);

        stopLogging();
        Tests.await(() -> TestLoggingAgent.isOnStopCalled);
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
