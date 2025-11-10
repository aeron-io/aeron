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

import io.aeron.test.CapturingPrintStream;
import net.bytebuddy.agent.builder.AgentBuilder;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.CachedEpochClock;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventLogReaderAgentTest
{
    @Test
    void shouldListEnabledLoggersOnStartPrintStream()
    {
        final CapturingPrintStream out = new CapturingPrintStream();
        testOnStart(null, out.resetAndGetPrintStream(), out::flushAndGetContent);
    }

    @Test
    void shouldListEnabledLoggersOnStartFile(@TempDir final Path tempDir)
    {
        final Path file = tempDir.resolve("test-out.log");
        assertFalse(Files.exists(file));

        testOnStart(
            file.toString(),
            null,
            () ->
            {
                assertTrue(Files.exists(file));
                try
                {
                    return Files.readString(file);
                }
                catch (final IOException ex)
                {
                    throw new UncheckedIOException(ex);
                }
            });
    }

    @Test
    void shouldListSingleEnabledLoggerOnStart()
    {
        final CapturingPrintStream out = new CapturingPrintStream();
        final CachedNanoClock nanoClock = new CachedNanoClock();
        nanoClock.update(System.nanoTime());
        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.update(System.currentTimeMillis());
        final EventLogReaderAgent logReaderAgent = new EventLogReaderAgent(
            null,
            out.resetAndGetPrintStream(),
            nanoClock,
            epochClock,
            List.of(new TestLogger(EventCodeType.SEQUENCER.getTypeCode(), "sequencer v0")));

        logReaderAgent.onStart();

        final String actual = out.flushAndGetContent();
        final StringBuilder expected = new StringBuilder();
        CommonEventDissector.dissectLogStartMessage(
            nanoClock.nanoTime(), epochClock.time(), ZoneId.systemDefault(), expected);
        expected.append(", enabled loggers: {SEQUENCER: sequencer v0}");
        expected.append(System.lineSeparator());
        assertThat(actual, equalTo(expected.toString()));
    }

    private static void testOnStart(final String fileName, final PrintStream out, final Supplier<String> loggedMessage)
    {
        final CachedNanoClock nanoClock = new CachedNanoClock();
        nanoClock.update(System.nanoTime());
        final CachedEpochClock epochClock = new CachedEpochClock();
        epochClock.update(System.currentTimeMillis());
        final EventLogReaderAgent logReaderAgent = new EventLogReaderAgent(
            fileName,
            out,
            nanoClock,
            epochClock,
            List.of(
                new TestLogger(EventCodeType.SEQUENCER.getTypeCode(), "sequencer v0"),
                new TestLogger(100, "logger 100"),
                new TestLogger(EventCodeType.DRIVER.getTypeCode(), "driver v1"),
                new TestLogger(EventCodeType.USER.getTypeCode(), "user logger"),
                new TestLogger(EventCodeType.STANDBY.getTypeCode(), "standby version=1.49.0 commit=100")));

        logReaderAgent.onStart();

        final String actual = loggedMessage.get();
        final StringBuilder expected = new StringBuilder();
        CommonEventDissector.dissectLogStartMessage(
            nanoClock.nanoTime(), epochClock.time(), ZoneId.systemDefault(), expected);
        expected.append(", enabled loggers: {DRIVER: driver v1, STANDBY: standby version=1.49.0 commit=100, ")
            .append("SEQUENCER: sequencer v0, USER: user logger, ")
            .append("io.aeron.agent.EventLogReaderAgentTest$TestLogger: logger 100}");
        expected.append(System.lineSeparator());
        assertThat(actual, equalTo(expected.toString()));
    }

    @Test
    void throwsNullPointerExceptionIfNanoClockIsNull()
    {
        assertThrowsExactly(NullPointerException.class, () -> new EventLogReaderAgent(
            null,
            System.out,
            null,
            SystemEpochClock.INSTANCE,
            List.of(new TestLogger(1, "x"))));
    }

    @Test
    void throwsNullPointerExceptionIfEpochClockIsNull()
    {
        assertThrowsExactly(NullPointerException.class, () -> new EventLogReaderAgent(
            null,
            System.out,
            SystemNanoClock.INSTANCE,
            null,
            List.of(new TestLogger(1, "x"))));
    }

    @Test
    void throwsNullPointerExceptionIfFileIsNullAndPrintStreamIsNull()
    {
        assertThrowsExactly(NullPointerException.class, () -> new EventLogReaderAgent(
            null,
            null,
            SystemNanoClock.INSTANCE,
            SystemEpochClock.INSTANCE,
            List.of(new TestLogger(1, "x"))));
    }

    private record TestLogger(int eventType, String version) implements ComponentLogger
    {
        public int typeCode()
        {
            return eventType;
        }

        public void decode(
            final MutableDirectBuffer buffer, final int offset, final int eventCodeId, final StringBuilder builder)
        {

        }

        public AgentBuilder addInstrumentation(final AgentBuilder agentBuilder, final Map<String, String> configOptions)
        {
            return null;
        }

        public void reset()
        {
        }
    }
}
