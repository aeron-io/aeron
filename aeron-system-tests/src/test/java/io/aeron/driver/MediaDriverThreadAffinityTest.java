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
package io.aeron.driver;

import io.aeron.CommonContext;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.topology.AffinityParser;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingStream;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.affinity.ThreadAffinity;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static io.aeron.CommonContext.THREAD_NAMING_NEW;
import static io.aeron.CommonContext.THREAD_NAMING_PROP_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_CONDUCTOR_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_RECEIVER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SENDER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_NETWORK_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_THREAD_NAME;
import static io.aeron.test.TestPropertiesUtil.backupAndOverrideSystemProperties;
import static io.aeron.test.TestPropertiesUtil.restoreSystemProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
class MediaDriverThreadAffinityTest
{
    // NOTE: There is a weirdness to this test in that it relies on Unix/Linux behavior and the cpu count of the
    // machine, which may or may not work on CI (not really sure how many CPUs our runners get for example.
    // It is a bit of a TODO to make this a lot more flexible and hardware agnostic.
    static Stream<Arguments> threadingModes()
    {
        return Stream.of(
            Arguments.of(ThreadingMode.DEDICATED, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, 1,
                AERON_DRIVER_SENDER_THREAD_NAME, 2,
                AERON_DRIVER_RECEIVER_THREAD_NAME, 3,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, 4)),
            Arguments.of(ThreadingMode.SHARED_NETWORK, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, 1,
                AERON_DRIVER_SHARED_NETWORK_THREAD_NAME, 1,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, 4)),
            Arguments.of(ThreadingMode.SHARED, Map.of(
                AERON_DRIVER_SHARED_THREAD_NAME, 1)));
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @SuppressWarnings("try")
    void shouldPinAgentThreadsToConfiguredCpus(
        final ThreadingMode threadingMode, final Map<String, Integer> expectedCpuIndexByThreadName) throws IOException
    {
        TestMediaDriver.notSupportedOnCMediaDriver("CPU affinity configuration is for the Java Media Driver only");

        final String cpusAllowedList = "Cpus_allowed_list:";
        final IntArrayList cpus = AffinityParser.parse(Files.readAllLines(Paths.get("/proc/self/status")).stream()
            .filter((line) -> line.startsWith(cpusAllowedList))
            .map((line) -> line.substring(cpusAllowedList.length()).trim())
            .findFirst()
            .orElseThrow());
        assumeTrue(cpus.size() >= 5, "requires at least 5 allowed CPUs");

        // Short thread names match the 15 char truncated native thread names.
        final Properties overrides = new Properties();
        overrides.setProperty(THREAD_NAMING_PROP_NAME, THREAD_NAMING_NEW);
        final Properties backup = backupAndOverrideSystemProperties(new Properties(), overrides);

        final Map<String, Long> osThreadIdByName = new ConcurrentHashMap<>();
        try (RecordingStream recording = new RecordingStream())
        {
            // Started before the driver so the agent thread start events are captured.
            recording.enable("jdk.ThreadStart");
            recording.onEvent(
                "jdk.ThreadStart",
                (event) ->
                {
                    final RecordedThread thread = event.getThread();
                    if (null != thread && null != thread.getJavaName())
                    {
                        osThreadIdByName.put(thread.getJavaName(), thread.getOSThreadId());
                    }
                });
            recording.startAsync();

            final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(threadingMode)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .conductorCpuAffinity(cpus.getInt(1))
                .senderCpuAffinity(cpus.getInt(2))
                .receiverCpuAffinity(cpus.getInt(3))
                .nativeResourceAgentCpuAffinity(cpus.getInt(4));

            try (TestMediaDriver ignore = TestMediaDriver.launch(context, null))
            {
                expectedCpuIndexByThreadName.forEach((threadName, cpuIndex) ->
                {
                    Tests.await(() -> osThreadIdByName.containsKey(threadName));
                    final int tid = Math.toIntExact(osThreadIdByName.get(threadName));
                    final int expectedCpu = cpus.getInt(cpuIndex);

                    Tests.await(() -> expectedCpu == ThreadAffinity.getAffinityFor(tid));
                    assertEquals(expectedCpu, ThreadAffinity.getAffinityFor(tid), threadName);
                });
            }
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }
}
