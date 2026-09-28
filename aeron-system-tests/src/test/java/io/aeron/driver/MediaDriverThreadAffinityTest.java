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
import io.aeron.test.ThreadAffinityRecording;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

import static io.aeron.driver.MediaDriver.AERON_DRIVER_CONDUCTOR_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_RECEIVER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SENDER_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_NETWORK_THREAD_NAME;
import static io.aeron.driver.MediaDriver.AERON_DRIVER_SHARED_THREAD_NAME;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@EnabledOnOs(OS.LINUX)
class MediaDriverThreadAffinityTest
{
    // Requested CPUs are remapped by rank onto the effective cpuset, so the expected value is the index into it.
    static Stream<Arguments> threadingModes()
    {
        return Stream.of(
            Arguments.of(ThreadingMode.DEDICATED, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, 0,
                AERON_DRIVER_SENDER_THREAD_NAME, 1,
                AERON_DRIVER_RECEIVER_THREAD_NAME, 2,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, 3)),
            Arguments.of(ThreadingMode.SHARED_NETWORK, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, 0,
                AERON_DRIVER_SHARED_NETWORK_THREAD_NAME, 1,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, 2)),
            Arguments.of(ThreadingMode.SHARED, Map.of(
                AERON_DRIVER_SHARED_THREAD_NAME, 0)));
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @SuppressWarnings("try")
    void shouldPinAgentThreadsToConfiguredCpus(
        final ThreadingMode threadingMode, final Map<String, Integer> expectedCpuIndexByThreadName)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("CPU affinity configuration is for the Java Media Driver only");

        final IntArrayList cpus = ThreadAffinityRecording.effectiveCpus();
        assumeTrue(cpus.size() >= 4, "requires at least 4 CPUs in the effective cpuset");

        try (ThreadAffinityRecording recording = new ThreadAffinityRecording())
        {
            final MediaDriver.Context context = new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .threadingMode(threadingMode)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .conductorCpuAffinity(1)
                .senderCpuAffinity(2)
                .receiverCpuAffinity(3)
                .nativeResourceAgentCpuAffinity(4);

            try (TestMediaDriver ignore = TestMediaDriver.launch(context, null))
            {
                expectedCpuIndexByThreadName.forEach(
                    (threadName, cpuIndex) -> recording.awaitAffinity(threadName, cpus.getInt(cpuIndex)));
            }
        }
    }
}
