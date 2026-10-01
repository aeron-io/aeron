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
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SlowTest;
import io.aeron.test.ThreadAffinityRecording;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.collections.IntArrayList;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
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
@ExtendWith(InterruptingTestCallback.class)
@SlowTest
class MediaDriverThreadAffinityTest
{
    private static final int CONDUCTOR_INDEX = 1;
    private static final int SENDER_INDEX = 2;
    private static final int RECEIVER_INDEX = 3;
    private static final int NATIVE_RESOURCE_AGENT_INDEX = 4;

    static Stream<Arguments> threadingModes()
    {
        return Stream.of(
            Arguments.of(ThreadingMode.DEDICATED, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, CONDUCTOR_INDEX,
                AERON_DRIVER_SENDER_THREAD_NAME, SENDER_INDEX,
                AERON_DRIVER_RECEIVER_THREAD_NAME, RECEIVER_INDEX,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, NATIVE_RESOURCE_AGENT_INDEX)),
            Arguments.of(ThreadingMode.SHARED_NETWORK, Map.of(
                AERON_DRIVER_CONDUCTOR_THREAD_NAME, CONDUCTOR_INDEX,
                AERON_DRIVER_SHARED_NETWORK_THREAD_NAME, SENDER_INDEX,
                AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME, NATIVE_RESOURCE_AGENT_INDEX)),
            Arguments.of(ThreadingMode.SHARED, Map.of(
                AERON_DRIVER_SHARED_THREAD_NAME, CONDUCTOR_INDEX)));
    }

    @ParameterizedTest
    @MethodSource("threadingModes")
    @SuppressWarnings("try")
    @InterruptAfter(10)
    void shouldPinAgentThreadsToConfiguredCpus(
        final ThreadingMode threadingMode, final Map<String, Integer> expectedCpuIndexByThreadName)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("CPU affinity configuration is for the Java Media Driver only");

        final IntArrayList cpus = ThreadAffinityRecording.effectiveCpus();
        assumeTrue(cpus.size() > NATIVE_RESOURCE_AGENT_INDEX, "requires at least 5 CPUs in the effective cpuset");

        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(threadingMode)
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .conductorCpuAffinity(cpus.getInt(CONDUCTOR_INDEX))
            .senderCpuAffinity(cpus.getInt(SENDER_INDEX))
            .receiverCpuAffinity(cpus.getInt(RECEIVER_INDEX))
            .nativeResourceAgentCpuAffinity(cpus.getInt(NATIVE_RESOURCE_AGENT_INDEX));

        try (ThreadAffinityRecording recording = new ThreadAffinityRecording();
            TestMediaDriver ignore = TestMediaDriver.launch(context, null))
        {
            expectedCpuIndexByThreadName.forEach(
                (threadName, cpuIndex) -> recording.awaitAffinity(threadName, cpus.getInt(cpuIndex)));
        }
    }
}
