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

package io.aeron.test;

import io.aeron.topology.CpusetV2Reader;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingStream;
import org.agrona.collections.IntArrayList;
import org.agrona.concurrent.affinity.ThreadAffinity;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import static io.aeron.CommonContext.THREAD_NAMING_NEW;
import static io.aeron.CommonContext.THREAD_NAMING_PROP_NAME;
import static io.aeron.test.TestPropertiesUtil.backupAndOverrideSystemProperties;
import static io.aeron.test.TestPropertiesUtil.restoreSystemProperties;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * Captures the OS thread ids of started threads via JFR so their CPU affinity can be checked. Enables the "new"
 * thread naming so Java thread names match the registry keys used by each component.
 */
public final class ThreadAffinityRecording implements AutoCloseable
{
    private final Map<String, Long> osThreadIdByName = new ConcurrentHashMap<>();
    private final RecordingStream recording = new RecordingStream();
    private final Properties backup;

    public ThreadAffinityRecording()
    {
        final Properties overrides = new Properties();
        overrides.setProperty(THREAD_NAMING_PROP_NAME, THREAD_NAMING_NEW);
        backup = backupAndOverrideSystemProperties(new Properties(), overrides);

        // Must be started before the components so the agent thread start events are captured.
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
    }

    /**
     * The CPUs of the effective cgroup cpuset, which cpuset affinity indices refer to.
     *
     * @return the effective cpuset CPUs in ascending order.
     */
    public static IntArrayList effectiveCpus()
    {
        return new CpusetV2Reader().readCpuSet().cpus();
    }

    public void awaitAffinity(final String threadName, final int expectedCpu)
    {
        Tests.await(() -> osThreadIdByName.containsKey(threadName));
        final int tid = Math.toIntExact(osThreadIdByName.get(threadName));

        Tests.await(() -> Arrays.equals(new int[]{ expectedCpu }, ThreadAffinity.getAffinity(tid)));
        assertArrayEquals(new int[]{ expectedCpu }, ThreadAffinity.getAffinity(tid), threadName);
    }

    public void close()
    {
        try
        {
            recording.close();
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }
}
