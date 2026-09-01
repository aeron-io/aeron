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

package io.aeron.topology;

import io.aeron.exceptions.AeronException;
import org.agrona.SystemUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;

import static java.lang.management.ManagementFactory.getThreadMXBean;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ThreadAffinityTest
{
    @Test
    void namingTest() throws InterruptedException
    {
//        Thread a = new Thread(() -> ThreadAffinity.setThreadName("test"));
//        a.start();
//        final ThreadMXBean threadBean = getThreadMXBean();
//        final long[] threadIds = threadBean.getAllThreadIds();
//        final ThreadInfo[] threadInfos = threadBean.getThreadInfo(threadIds, 0);
//        assertNotEquals(0, Arrays.stream(threadInfos)
//            .filter(threadInfo -> "test".equals(threadInfo.getThreadName())).count());
//        a.join();

    }
    @Test
    void setAffinityPinsCallingThreadOnLinux()
    {
        assumeTrue(SystemUtil.isLinux());
        assertDoesNotThrow(() -> ThreadAffinity.setAffinity(5));
        assertEquals(5, ThreadAffinity.getAffinity());
    }

}
