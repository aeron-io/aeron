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
package io.aeron.test.driver;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.Strings.isEmpty;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public interface TestMediaDriver extends AutoCloseable
{
    String AERONMD_PATH_PROP_NAME = "aeron.test.system.aeronmd.path";
    String ATS_LIBRARY_PATH_PROP_NAME = "aeron.test.system.ats.path";
    String ATS_LIBRARY_CONF_PATH_PROP_NAME = "aeron.test.system.ats.conf.dir";
    String ATS_LIBRARY_CONF_FILE_PROP_NAME = "aeron.test.system.ats.conf.file";

    static boolean shouldRunCMediaDriver()
    {
        return !isEmpty(System.getProperty(AERONMD_PATH_PROP_NAME));
    }

    static boolean shouldRunJavaMediaDriver()
    {
        return !shouldRunCMediaDriver();
    }

    static void notSupportedOnCMediaDriver(final String reason)
    {
        assumeFalse(shouldRunCMediaDriver(), () -> "not support by C Media Driver: " + reason);
    }

    static TestMediaDriver launch(final MediaDriver.Context context, final DriverOutputConsumer driverOutputConsumer)
    {
        return shouldRunCMediaDriver() ?
            CTestMediaDriver.launch(context, true, driverOutputConsumer) : JavaTestMediaDriver.launch(context);
    }

    static void enableRandomLoss(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.enableRandomLossOnReceive(context, rate, seed, loseDataMessages, loseControlMessages);
        }
        else
        {
            JavaTestMediaDriver.enableRandomLossOnReceive(
                context, rate, seed, loseDataMessages, loseControlMessages);
        }
    }

    static void enableFixedLoss(
        final MediaDriver.Context context,
        final int termId,
        final int termOffset,
        final int length)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.enableFixedLossOnReceive(context, termId, termOffset, length);
        }
        else
        {
            JavaTestMediaDriver.enableFixedLossOnReceive(context, termId, termOffset, length);
        }
    }

    static void enableMultiGapLoss(
        final MediaDriver.Context context,
        final int termId,
        final int gapRadix,
        final int gapLength,
        final int totalGaps)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.enableMultiGapLossOnReceive(context, termId, gapRadix, gapLength, totalGaps);
        }
        else
        {
            JavaTestMediaDriver.enableMultiGapLossOnReceive(context, termId, gapRadix, gapLength, totalGaps);
        }
    }

    static void dontCoalesceNaksOnReceiverByDefault(final MediaDriver.Context context)
    {
        if (shouldRunCMediaDriver())
        {
            CTestMediaDriver.dontCoalesceNaksOnReceiverByDefault(context);
        }
        else
        {
            JavaTestMediaDriver.dontCoalesceNaksOnReceiverByDefault(context);
        }
    }

    MediaDriver.Context context();

    String aeronDirectoryName();

    void close();

    void cleanup();

    AgentInvoker sharedAgentInvoker();

    CountersReader counters();
}
