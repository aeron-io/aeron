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
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.ErrorCode;
import io.aeron.Subscription;
import io.aeron.exceptions.RegistrationException;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class SocketLifecycleTest
{
    private static final int TEST_ITERATION_COUNT = 10;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(10)
    void supportsClosingOpeningSubscriptionWithSameChannelUri0()
    {
        try (TestMediaDriver driver = launchDriver();
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final CountersReader countersReader = aeron.countersReader();

            for (int i = 0; i < TEST_ITERATION_COUNT; i++)
            {
                final Subscription subscription = aeron.addSubscription(
                    "aeron:udp?endpoint=localhost:10000", 1000);
                final int counterId = subscription.channelStatusId();
                final long registrationId = countersReader.getCounterRegistrationId(counterId);
                assertEquals(subscription.registrationId(), registrationId);
                subscription.close();
                while (registrationId == countersReader.getCounterRegistrationId(counterId) &&
                    CountersReader.RECORD_ALLOCATED == countersReader.getCounterState(counterId))
                {
                    Tests.yield();
                }
            }

            assertEquals(0, errorCount(aeron));
        }
    }

    @Test
    @InterruptAfter(10)
    void supportsClosingOpeningSubscriptionWithSameChannelUri1()
    {
        int unavailableCount = 0;
        try (TestMediaDriver driver = launchDriver();
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            for (int i = 0; i < TEST_ITERATION_COUNT; i++)
            {
                Subscription subscription = null;
                while (null == subscription)
                {
                    try
                    {
                        subscription = aeron.addSubscription("aeron:udp?endpoint=localhost:10000", 1000);
                    }
                    catch (final RegistrationException exception)
                    {
                        if (ErrorCode.RESOURCE_TEMPORARILY_UNAVAILABLE == exception.errorCode())
                        {
                            ++unavailableCount;
                            Tests.yield();
                        }
                        else
                        {
                            throw exception;
                        }
                    }
                }
                subscription.close();
            }

            assumeTrue(unavailableCount > 0, "Expected at least one RESOURCE_TEMPORARILY_UNAVAILABLE exception");
            assertEquals(0, errorCount(aeron));
        }
    }

    private TestMediaDriver launchDriver()
    {
        TestMediaDriver.notSupportedOnCMediaDriver("C Media Driver requires more work");

        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(ThreadingMode.DEDICATED)
            .dirDeleteOnStart(true)
            .conductorIdleStrategy(new NoOpIdleStrategy())
            .receiverIdleStrategy(new SleepingMillisIdleStrategy(2))
            .senderIdleStrategy(new SleepingMillisIdleStrategy(2));

        final TestMediaDriver driver = TestMediaDriver.launch(driverCtx, systemTestWatcher);

        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        return driver;
    }

    private static long errorCount(final Aeron aeron)
    {
        final CountersReader countersReader = aeron.countersReader();
        final int counterId = countersReader.findByTypeIdAndRegistrationId(
            AeronCounters.DRIVER_SYSTEM_COUNTER_TYPE_ID,
            AeronCounters.SYSTEM_COUNTER_ID_ERRORS);
        return countersReader.getCounterValue(counterId);
    }
}
