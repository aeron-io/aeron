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
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.exceptions.RegistrationException;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.FieldSource;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class SocketLifecycleTest
{
    private static final int TEST_ITERATION_COUNT = 10;
    private static final String PUBLISHER_MDC_URI = "aeron:udp?control-mode=dynamic|control=localhost:5001";
    private static final String PUBLISHER_UNICAST_URI = "aeron:udp?control=localhost:5000|endpoint=localhost:10000";
    private static final String IPC_URI = "aeron:ipc";
    private static final List<String> PUBLISHER_URIS = List.of(
        PUBLISHER_UNICAST_URI,
        PUBLISHER_MDC_URI,
        IPC_URI
    );
    private static final int STREAM_ID = 1000;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @ParameterizedTest
    @EnumSource(value = ThreadingMode.class, mode = EnumSource.Mode.EXCLUDE, names = "INVOKER")
    @InterruptAfter(10)
    void supportsClosingOpeningSubscriptionWithSameChannelUri0(final ThreadingMode threadingMode)
    {
        try (TestMediaDriver driver = launchDriver(threadingMode);
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

    @ParameterizedTest
    @EnumSource(value = ThreadingMode.class, mode = EnumSource.Mode.EXCLUDE, names = "INVOKER")
    @InterruptAfter(10)
    void supportsClosingOpeningSubscriptionWithSameChannelUri1(final ThreadingMode threadingMode)
    {
        int unavailableCount = 0;
        try (TestMediaDriver driver = launchDriver(threadingMode);
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

    @ParameterizedTest
    @FieldSource("PUBLISHER_URIS")
    @InterruptAfter(10)
    void supportsClosingAndImmediatelyOpeningPublicationWithSameChannel(final String channelUri)
    {
        try (TestMediaDriver driver = launchDriver(ThreadingMode.DEDICATED);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            for (int i = 0; i < TEST_ITERATION_COUNT; i++)
            {
                final Publication publication = aeron.addPublication(channelUri, STREAM_ID);
                assertThat(
                    publication.channelStatus(),
                    oneOf(ChannelEndpointStatus.INITIALIZING, ChannelEndpointStatus.ACTIVE)
                );
                publication.close();
            }

            assertEquals(0, errorCount(aeron));
        }
    }

    @ParameterizedTest
    @FieldSource("PUBLISHER_URIS")
    void supportsClosingAndImmediatelyOpeningExclusivePublicationWithSameChannel(final String channelUri)
    {
        try (TestMediaDriver driver = launchDriver(ThreadingMode.DEDICATED);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            for (int i = 0; i < TEST_ITERATION_COUNT; i++)
            {
                final long registrationId = aeron.asyncAddExclusivePublication(channelUri, STREAM_ID);
                aeron.asyncRemovePublication(registrationId);
            }

            assertEquals(0, errorCount(aeron));
        }
    }

    @Test
    @InterruptAfter(10)
    void noInterferenceBetweenMdsSubscriptionsSharingSameMulticastDestination()
    {
        try (TestMediaDriver driver = launchDriver(ThreadingMode.DEDICATED);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            final Subscription subscription1 = aeron.addSubscription("aeron:udp?control-mode=manual", STREAM_ID);
            final Subscription subscription2 = aeron.addSubscription("aeron:udp?control-mode=manual", STREAM_ID);

            final String multicastChannel = "aeron:udp?endpoint=239.192.12.87:20123|interface=127.0.0.1";
            subscription1.asyncAddDestination(multicastChannel);
            subscription2.asyncAddDestination(multicastChannel);

            final CountersReader countersReader = aeron.countersReader();

            int counterId1 = CountersReader.NULL_COUNTER_ID;
            int counterId2 = CountersReader.NULL_COUNTER_ID;

            while (counterId1 == CountersReader.NULL_COUNTER_ID || counterId2 == CountersReader.NULL_COUNTER_ID)
            {
                counterId1 = countersReader.findByTypeIdAndRegistrationId(
                    LocalSocketAddressStatus.LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID,
                    subscription1.registrationId()
                );

                counterId2 = countersReader.findByTypeIdAndRegistrationId(
                    LocalSocketAddressStatus.LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID,
                    subscription2.registrationId()
                );

                Tests.yield();
            }

            subscription1.removeDestination(multicastChannel);

            final int finalCounterId1 = counterId1;
            Tests.await(() -> countersReader.getCounterState(finalCounterId1) == CountersReader.RECORD_RECLAIMED);
            assertEquals(ChannelEndpointStatus.ACTIVE, countersReader.getCounterValue(counterId2));
        }
    }

    private TestMediaDriver launchDriver(final ThreadingMode threadingMode)
    {
        TestMediaDriver.notSupportedOnCMediaDriver("C Media Driver requires more work");

        final String aeronDirectoryName = CommonContext.generateRandomDirName();

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirectoryName)
            .termBufferSparseFile(true)
            .threadingMode(threadingMode)
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
