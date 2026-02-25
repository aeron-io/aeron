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
import io.aeron.Publication;
import io.aeron.Subscription;
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
import java.util.Random;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
class SocketLifecycleTest
{
    private static final int EXAMPLE_TEST_ITERATION_COUNT = 100;
    private static final int PROPERTY_TEST_ITERATION_COUNT = 1000;
    private static final String PUBLISHER_MDC_URI = "aeron:udp?control-mode=dynamic|control=localhost:5001";
    private static final String PUBLISHER_UNICAST_URI = "aeron:udp?control=localhost:5000|endpoint=localhost:10000";
    private static final String SUBSCRIBER_UNICAST_URI = "aeron:udp?endpoint=localhost:10000";
    private static final String IPC_URI = "aeron:ipc";
    private static final List<String> PUBLISHER_URIS = List.of(
        PUBLISHER_UNICAST_URI,
        PUBLISHER_MDC_URI,
        IPC_URI
    );
    private static final List<String> SUBSCRIBER_URIS = List.of(
        SUBSCRIBER_UNICAST_URI,
        IPC_URI
    );
    private static final int STREAM_ID = 1000;

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    @ParameterizedTest
    @FieldSource("SUBSCRIBER_URIS")
    void supportsClosingAndImmediatelyOpeningSubscriptionWithSameChannel(final String channelUri)
    {
        try (TestMediaDriver driver = launchDriver(ThreadingMode.DEDICATED);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {
            for (int i = 0; i < EXAMPLE_TEST_ITERATION_COUNT; i++)
            {
                final Subscription subscription = aeron.addSubscription(channelUri, STREAM_ID);
                assertThat(
                    subscription.channelStatus(),
                    oneOf(ChannelEndpointStatus.INITIALIZING, ChannelEndpointStatus.ACTIVE)
                );
                subscription.close();
            }

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
            for (int i = 0; i < EXAMPLE_TEST_ITERATION_COUNT; i++)
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
            for (int i = 0; i < EXAMPLE_TEST_ITERATION_COUNT; i++)
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
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        )
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

    @ParameterizedTest
    @EnumSource(value = ThreadingMode.class, mode = EnumSource.Mode.EXCLUDE, names = "INVOKER")
    void randomlyAttemptToReuseSockets(final ThreadingMode threadingMode)
    {
        try (TestMediaDriver driver = launchDriver(threadingMode);
            Aeron aeron1 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron aeron2 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron aeron3 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron aeron4 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Aeron aeron5 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName())))
        {

            final AeronClientState[] state = {
                new AeronClientState(aeron1),
                new AeronClientState(aeron2),
                new AeronClientState(aeron3),
                new AeronClientState(aeron4),
                new AeronClientState(aeron5)
            };

            final Random random = new Random();

            for (int i = 0; i < PROPERTY_TEST_ITERATION_COUNT; i++)
            {
                state[i % state.length].performOperation(random);
            }
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
            .receiverIdleStrategy(new SleepingMillisIdleStrategy(1))
            .senderIdleStrategy(new SleepingMillisIdleStrategy(1));

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

    private static final class AeronClientState
    {
        private final Aeron aeron;
        private Subscription subscription;
        private Publication publication;

        AeronClientState(final Aeron aeron)
        {
            this.aeron = aeron;
        }

        public void performOperation(final Random random)
        {
            final boolean isPublicationOp = random.nextBoolean();
            if (isPublicationOp)
            {
                if (null != publication)
                {
                    publication.close();
                    publication = null;
                }
                else
                {
                    final String uri = PUBLISHER_URIS.get(random.nextInt(PUBLISHER_URIS.size()));
                    final boolean isExclusive = random.nextBoolean();
                    if (isExclusive)
                    {
                        publication = aeron.addExclusivePublication(uri, STREAM_ID);
                    }
                    else
                    {
                        publication = aeron.addPublication(uri, STREAM_ID);
                    }
                }
            }
            else
            {
                if (null != subscription)
                {
                    subscription.close();
                    subscription = null;
                }
                else
                {
                    subscription = aeron.addSubscription(SUBSCRIBER_UNICAST_URI, STREAM_ID);
                }
            }
        }
    }
}
