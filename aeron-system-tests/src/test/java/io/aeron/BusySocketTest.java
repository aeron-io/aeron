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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.RegistrationException;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(InterruptingTestCallback.class)
class BusySocketTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();
    private TestMediaDriver driver1, driver2;

    @BeforeEach
    void setup()
    {
        driver1 = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.SHARED), testWatcher);
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(driver1, driver2);
    }

    @InterruptAfter(20)
    @ParameterizedTest
    @MethodSource("subscriptionTests")
    void subscriptionShouldConnectToASocketOnceItIsFree(
        final ThreadingMode threadingMode, final String pubChannel, final String subChannel)
    {
        driver2 = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(threadingMode), testWatcher);

        final int streamId = 10001;

        try (Aeron aeron1 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron aeron2 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName())))
        {
            final ExclusivePublication publication = aeron2.addExclusivePublication(pubChannel, streamId);
            final Subscription subscription = aeron1.addSubscription(subChannel, streamId);
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            final int channelStatusId = subscription.channelStatusId();

            for (int i = 0; i < 5; i++)
            {
                try
                {
                    if (0 == (i & 1))
                    {
                        aeron2.addSubscription(subChannel, streamId);
                    }
                    else
                    {
                        final long registrationId = aeron2.asyncAddSubscription(subChannel, streamId);
                        while (null == aeron2.getSubscription(registrationId))
                        {
                            Tests.yield();
                        }
                    }
                    fail("Subscription should not be created");
                }
                catch (final RegistrationException ex)
                {
                    assertAddressInUseException(subChannel, ex);
                }
            }

            subscription.close();

            Tests.await(
                () -> CountersReader.RECORD_RECLAIMED == aeron1.countersReader().getCounterState(channelStatusId));

            final Subscription newSubscription = aeron2.addSubscription(subChannel, streamId);
            Tests.awaitConnected(newSubscription);
        }
    }

    @InterruptAfter(20)
    @ParameterizedTest
    @EnumSource(value = ThreadingMode.class, names = "INVOKER", mode = EnumSource.Mode.EXCLUDE)
    void mdsSubscriptionShouldConnectToASocketOnceItIsFree(final ThreadingMode threadingMode)
    {
        driver2 = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(threadingMode), testWatcher);

        final int streamId = 10001;
        final String destination1 = "aeron:udp?endpoint=localhost:8989";
        final String destination2 = "aeron:udp?endpoint=localhost:9898";
        final String mdsChannel = "aeron:udp?control-mode=manual";

        try (Aeron aeron1 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron aeron2 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName())))
        {
            final ExclusivePublication publication1 =
                aeron1.addExclusivePublication(destination1 + "|term-length=64k", streamId);
            final ExclusivePublication publication2 =
                aeron2.addExclusivePublication(destination2 + "|term-length=64k", streamId);
            final Subscription subscription = aeron1.addSubscription(destination1, streamId);
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication1);

            final int channelStatusId = subscription.channelStatusId();

            final Subscription mdsSubscription = aeron2.addSubscription(mdsChannel, streamId);
            mdsSubscription.addDestination(destination2);
            Tests.await(() -> ChannelEndpointStatus.ACTIVE == mdsSubscription.channelStatus());

            for (int i = 0; i < 3; i++)
            {
                try
                {
                    mdsSubscription.addDestination(destination1);
                    fail("Destination should not be added");
                }
                catch (final RegistrationException ex)
                {
                    assertAddressInUseException(destination1, ex);
                    assertEquals(ChannelEndpointStatus.ACTIVE, mdsSubscription.channelStatus());
                }
            }

            subscription.close();

            Tests.await(
                () -> CountersReader.RECORD_RECLAIMED == aeron1.countersReader().getCounterState(channelStatusId));

            mdsSubscription.addDestination(destination1);
            Tests.await(() -> mdsSubscription.imageCount() == 2);
            Tests.awaitConnected(publication2);
            Tests.awaitConnected(publication1);
            assertEquals(ChannelEndpointStatus.ACTIVE, mdsSubscription.channelStatus());
            assertThat(
                mdsSubscription.localSocketAddresses(),
                containsInAnyOrder(containsString(":9898"), containsString(":8989")));
        }
    }

    @InterruptAfter(20)
    @ParameterizedTest
    @EnumSource(value = ThreadingMode.class, names = "INVOKER", mode = EnumSource.Mode.EXCLUDE)
    void mdcPublicationShouldConnectToASocketOnceItIsFree(final ThreadingMode threadingMode)
    {
        driver2 = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(threadingMode), testWatcher);


        try (Aeron aeron1 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron aeron2 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName())))
        {
            final int streamId = 10001;
            final String channel = "aeron:udp?endpoint=localhost:8989";
            final ExclusivePublication publication =
                aeron1.addExclusivePublication(channel + "|term-length=64k", streamId);
            final Subscription subscription = aeron1.addSubscription(channel, streamId);
            Tests.awaitConnected(subscription);
            Tests.awaitConnected(publication);

            final int channelStatusId = subscription.channelStatusId();

            final int mdcStreamId = 20002;
            final String mdcChannel = "aeron:udp?control=localhost:8989|control-mode=dynamic|term-length=64k";
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    aeron2.addExclusivePublication(mdcChannel, mdcStreamId);
                    fail("Publication should not be added");
                }
                catch (final RegistrationException ex)
                {
                    assertAddressInUseException(mdcChannel, ex);
                }
            }

            subscription.close();

            Tests.await(
                () -> CountersReader.RECORD_RECLAIMED == aeron1.countersReader().getCounterState(channelStatusId));

            final ExclusivePublication mdcPublication = aeron2.addExclusivePublication(mdcChannel, mdcStreamId);
            final Subscription mdcSubscription =
                aeron1.addSubscription("aeron:udp?control=localhost:8989", mdcStreamId);
            Tests.awaitConnected(mdcSubscription);
            Tests.awaitConnected(mdcPublication);
        }
    }

    @InterruptAfter(20)
    @Test
    void publicationShouldConnectToASocketOnceItIsFree()
    {
        driver2 = TestMediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .threadingMode(ThreadingMode.DEDICATED), testWatcher);

        final int streamId = 10001;
        final String interfaceEndpoint = "interface=localhost:9090";
        final String channel = "aeron:udp?term-length=64k";

        try (Aeron aeron1 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver1.aeronDirectoryName()));
            Aeron aeron2 = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver2.aeronDirectoryName())))
        {
            final ExclusivePublication publication = aeron1.addExclusivePublication(
                channel + "|endpoint=localhost:8888|" + interfaceEndpoint, streamId);

            final int channelStatusId = publication.channelStatusId();
            Tests.await(
                () -> CountersReader.RECORD_ALLOCATED == aeron1.countersReader().getCounterState(channelStatusId));

            final String conflictingChannel = channel + "|endpoint=localhost:7777|" + interfaceEndpoint;
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    aeron2.addExclusivePublication(conflictingChannel, streamId);
                    fail("Publication should not be added");
                }
                catch (final RegistrationException ex)
                {
                    assertAddressInUseException(conflictingChannel, ex);
                }
            }

            publication.close();

            Tests.await(
                () -> CountersReader.RECORD_RECLAIMED == aeron1.countersReader().getCounterState(channelStatusId));

            final ExclusivePublication newPublication = aeron2.addExclusivePublication(conflictingChannel, streamId);
            Tests.await(() ->
                CountersReader.RECORD_ALLOCATED ==
                aeron2.countersReader().getCounterState(newPublication.channelStatusId()));
        }
    }

    private static List<Arguments> subscriptionTests()
    {
        final ArrayList<Arguments> arguments = new ArrayList<>();
        final String[] pubChannels = {
            "aeron:udp?term-length=64k|endpoint=localhost:8191",
            "aeron:udp?term-length=64k|control=localhost:7779|control-mode=dynamic" };
        final String[] subChannels = {
            "aeron:udp?endpoint=localhost:8191",
            "aeron:udp?endpoint=0.0.0.0:8191|control=localhost:7779" };
        for (final ThreadingMode threadingMode : ThreadingMode.values())
        {
            if (ThreadingMode.INVOKER != threadingMode)
            {
                for (int i = 0; i < pubChannels.length; i++)
                {
                    arguments.add(Arguments.of(threadingMode, pubChannels[i], subChannels[i]));
                }
            }
        }
        return arguments;
    }

    private static void assertAddressInUseException(final String subChannel, final RegistrationException ex)
    {
        assertEquals(ErrorCode.GENERIC_ERROR, ex.errorCode());
        assertThat(ex.getMessage(),
            allOf(containsString(subChannel),
            anyOf(containsString("Address already in use"),
            containsString("Address in use"),
            containsString("failed to bind to address"))));
    }
}
