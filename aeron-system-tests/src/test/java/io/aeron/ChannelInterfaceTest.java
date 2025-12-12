/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
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
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.MutableDirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.concurrent.ThreadLocalRandom;

import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

@ExtendWith(InterruptingTestCallback.class)
class ChannelInterfaceTest
{
    private static final String LOOPBACK_INTERFACE = findLoopbackInterface();
    private static final int STREAM_ID = 1001;

    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private Aeron publishingClient;
    private Aeron subscribingClient;
    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverContext = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED)
            .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH);

        driver = TestMediaDriver.launch(driverContext, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());

        subscribingClient = Aeron.connect();
        publishingClient = Aeron.connect();
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.closeAll(publishingClient, subscribingClient, driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldAcceptInterfaceNameInUnicastChannels()
    {
        assumeNotNativeDriverOnWindows();

        final String subChannel = "aeron:udp?endpoint=127.0.0.1:24325";
        final String pubChannel = subChannel + "|interface={" + LOOPBACK_INTERFACE + "}:24324";

        final Subscription subscription = subscribingClient.addSubscription(subChannel, STREAM_ID);
        final Publication publication = publishingClient.addExclusivePublication(pubChannel, STREAM_ID);

        passMessage(subscription, publication);

        final String pubSocketAddress = publication.localSocketAddresses().get(0);
        assertEquals("127.0.0.1:24324", pubSocketAddress);
    }

    @Test
    @InterruptAfter(10)
    void shouldAcceptInterfaceNameInMulticastChannels()
    {
        assumeNotNativeDriverOnWindows();

        final String channel = "aeron:udp?endpoint=224.20.30.39:24326|interface={" + LOOPBACK_INTERFACE + "}|alias=foo";

        final Subscription subscription = subscribingClient.addSubscription(channel, STREAM_ID);
        final Publication publication = publishingClient.addExclusivePublication(channel, STREAM_ID);

        passMessage(subscription, publication);
    }

    @ParameterizedTest
    @InterruptAfter(10)
    @ValueSource(strings = {
        "aeron:udp?endpoint=localhost:24325|interface={does_not_exist}:24326",
        "aeron:udp?endpoint=224.20.30.39:24325|interface={does_not_exist}",
    })
    void shouldThrowIfSpecifiedInterfaceDoesNotExist(final String channel)
    {
        final String expectedError = "unknown interface does_not_exist";
        watcher.ignoreErrorsMatching((s) -> s.contains(expectedError));

        final RegistrationException ex = assertThrows(RegistrationException.class,
            () -> publishingClient.addPublication(channel, STREAM_ID));

        assertThat(ex.getMessage(), containsString(expectedError));
    }

    private void passMessage(final Subscription subscription, final Publication publication)
    {
        final long payload = ThreadLocalRandom.current().nextLong();

        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[SIZE_OF_LONG]);
        buffer.putLong(0, payload);

        while (0 > publication.offer(buffer))
        {
            Tests.yield();
        }

        final FragmentHandler fragmentHandler = (buf, off, len, hdr) ->
            assertEquals(payload, buf.getLong(off));
        while (0 == subscription.poll(fragmentHandler, 1))
        {
            Tests.yield();
        }
    }

    private static String findLoopbackInterface()
    {
        try
        {
            final InetAddress home = InetAddress.getByName("127.0.0.1");
            final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements())
            {
                final NetworkInterface networkInterface = networkInterfaces.nextElement();
                if (networkInterface.isLoopback() && networkInterface.inetAddresses().anyMatch(ia -> ia.equals(home)))
                {
                    return networkInterface.getName();
                }
            }
        }
        catch (final UnknownHostException | SocketException e)
        {
            throw new RuntimeException("failed to find loopback interface", e);
        }

        throw new RuntimeException("failed to find loopback interface");
    }

    private static void assumeNotNativeDriverOnWindows()
    {
        assumeFalse(SystemUtil.isWindows() && TestMediaDriver.shouldRunCMediaDriver(),
            "we don't have access to the interface names used by the native MD on Windows");
    }
}
