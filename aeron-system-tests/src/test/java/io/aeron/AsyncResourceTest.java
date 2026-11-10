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
import io.aeron.test.*;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.MutableReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@SuppressWarnings("try")
@ExtendWith(InterruptingTestCallback.class)
class AsyncResourceTest
{
    private static final int STREAM_ID = 7777;
    private static final String AERON_IPC = "aeron:ipc";

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;

    @BeforeEach
    void setUp()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .threadingMode(ThreadingMode.SHARED);

        driver = TestMediaDriver.launch(driverCtx, testWatcher);
        testWatcher.dataCollector().add(driverCtx.aeronDirectory());
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(driver);
    }

    @Test
    @InterruptAfter(10)
    void shouldAddAsyncPublications()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .useConductorAgentInvoker(true)
            .errorHandler(Tests::onError);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationIdOne = aeron.asyncAddPublication(AERON_IPC, STREAM_ID);
            final long registrationIdTwo = aeron.asyncAddExclusivePublication(AERON_IPC, STREAM_ID);

            ExclusivePublication publicationTwo;
            while (null == (publicationTwo = aeron.getExclusivePublication(registrationIdTwo)))
            {
                Tests.yield();
            }

            ConcurrentPublication publicationOne;
            while (null == (publicationOne = aeron.getPublication(registrationIdOne)))
            {
                Tests.yield();
            }

            assertFalse(aeron.isCommandActive(registrationIdOne));
            assertFalse(aeron.isCommandActive(registrationIdTwo));
            assertFalse(aeron.hasActiveCommands());

            assertNotNull(publicationOne);
            assertNotNull(publicationTwo);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAsyncRemovePublication()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            final long registrationId = aeron.asyncAddPublication(AERON_IPC, STREAM_ID);

            Publication publication;
            while (null == (publication = aeron.getPublication(registrationId)))
            {
                Tests.yield();
            }

            assertFalse(aeron.hasActiveCommands());
            assertEquals(registrationId, publication.registrationId());

            aeron.asyncRemovePublication(registrationId);
            assertTrue(publication.isClosed());
            assertNull(aeron.getPublication(registrationId));
        }
    }

    @ParameterizedTest(name = "{0}")
    @InterruptAfter(10)
    @MethodSource("resourcesAddAndGet")
    void shouldDetectInvalidUri(final String name, final Resource resource)
    {
        final MutableReference<Throwable> mockClientErrorHandler = new MutableReference<>();
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler::set);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            testWatcher.ignoreErrorsMatching(
                (s) -> s.contains("Aeron URIs must start with") || s.contains("invalid channel"));

            final long registrationId = resource.add(aeron, "invalid" + AERON_IPC, STREAM_ID);

            try
            {
                while (null == resource.get(aeron, registrationId))
                {
                    Tests.yield();
                }

                fail("RegistrationException not thrown");
            }
            catch (final RegistrationException ignore)
            {
                // Expected
            }

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
        }
    }

    @Test
    @SlowTest
    @InterruptAfter(60)
    void shouldDetectUnknownHost()
    {
        final ErrorHandler mockClientErrorHandler = mock(ErrorHandler.class);
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(mockClientErrorHandler);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            testWatcher.ignoreErrorsMatching(
                (s) -> s.contains("unresolved") || s.contains("unknown host"));

            final long registrationId = aeron.asyncAddPublication("aeron:udp?endpoint=wibble:1234", STREAM_ID);

            try
            {
                while (null == aeron.getPublication(registrationId))
                {
                    Tests.yield();
                }

                fail("RegistrationException not thrown");
            }
            catch (final RegistrationException ignore)
            {
                // Expected
            }

            assertFalse(aeron.isCommandActive(registrationId));
            assertFalse(aeron.hasActiveCommands());
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAddAsyncSubscriptions()
    {
        final Aeron.Context clientCtx = new Aeron.Context()
            .errorHandler(Tests::onError);

        try (Aeron aeron = Aeron.connect(clientCtx))
        {
            final AvailableImageHandler availableImageHandler = image -> {};
            final UnavailableImageHandler unavailableImageHandler = image -> {};

            final long registrationIdOne = aeron.asyncAddSubscription(
                AERON_IPC, STREAM_ID, availableImageHandler, unavailableImageHandler);
            final long registrationIdTwo = aeron.asyncAddSubscription(AERON_IPC, STREAM_ID);

            Subscription subscriptionOne;
            while (null == (subscriptionOne = aeron.getSubscription(registrationIdOne)))
            {
                Tests.yield();
            }

            Subscription subscriptionTwo;
            while (null == (subscriptionTwo = aeron.getSubscription(registrationIdTwo)))
            {
                Tests.yield();
            }

            assertFalse(aeron.isCommandActive(registrationIdOne));
            assertFalse(aeron.isCommandActive(registrationIdTwo));
            assertFalse(aeron.hasActiveCommands());

            assertNotNull(subscriptionOne);
            assertNotNull(subscriptionTwo);
        }
    }

    interface Resource
    {
        long add(Aeron aeron, String channel, int streamId);

        Object get(Aeron aeron, long registrationId);
    }

    private static Stream<Arguments> resourcesAddAndGet()
    {
        return Stream.of(
            Arguments.of(
                "asyncAddPublication",
                new Resource()
                {
                    @Override
                    public long add(final Aeron aeron, final String channel, final int streamId)
                    {
                        return aeron.asyncAddPublication(channel, streamId);
                    }

                    @Override
                    public Object get(final Aeron aeron, final long registrationId)
                    {
                        return aeron.getPublication(registrationId);
                    }
                }),
            Arguments.of(
                "asyncAddExclusivePublication",
                new Resource()
                {
                    @Override
                    public long add(final Aeron aeron, final String channel, final int streamId)
                    {
                        return aeron.asyncAddExclusivePublication(channel, streamId);
                    }

                    @Override
                    public Object get(final Aeron aeron, final long registrationId)
                    {
                        return aeron.getExclusivePublication(registrationId);
                    }
                }),
            Arguments.of(
                "asyncAddSubscription",
                new Resource()
                {
                    @Override
                    public long add(final Aeron aeron, final String channel, final int streamId)
                    {
                        return aeron.asyncAddSubscription(channel, streamId);
                    }

                    @Override
                    public Object get(final Aeron aeron, final long registrationId)
                    {
                        return aeron.getSubscription(registrationId);
                    }
                }));
    }
}
