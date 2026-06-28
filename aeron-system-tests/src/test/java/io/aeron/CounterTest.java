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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ClientTimeoutException;
import io.aeron.exceptions.ConductorServiceTimeoutException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.status.ReadableCounter;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.aeron.Aeron.NULL_VALUE;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@ExtendWith(InterruptingTestCallback.class)
class CounterTest
{
    private static final int COUNTER_TYPE_ID = 1101;
    private static final String COUNTER_LABEL = "counter label";

    private final UnsafeBuffer keyBuffer = new UnsafeBuffer(new byte[64]);
    private final UnsafeBuffer labelBuffer = new UnsafeBuffer(new byte[COUNTER_LABEL.length()]);

    private Aeron clientA;
    private Aeron clientB;
    private TestMediaDriver driver;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private volatile ReadableCounter readableCounter;

    @BeforeEach
    void before()
    {
        labelBuffer.putStringWithoutLengthAscii(0, COUNTER_LABEL);

        driver = TestMediaDriver.launch(
            new MediaDriver.Context()
                .aeronDirectoryName(CommonContext.generateRandomDirName())
                .errorHandler(Tests::onError)
                .threadingMode(ThreadingMode.SHARED)
                .clientLivenessTimeoutNs(TimeUnit.SECONDS.toNanos(1))
                .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(500)),
            testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        clientA = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
        clientB = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(clientA, clientB, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldBeAbleToAddCounter()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        assertFalse(counter.isClosed());
        assertEquals(counter.registrationId(), clientA.countersReader().getCounterRegistrationId(counter.id()));
        assertEquals(clientA.clientId(), clientA.countersReader().getCounterOwnerId(counter.id()));

        verify(availableCounterHandlerClientA, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
        verify(availableCounterHandlerClientB, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldBeAbleToAddReadableCounterWithinHandler()
    {
        clientB.addAvailableCounterHandler(this::createReadableCounter);

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        while (null == readableCounter)
        {
            Tests.sleep(1);
        }

        assertEquals(RECORD_ALLOCATED, readableCounter.state());
        assertEquals(counter.id(), readableCounter.counterId());
        assertEquals(counter.registrationId(), readableCounter.registrationId());
    }

    @Test
    @InterruptAfter(10)
    void shouldCloseReadableCounterOnUnavailableCounter()
    {
        clientB.addAvailableCounterHandler(this::createReadableCounter);
        clientB.addUnavailableCounterHandler(this::unavailableCounterHandler);

        final Counter counter = clientA.addCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        while (null == readableCounter)
        {
            Tests.sleep(1);
        }

        assertFalse(readableCounter.isClosed());
        assertEquals(RECORD_ALLOCATED, readableCounter.state());

        counter.close();

        while (!readableCounter.isClosed())
        {
            Tests.sleep(1);
        }

        while (clientA.hasActiveCommands())
        {
            Tests.sleep(1);
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldGetUnavailableCounterWhenOwningClientIsClosed()
    {
        clientB.addAvailableCounterHandler(this::createReadableCounter);
        clientB.addUnavailableCounterHandler(this::unavailableCounterHandler);

        clientA.addCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        while (null == readableCounter)
        {
            Tests.sleep(1);
        }

        assertFalse(readableCounter.isClosed());
        assertEquals(RECORD_ALLOCATED, readableCounter.state());

        clientA.close();

        while (!readableCounter.isClosed())
        {
            Tests.sleep(1, "Counter not closed");
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldBeAbleToAddStaticCounter()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientA = mock(UnavailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);
        clientA.addUnavailableCounterHandler(unavailableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientB = mock(UnavailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);
        clientB.addUnavailableCounterHandler(unavailableCounterHandlerClientB);

        final Counter counter1 = clientA.addStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            100);

        assertFalse(counter1.isClosed());
        assertThat(counter1.correlationId(), Matchers.greaterThan(0L));
        assertEquals(100, counter1.registrationId());
        assertNotEquals(counter1.correlationId(), counter1.registrationId());
        assertEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(counter1.id()));
        assertEquals(counter1.registrationId(), clientA.countersReader().getCounterRegistrationId(counter1.id()));
        assertEquals(NULL_VALUE, clientA.countersReader().getCounterOwnerId(counter1.id()));
        assertEquals(COUNTER_TYPE_ID, clientA.countersReader().getCounterTypeId(counter1.id()));

        final Counter counter2 = clientB.addStaticCounter(COUNTER_TYPE_ID, "test static counter", 200);

        assertFalse(counter2.isClosed());
        assertThat(counter2.correlationId(), Matchers.greaterThan(counter1.correlationId()));
        assertEquals(200, counter2.registrationId());
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter2.id()));
        assertEquals(counter2.registrationId(), clientB.countersReader().getCounterRegistrationId(counter2.id()));
        assertEquals(NULL_VALUE, clientB.countersReader().getCounterOwnerId(counter2.id()));
        assertEquals("test static counter", clientB.countersReader().getCounterLabel(counter2.id()));
        assertEquals(COUNTER_TYPE_ID, clientB.countersReader().getCounterTypeId(counter2.id()));

        verify(availableCounterHandlerClientA, Mockito.after(1000L).never())
            .onAvailableCounter(any(CountersReader.class), eq(counter1.registrationId()), eq(counter1.id()));
        verify(availableCounterHandlerClientA, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));
        verify(availableCounterHandlerClientB, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter1.registrationId()), eq(counter1.id()));
        verify(availableCounterHandlerClientB, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldReturnExistingStaticCounterAndNotUpdateAnything()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientA = mock(UnavailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);
        clientA.addUnavailableCounterHandler(unavailableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientB = mock(UnavailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);
        clientB.addUnavailableCounterHandler(unavailableCounterHandlerClientB);

        final long registrationId = 888;
        ThreadLocalRandom.current().nextBytes(keyBuffer.byteArray());
        final byte[] expectedKeyBytes = Arrays.copyOf(keyBuffer.byteArray(), keyBuffer.capacity());
        final Counter counter1 = clientA.addStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            registrationId);

        assertFalse(counter1.isClosed());
        assertEquals(registrationId, counter1.registrationId());
        assertEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(counter1.id()));
        assertEquals(counter1.registrationId(), clientA.countersReader().getCounterRegistrationId(counter1.id()));
        assertEquals(NULL_VALUE, clientA.countersReader().getCounterOwnerId(counter1.id()));
        assertEquals(COUNTER_TYPE_ID, clientA.countersReader().getCounterTypeId(counter1.id()));
        assertEquals(COUNTER_LABEL, clientA.countersReader().getCounterLabel(counter1.id()));

        final Counter counter2 = clientB.addStaticCounter(COUNTER_TYPE_ID, "test static counter", registrationId);

        assertNotSame(counter1, counter2);
        assertEquals(counter1.id(), counter2.id());
        assertThat(counter2.correlationId(), Matchers.greaterThan(counter1.correlationId()));
        assertEquals(registrationId, counter2.registrationId());
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter2.id()));
        assertEquals(registrationId, clientB.countersReader().getCounterRegistrationId(counter2.id()));
        assertEquals(NULL_VALUE, clientB.countersReader().getCounterOwnerId(counter2.id()));
        assertEquals(COUNTER_TYPE_ID, clientB.countersReader().getCounterTypeId(counter2.id()));
        assertEquals(COUNTER_LABEL, clientB.countersReader().getCounterLabel(counter2.id()));

        final MutableBoolean keyChecked = new MutableBoolean(false);
        clientB.countersReader().forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (counterId == counter1.id())
            {
                final byte[] actualKeyBytes = new byte[expectedKeyBytes.length];
                keyBuffer.getBytes(0, actualKeyBytes, 0, expectedKeyBytes.length);
                assertArrayEquals(expectedKeyBytes, actualKeyBytes);
                keyChecked.set(true);
            }
        });
        assertTrue(keyChecked.get());

        verify(availableCounterHandlerClientA, Mockito.after(1000L).never())
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter1.id()));
        verify(availableCounterHandlerClientB, never())
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter2.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotDeleteStaticCounterIfClosed()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientA = mock(UnavailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);
        clientA.addUnavailableCounterHandler(unavailableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientB = mock(UnavailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);
        clientB.addUnavailableCounterHandler(unavailableCounterHandlerClientB);

        final Counter counter1 = clientA.addStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            100);

        assertFalse(counter1.isClosed());
        assertEquals(100, counter1.registrationId());
        assertEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(counter1.id()));
        assertEquals(counter1.registrationId(), clientA.countersReader().getCounterRegistrationId(counter1.id()));
        assertEquals(NULL_VALUE, clientA.countersReader().getCounterOwnerId(counter1.id()));
        assertEquals(COUNTER_TYPE_ID, clientB.countersReader().getCounterTypeId(counter1.id()));

        counter1.close();
        assertTrue(counter1.isClosed());

        verify(unavailableCounterHandlerClientA, Mockito.after(1000L).never())
            .onUnavailableCounter(any(CountersReader.class), eq(counter1.registrationId()), eq(counter1.id()));

        assertEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(counter1.id()));
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter1.id()));
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("indentation")
    void shouldReturnErrorIfANonStaticCounterExistsForTypeIdRegistrationId()
    {
        testWatcher.ignoreErrorsMatching((error) -> error.contains("(-11) generic error, see message"));

        final Counter counter = clientA.addCounter(COUNTER_TYPE_ID, "test session-specific counter");
        assertNotEquals(NULL_VALUE, counter.registrationId());
        assertEquals(clientA.clientId(), clientA.countersReader().getCounterOwnerId(counter.id()));

        final RegistrationException registrationException = assertThrowsExactly(
            RegistrationException.class,
            () -> clientA.addStaticCounter(
                COUNTER_TYPE_ID,
                keyBuffer,
                0,
                keyBuffer.capacity(),
                labelBuffer,
                0,
                COUNTER_LABEL.length(),
                counter.registrationId()));

        assertThat(
            registrationException.getMessage(),
            allOf(
                containsString("cannot add static counter, because a non-static counter exists"),
                containsString("counterId=" + counter.id()),
                containsString("typeId=" + COUNTER_TYPE_ID),
                containsString("registrationId=" + counter.registrationId()),
                containsString("errorCodeValue=11")));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotCloseStaticCounterWhenClientInstanceIsClosed()
    {
        final AtomicBoolean clientClosed = new AtomicBoolean();
        clientA.addCloseHandler(() -> clientClosed.set(true));

        final Counter counter1 = clientA.addStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            1);

        final Counter counter2 = clientB.addStaticCounter(COUNTER_TYPE_ID + 2, "test static counter", 22);

        final Counter counter3 = clientA.addCounter(COUNTER_TYPE_ID, "delete me");

        clientA.close();

        Tests.await(clientClosed::get);
        assertFalse(counter1.isClosed());
        assertTrue(counter3.isClosed());

        Tests.await(() -> CountersReader.RECORD_RECLAIMED == clientB.countersReader().getCounterState(counter3.id()));
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter1.id()));
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter2.id()));
        assertFalse(counter2.isClosed());
    }

    @Test
    @InterruptAfter(10)
    void shouldNotCloseStaticCounterIfClientTimesOut()
    {
        final AvailableCounterHandler availableCounterHandler = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandler = mock(UnavailableCounterHandler.class);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(driver.aeronDirectoryName())
            .useConductorAgentInvoker(true)
            .availableCounterHandler(availableCounterHandler)
            .unavailableCounterHandler(unavailableCounterHandler)
            .errorHandler(error::set)))
        {
            final AgentInvoker conductorAgentInvoker = aeron.conductorAgentInvoker();
            assertNotNull(conductorAgentInvoker);
            final CountersReader countersReader = clientA.countersReader();
            assertEquals(0, countersReader.getCounterValue(SystemCounterDescriptor.CLIENT_TIMEOUTS.id()));

            final Counter counter = aeron.addStaticCounter(COUNTER_TYPE_ID, "test", 42);
            assertNotNull(counter);
            assertFalse(counter.isClosed());
            assertEquals(RECORD_ALLOCATED, aeron.countersReader().getCounterState(counter.id()));

            final Counter counter2 = aeron.addCounter(COUNTER_TYPE_ID * 2, "delete me");

            conductorAgentInvoker.invoke();

            Tests.await(() -> 1 == countersReader.getCounterValue(SystemCounterDescriptor.CLIENT_TIMEOUTS.id()));

            while (null == error.get())
            {
                conductorAgentInvoker.invoke();
                Thread.yield();
            }
            final Throwable timeoutException = error.get();
            if (timeoutException instanceof ClientTimeoutException)
            {
                assertEquals("FATAL - client timeout from driver", timeoutException.getMessage());
            }
            else if (timeoutException instanceof ConductorServiceTimeoutException)
            {
                assertThat(timeoutException.getMessage(), CoreMatchers.startsWith("FATAL - service interval exceeded"));
            }
            else if (timeoutException instanceof AeronException)
            {
                assertThat(
                    timeoutException.getMessage(),
                    CoreMatchers.startsWith("ERROR - unexpected close of heartbeat timestamp counter:"));
            }
            else
            {
                // on unknown error print stack trace
                timeoutException.printStackTrace();
            }

            assertTrue(counter2.isClosed());
            assertEquals(CountersReader.RECORD_RECLAIMED, aeron.countersReader().getCounterState(counter2.id()));
            verify(availableCounterHandler).onAvailableCounter(
                any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));
            verify(unavailableCounterHandler).onUnavailableCounter(
                any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));

            assertFalse(counter.isClosed());
            assertEquals(RECORD_ALLOCATED, aeron.countersReader().getCounterState(counter.id()));
            verify(availableCounterHandler, never()).onAvailableCounter(
                any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
            verify(unavailableCounterHandler, never()).onUnavailableCounter(
                any(CountersReader.class), eq(counter.registrationId()), eq(counter.id()));
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAddCounterAsynchronously()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);

        final long registrationId = clientA.asyncAddCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length());

        Counter counter;
        while (null == (counter = clientA.getCounter(registrationId)))
        {
            Tests.yield();
        }

        assertNotNull(counter);
        assertFalse(counter.isClosed());
        assertEquals(registrationId, clientA.countersReader().getCounterRegistrationId(counter.id()));
        assertEquals(clientA.clientId(), clientA.countersReader().getCounterOwnerId(counter.id()));

        assertNull(clientB.getCounter(registrationId));

        verify(availableCounterHandlerClientA, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter.id()));
        verify(availableCounterHandlerClientB, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldAddCounterAsynchronouslyUsingLabelAndTypeId()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);

        final long registrationId = clientA.asyncAddCounter(COUNTER_TYPE_ID, "test");

        Counter counter;
        while (null == (counter = clientA.getCounter(registrationId)))
        {
            Tests.yield();
        }

        assertNotNull(counter);
        assertFalse(counter.isClosed());
        assertEquals(registrationId, clientA.countersReader().getCounterRegistrationId(counter.id()));
        assertEquals(clientA.clientId(), clientA.countersReader().getCounterOwnerId(counter.id()));

        assertNull(clientB.getCounter(registrationId));

        verify(availableCounterHandlerClientA, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter.id()));
        verify(availableCounterHandlerClientB, timeout(5000L))
            .onAvailableCounter(any(CountersReader.class), eq(registrationId), eq(counter.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldAddStaticCounterAsynchronously()
    {
        final AvailableCounterHandler availableCounterHandlerClientA = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientA = mock(UnavailableCounterHandler.class);
        clientA.addAvailableCounterHandler(availableCounterHandlerClientA);
        clientA.addUnavailableCounterHandler(unavailableCounterHandlerClientA);

        final AvailableCounterHandler availableCounterHandlerClientB = mock(AvailableCounterHandler.class);
        final UnavailableCounterHandler unavailableCounterHandlerClientB = mock(UnavailableCounterHandler.class);
        clientB.addAvailableCounterHandler(availableCounterHandlerClientB);
        clientB.addUnavailableCounterHandler(unavailableCounterHandlerClientB);

        final long registrationId1 = 100;
        final long correlationId1 = clientA.asyncAddStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            registrationId1);
        assertNotEquals(registrationId1, correlationId1);

        Counter counter1;
        while (null == (counter1 = clientA.getCounter(correlationId1)))
        {
            Thread.yield();
        }

        assertNotNull(counter1);
        assertNull(clientB.getCounter(correlationId1));
        assertFalse(counter1.isClosed());
        assertEquals(correlationId1, counter1.correlationId());
        assertEquals(registrationId1, counter1.registrationId());
        assertEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(counter1.id()));
        assertEquals(registrationId1, clientA.countersReader().getCounterRegistrationId(counter1.id()));
        assertEquals(NULL_VALUE, clientA.countersReader().getCounterOwnerId(counter1.id()));
        assertEquals(COUNTER_TYPE_ID, clientA.countersReader().getCounterTypeId(counter1.id()));

        final long registrationId2 = 200;
        final long correlationId2 =
            clientB.asyncAddStaticCounter(COUNTER_TYPE_ID, "test static counter", registrationId2);
        assertNotEquals(registrationId2, correlationId2);

        Counter counter2;
        while (null == (counter2 = clientB.getCounter(correlationId2)))
        {
            Thread.yield();
        }

        assertNotNull(counter2);
        assertNull(clientA.getCounter(correlationId2));
        assertFalse(counter2.isClosed());
        assertEquals(correlationId2, counter2.correlationId());
        assertEquals(registrationId2, counter2.registrationId());
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter2.id()));
        assertEquals(registrationId2, clientB.countersReader().getCounterRegistrationId(counter2.id()));
        assertEquals(NULL_VALUE, clientB.countersReader().getCounterOwnerId(counter2.id()));
        assertEquals("test static counter", clientB.countersReader().getCounterLabel(counter2.id()));
        assertEquals(COUNTER_TYPE_ID, clientB.countersReader().getCounterTypeId(counter2.id()));

        assertSame(counter1, clientA.getCounter(correlationId1));
        assertSame(counter2, clientB.getCounter(correlationId2));

        verify(availableCounterHandlerClientA, Mockito.after(1000L).never())
            .onAvailableCounter(any(CountersReader.class), eq(counter1.registrationId()), eq(counter1.id()));
        verify(availableCounterHandlerClientA, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));
        verify(availableCounterHandlerClientB, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter1.registrationId()), eq(counter1.id()));
        verify(availableCounterHandlerClientB, never())
            .onAvailableCounter(any(CountersReader.class), eq(counter2.registrationId()), eq(counter2.id()));
    }

    @Test
    @InterruptAfter(10)
    void shouldNotCloseAsyncStaticCounterWhenClientInstanceIsClosed()
    {
        final AtomicBoolean clientClosed = new AtomicBoolean();
        clientA.addCloseHandler(() -> clientClosed.set(true));

        final long correlationId1 = clientA.asyncAddStaticCounter(
            COUNTER_TYPE_ID,
            keyBuffer,
            0,
            keyBuffer.capacity(),
            labelBuffer,
            0,
            COUNTER_LABEL.length(),
            1);

        final long correlationId2 = clientB.asyncAddStaticCounter(COUNTER_TYPE_ID + 2, "test static counter", 22);

        final Counter counter3 = clientA.addCounter(COUNTER_TYPE_ID, "delete me");

        Counter counter1, counter2;
        while (null == (counter1 = clientA.getCounter(correlationId1)))
        {
            Tests.yield();
        }
        while (null == (counter2 = clientB.getCounter(correlationId2)))
        {
            Tests.yield();
        }

        clientA.close();

        Tests.await(clientClosed::get);

        assertTrue(counter3.isClosed());
        Tests.await(() -> CountersReader.RECORD_RECLAIMED == clientB.countersReader().getCounterState(counter3.id()));
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter1.id()));
        assertEquals(RECORD_ALLOCATED, clientB.countersReader().getCounterState(counter2.id()));
        assertFalse(counter1.isClosed());
        assertFalse(counter2.isClosed());
    }

    @Test
    void shouldSupportMultipleStaticCountersWithTheSameRegistrationId()
    {
        final String channel = "aeron:ipc?term-length=64k";
        final int streamId = 1000;
        final Publication publication = clientB.addPublication(channel, streamId);

        final Counter shared = clientA.addCounter(555, "shared");
        final long sharedRegistrationId = shared.registrationId();
        final Counter counter1 = clientA.addStaticCounter(1000, "counter1", sharedRegistrationId);
        final Counter counter2 = clientA.addStaticCounter(2000, "counter2", sharedRegistrationId);
        final Counter counter3 = clientB.addStaticCounter(3000, "counter3", sharedRegistrationId);
        assertNotEquals(shared.id(), counter1.id());
        assertNotEquals(counter1.id(), counter2.id());
        assertNotEquals(counter2.id(), counter3.id());
        assertNotEquals(counter1.id(), counter3.id());

        final Counter counter4 = clientB.addStaticCounter(4000, "counter4", publication.registrationId());
        final Counter counter5 = clientA.addStaticCounter(5000, "counter5", publication.registrationId());
        assertNotEquals(counter4.id(), counter5.id());
        assertEquals(publication.registrationId(), counter4.registrationId());
        assertEquals(publication.registrationId(), counter5.registrationId());

        final Counter counter6 = clientB.addStaticCounter(4000, "counter6", publication.registrationId());
        assertEquals(counter4.id(), counter6.id());
        assertEquals(counter4.label(), counter6.label());
        assertEquals(publication.registrationId(), counter6.registrationId());

        final Subscription subscription1 = clientA.addSubscription(channel, streamId);
        final Subscription subscription2 = clientB.addSubscription(channel, streamId);
        assertNotEquals(subscription1.registrationId(), subscription2.registrationId());

        final long asyncCorrelationId = clientB.asyncAddStaticCounter(6000, "counter7", publication.registrationId());

        final int positionLimitId = publication.positionLimitId();
        shared.close();
        publication.close();

        Tests.await(() -> RECORD_ALLOCATED != clientA.countersReader().getCounterState(positionLimitId));
        assertNotEquals(RECORD_ALLOCATED, clientA.countersReader().getCounterState(shared.id()));

        Counter counter7;
        while (null == (counter7 = clientB.getCounter(asyncCorrelationId)))
        {
            Tests.yield();
        }
        assertEquals(publication.registrationId(), counter7.registrationId());
    }

    private void createReadableCounter(final CountersReader counters, final long registrationId, final int counterId)
    {
        if (COUNTER_TYPE_ID == counters.getCounterTypeId(counterId))
        {
            readableCounter = new ReadableCounter(counters, registrationId, counterId);
        }
    }

    private void unavailableCounterHandler(
        final CountersReader counters, final long registrationId, final int counterId)
    {
        if (null != readableCounter && readableCounter.registrationId() == registrationId)
        {
            readableCounter.close();
        }
    }
}
