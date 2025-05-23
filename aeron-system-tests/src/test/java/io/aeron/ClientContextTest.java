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
import io.aeron.driver.status.ClientHeartbeatTimestamp;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.status.HeartbeatTimestamp;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(InterruptingTestCallback.class)
class ClientContextTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();
    private TestMediaDriver mediaDriver;

    @BeforeEach
    void before()
    {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);

        mediaDriver = TestMediaDriver.launch(driverCtx, systemTestWatcher);
        systemTestWatcher.dataCollector().add(mediaDriver.context().aeronDirectory());
    }

    @AfterEach
    void after()
    {
        CloseHelper.close(mediaDriver);
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldPreventCreatingMultipleClientsWithTheSameContext()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName());

        try (Aeron ignore = Aeron.connect(ctx))
        {
            assertThrows(ConcurrentConcludeException.class, () -> Aeron.connect(ctx));
        }
    }

    @Test
    @SuppressWarnings("try")
    void shouldRequireInvokerModeIfClientLockIsSet()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientLock(NoOpLock.INSTANCE);

        assertThrows(AeronException.class, () -> Aeron.connect(ctx));
    }

    @Test
    @InterruptAfter(10)
    @SuppressWarnings("try")
    void shouldAllowCustomLockInAgentRunnerModeIfNotInstanceOfNoOpLock()
    {
        final Aeron.Context ctx = new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientLock(new ReentrantLock());

        try (Aeron aeron = Aeron.connect(ctx))
        {
            aeron.clientId();
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldHaveUniqueCorrelationIdsAcrossMultipleClientsToTheSameDriver()
    {
        final Aeron.Context ctx = new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName());

        try (Aeron aeron0 = Aeron.connect(ctx.clone());
            Aeron aeron1 = Aeron.connect(ctx.clone()))
        {
            assertNotEquals(aeron0.nextCorrelationId(), aeron1.nextCorrelationId());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "my-test-client" })
    @InterruptAfter(10)
    void shouldAddClientInfoToTheHeartbeatTimestampCounter(final String clientName)
    {
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientName(clientName)
            .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))))
        {
            // trigger creation of the timestamp counter on the driver side
            assertNotNull(aeron.addCounter(1000, "test"));

            final long clientId = aeron.clientId();
            int counterId = Aeron.NULL_VALUE;
            final CountersReader countersReader = aeron.countersReader();
            final String baseLabel = ClientHeartbeatTimestamp.NAME + ": id=" + clientId;
            final String expandedLabel = baseLabel + " name=" + clientName + " " +
                AeronCounters.formatVersionInfo(AeronVersion.VERSION, AeronVersion.GIT_SHA);
            while (true)
            {
                if (Aeron.NULL_VALUE == counterId)
                {
                    counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                        countersReader, HeartbeatTimestamp.HEARTBEAT_TYPE_ID, clientId);
                }
                else
                {
                    final int labelLength =
                        countersReader.metaDataBuffer().getInt(
                        CountersReader.metaDataOffset(counterId) + CountersReader.LABEL_OFFSET);
                    if (labelLength > baseLabel.length())
                    {
                        assertEquals(expandedLabel, countersReader.getCounterLabel(counterId));
                        break;
                    }
                }
                Tests.yield();
            }
        }
    }

    @Test
    @InterruptAfter(10)
    void shouldAddClientInfoToTheHeartbeatTimestampCounterUpToMaxLabelLength()
    {
        final String clientName = Tests.generateStringWithSuffix("", "X", 100);
        try (Aeron aeron = Aeron.connect(new Aeron.Context()
            .aeronDirectoryName(mediaDriver.aeronDirectoryName())
            .clientName(clientName)
            .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(10))))
        {
            // trigger creation of the timestamp counter on the driver side
            assertNotNull(aeron.addCounter(1000, "test"));

            final long clientId = aeron.clientId();
            int counterId = Aeron.NULL_VALUE;
            final CountersReader countersReader = aeron.countersReader();
            final String baseLabel = ClientHeartbeatTimestamp.NAME + ": id=" + clientId;
            final String expandedLabel =
                baseLabel + " name=" + clientName.substring(0, 100) +
                " version=" + AeronVersion.VERSION + " commit=" + AeronVersion.GIT_SHA;
            while (true)
            {
                if (Aeron.NULL_VALUE == counterId)
                {
                    counterId = HeartbeatTimestamp.findCounterIdByRegistrationId(
                        countersReader, HeartbeatTimestamp.HEARTBEAT_TYPE_ID, clientId);
                }
                else
                {
                    final int labelLength =
                        countersReader.metaDataBuffer().getInt(
                        CountersReader.metaDataOffset(counterId) + CountersReader.LABEL_OFFSET);
                    if (labelLength > baseLabel.length())
                    {
                        assertEquals(expandedLabel, countersReader.getCounterLabel(counterId));
                        break;
                    }
                }
                Tests.yield();
            }
        }
    }
}
