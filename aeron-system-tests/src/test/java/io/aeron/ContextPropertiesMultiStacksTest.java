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
package io.aeron;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PersistentSubscription;
import io.aeron.cluster.ClusterBackup;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.Configuration;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.aeron.ContextProperties.newContext;
import static io.aeron.test.TestPropertiesUtil.backupAndOverrideSystemProperties;
import static io.aeron.test.TestPropertiesUtil.backupAndSetSystemProperties;
import static io.aeron.test.TestPropertiesUtil.restoreSystemProperties;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Several independently configured Aeron stacks within one JVM, each configured from its own {@link Properties}.
 */
@ExtendWith(InterruptingTestCallback.class)
class ContextPropertiesMultiStacksTest
{
    private static final List<Class<?>> CONFIGURATION_CLASSES = List.of(
        CommonContext.class,
        Aeron.Configuration.class,
        Configuration.class,
        Archive.Configuration.class,
        AeronArchive.Configuration.class,
        ConsensusModule.Configuration.class,
        ClusteredServiceContainer.Configuration.class,
        ClusterBackup.Configuration.class,
        AeronCluster.Configuration.class);

    /**
     * Read from the system properties when needed rather than when a context is created, as they apply to the JVM.
     */
    private static final Set<String> JVM_WIDE_PROPERTIES = Set.of(
        CommonContext.THREAD_NAMING_PROP_NAME,
        CommonContext.DEBUG_TIMEOUT_PROP_NAME,
        CommonContext.FALLBACK_LOGGER_PROP_NAME);

    private static Stream<Arguments> contexts()
    {
        return Stream.of(
            Arguments.of("Aeron.Context", (Supplier<?>)Aeron.Context::new),
            Arguments.of("MediaDriver.Context", (Supplier<?>)MediaDriver.Context::new),
            Arguments.of("Archive.Context", (Supplier<?>)Archive.Context::new),
            Arguments.of("AeronArchive.Context", (Supplier<?>)AeronArchive.Context::new),
            Arguments.of("ConsensusModule.Context", (Supplier<?>)ConsensusModule.Context::new),
            Arguments.of("ClusteredServiceContainer.Context", (Supplier<?>)ClusteredServiceContainer.Context::new),
            Arguments.of("ClusterBackup.Context", (Supplier<?>)ClusterBackup.Context::new),
            Arguments.of("AeronCluster.Context", (Supplier<?>)AeronCluster.Context::new));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("contexts")
    void shouldNotReadSystemPropertiesWhenCreatedFromProperties(
        final String name, final Supplier<?> contextSupplier) throws IllegalAccessException
    {
        final Object expected = newContext(new Properties(), contextSupplier);

        // a number changes the numeric and string values if read, "true" the boolean ones
        for (final String systemValue : List.of("1234567", "true"))
        {
            final Properties backup = new Properties();
            backupAndOverrideSystemProperties(backup, everyConfigurationProperty(systemValue));
            try
            {
                final Object actual = newContext(new Properties(), contextSupplier);
                assertConfigurationEquals(expected, actual, systemValue);
            }
            finally
            {
                restoreSystemProperties(backup);
            }
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldRunIndependentlyConfiguredStacksInOneJvm(@TempDir final Path tempDir)
    {
        final File systemAeronDir = tempDir.resolve("system-aeron").toFile();
        final File systemArchiveDir = tempDir.resolve("system-archive").toFile();
        final Properties stackA = stackProperties(tempDir, "a", ThreadingMode.SHARED, "64k", 1, "11000");
        final Properties stackB = stackProperties(tempDir, "b", ThreadingMode.SHARED_NETWORK, "128k", 2, "12000");

        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            CommonContext.AERON_DIR_PROP_NAME, systemAeronDir.getAbsolutePath(),
            CommonContext.DRIVER_TIMEOUT_PROP_NAME, "20000",
            Configuration.THREADING_MODE_PROP_NAME, ThreadingMode.DEDICATED.name(),
            Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME, "1m",
            Archive.Configuration.ARCHIVE_DIR_PROP_NAME, systemArchiveDir.getAbsolutePath(),
            Archive.Configuration.ARCHIVE_ID_PROP_NAME, "99",
            Archive.Configuration.THREADING_MODE_PROP_NAME, ArchiveThreadingMode.DEDICATED.name(),
            AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:1");

        MediaDriver driverA = null, driverB = null;
        Archive archiveA = null, archiveB = null;
        AeronArchive clientA = null, clientB = null;
        try
        {
            driverA = MediaDriver.launch(newContext(stackA, MediaDriver.Context::new));
            driverB = MediaDriver.launch(newContext(stackB, MediaDriver.Context::new));
            archiveA = Archive.launch(newContext(stackA, Archive.Context::new));
            archiveB = Archive.launch(newContext(stackB, Archive.Context::new));
            clientA = AeronArchive.connect(newContext(stackA, AeronArchive.Context::new));
            clientB = AeronArchive.connect(newContext(stackB, AeronArchive.Context::new));

            assertEquals(ThreadingMode.SHARED, driverA.context().threadingMode());
            assertEquals(ThreadingMode.SHARED_NETWORK, driverB.context().threadingMode());
            assertEquals(ArchiveThreadingMode.SHARED, archiveA.context().threadingMode());
            assertEquals(ArchiveThreadingMode.SHARED, archiveB.context().threadingMode());
            assertEquals(1, clientA.archiveId());
            assertEquals(2, clientB.archiveId());
            assertEquals(11000, archiveA.context().aeron().context().driverTimeoutMs());
            assertEquals(12000, archiveB.context().aeron().context().driverTimeoutMs());
            assertEquals(11000, clientA.context().aeron().context().driverTimeoutMs());
            assertEquals(12000, clientB.context().aeron().context().driverTimeoutMs());

            exchangeMessage(clientA.context().aeron(), 64 * 1024);
            exchangeMessage(clientB.context().aeron(), 128 * 1024);

            assertFalse(systemAeronDir.exists());
            assertFalse(systemArchiveDir.exists());
        }
        finally
        {
            CloseHelper.closeAll(clientA, clientB, archiveA, archiveB, driverA, driverB);
            restoreSystemProperties(backup);
        }
    }

    @Test
    void shouldCreateIdleStrategiesFromThePropertiesTheirSupplierWasCreatedWith()
    {
        final Properties properties = new Properties();
        properties.setProperty(Archive.Configuration.ARCHIVE_IDLE_STRATEGY_PROP_NAME, YieldingIdleStrategy.ALIAS);
        properties.setProperty(
            ClusteredServiceContainer.Configuration.CLUSTER_IDLE_STRATEGY_PROP_NAME, YieldingIdleStrategy.ALIAS);

        // created when a context is concluded, but only used later when the agents are created
        final Supplier<IdleStrategy> archiveSupplier =
            newContext(properties, () -> Archive.Configuration.idleStrategySupplier(null));
        final Supplier<IdleStrategy> clusterSupplier =
            newContext(properties, () -> ClusteredServiceContainer.Configuration.idleStrategySupplier(null));

        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            Archive.Configuration.ARCHIVE_IDLE_STRATEGY_PROP_NAME, NoOpIdleStrategy.ALIAS,
            ClusteredServiceContainer.Configuration.CLUSTER_IDLE_STRATEGY_PROP_NAME, NoOpIdleStrategy.ALIAS);
        try
        {
            assertInstanceOf(YieldingIdleStrategy.class, archiveSupplier.get());
            assertInstanceOf(YieldingIdleStrategy.class, clusterSupplier.get());
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    @Test
    @InterruptAfter(30)
    void shouldConcludePersistentSubscriptionWithAeronClientFromTheSameProperties(@TempDir final Path tempDir)
    {
        final Properties properties = new Properties();
        properties.setProperty(CommonContext.AERON_DIR_PROP_NAME, tempDir.resolve("aeron").toString());
        properties.setProperty(Configuration.DIR_DELETE_ON_START_PROP_NAME, "true");
        properties.setProperty(Configuration.DIR_DELETE_ON_SHUTDOWN_PROP_NAME, "true");
        properties.setProperty(Configuration.THREADING_MODE_PROP_NAME, ThreadingMode.SHARED.name());

        // fail fast if the Aeron client looked for the driver in the system properties' directory
        final Properties backup = new Properties();
        backupAndSetSystemProperties(
            backup,
            CommonContext.AERON_DIR_PROP_NAME, tempDir.resolve("system-aeron").toString(),
            CommonContext.DRIVER_TIMEOUT_PROP_NAME, "1000");
        try (MediaDriver driver = MediaDriver.launch(newContext(properties, MediaDriver.Context::new)))
        {
            final PersistentSubscription.Context context = newContext(properties, PersistentSubscription.Context::new)
                .recordingId(0)
                .liveChannel("aeron:ipc")
                .liveStreamId(1001)
                .replayChannel("aeron:ipc")
                .replayStreamId(1002)
                .aeronArchiveContext(newContext(properties, AeronArchive.Context::new));
            try
            {
                context.conclude();

                assertEquals(driver.aeronDirectoryName(), context.aeron().context().aeronDirectoryName());
            }
            finally
            {
                if (null != context.aeron())
                {
                    context.close();
                }
            }
        }
        finally
        {
            restoreSystemProperties(backup);
        }
    }

    private static Properties stackProperties(
        final Path tempDir,
        final String name,
        final ThreadingMode threadingMode,
        final String ipcTermLength,
        final int archiveId,
        final String driverTimeoutMs)
    {
        final Properties properties = new Properties();
        properties.setProperty(CommonContext.AERON_DIR_PROP_NAME, tempDir.resolve("aeron-" + name).toString());
        properties.setProperty(Configuration.DIR_DELETE_ON_START_PROP_NAME, "true");
        properties.setProperty(Configuration.DIR_DELETE_ON_SHUTDOWN_PROP_NAME, "true");
        properties.setProperty(Configuration.THREADING_MODE_PROP_NAME, threadingMode.name());
        properties.setProperty(Configuration.IPC_TERM_BUFFER_LENGTH_PROP_NAME, ipcTermLength);
        properties.setProperty(CommonContext.DRIVER_TIMEOUT_PROP_NAME, driverTimeoutMs);
        properties.setProperty(
            Archive.Configuration.ARCHIVE_DIR_PROP_NAME, tempDir.resolve("archive-" + name).toString());
        properties.setProperty(Archive.Configuration.ARCHIVE_ID_PROP_NAME, Integer.toString(archiveId));
        properties.setProperty(Archive.Configuration.THREADING_MODE_PROP_NAME, ArchiveThreadingMode.SHARED.name());
        properties.setProperty(Archive.Configuration.CONTROL_CHANNEL_ENABLED_PROP_NAME, "false");
        properties.setProperty(
            Archive.Configuration.REPLICATION_CHANNEL_PROP_NAME, "aeron:udp?endpoint=localhost:0");
        properties.setProperty(AeronArchive.Configuration.CONTROL_CHANNEL_PROP_NAME, "aeron:ipc");
        properties.setProperty(AeronArchive.Configuration.CONTROL_RESPONSE_CHANNEL_PROP_NAME, "aeron:ipc");

        return properties;
    }

    private static void exchangeMessage(final Aeron aeron, final int expectedTermLength)
    {
        try (Subscription subscription = aeron.addSubscription("aeron:ipc", 1001);
            Publication publication = aeron.addPublication("aeron:ipc", 1001))
        {
            assertEquals(expectedTermLength, publication.termBufferLength());

            final UnsafeBuffer message = new UnsafeBuffer(new byte[64]);
            while (publication.offer(message) < 0)
            {
                Tests.yield();
            }

            while (0 == subscription.poll((buffer, offset, length, header) -> {}, 1))
            {
                Tests.yield();
            }
        }
    }

    private static Properties everyConfigurationProperty(final String value) throws IllegalAccessException
    {
        final Properties properties = new Properties();
        for (final Class<?> configurationClass : CONFIGURATION_CLASSES)
        {
            for (final Field field : configurationClass.getFields())
            {
                if (Modifier.isStatic(field.getModifiers()) &&
                    String.class == field.getType() &&
                    field.getName().endsWith("_PROP_NAME"))
                {
                    final String propertyName = (String)field.get(null);
                    if (!JVM_WIDE_PROPERTIES.contains(propertyName))
                    {
                        properties.setProperty(propertyName, value);
                    }
                }
            }
        }

        return properties;
    }

    private static void assertConfigurationEquals(final Object expected, final Object actual, final String systemValue)
        throws IllegalAccessException
    {
        for (Class<?> type = expected.getClass(); Object.class != type; type = type.getSuperclass())
        {
            for (final Field field : type.getDeclaredFields())
            {
                final Class<?> fieldType = field.getType();
                if (!Modifier.isStatic(field.getModifiers()) &&
                    (fieldType.isPrimitive() || fieldType.isEnum() || fieldType == String.class ||
                    Number.class.isAssignableFrom(fieldType) || fieldType == Boolean.class ||
                    fieldType == byte[].class))
                {
                    field.setAccessible(true);
                    final Object expectedValue = field.get(expected);
                    final Object actualValue = field.get(actual);
                    final boolean equal = expectedValue instanceof byte[] ?
                        Arrays.equals((byte[])expectedValue, (byte[])actualValue) :
                        Objects.equals(expectedValue, actualValue);
                    if (!equal)
                    {
                        throw new AssertionError(
                            type.getName() + "." + field.getName() + " read the system properties set to " +
                            systemValue + ": expected=" + expectedValue + " actual=" + actualValue);
                    }
                }
            }
        }
    }
}
