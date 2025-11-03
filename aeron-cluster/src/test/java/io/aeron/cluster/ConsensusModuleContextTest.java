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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.Counter;
import io.aeron.RethrowingErrorHandler;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.mark.MarkFileHeaderDecoder;
import io.aeron.cluster.service.ClusterClock;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.security.Authenticator;
import io.aeron.security.AuthenticatorSupplier;
import io.aeron.security.AuthorisationService;
import io.aeron.security.AuthorisationServiceSupplier;
import io.aeron.security.DefaultAuthenticatorSupplier;
import io.aeron.security.SessionProxy;
import io.aeron.test.TestContexts;
import io.aeron.test.Tests;
import io.aeron.test.cluster.TestClusterClock;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static io.aeron.AeronCounters.CLUSTER_ELECTION_COUNT_TYPE_ID;
import static io.aeron.AeronCounters.CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID;
import static io.aeron.AeronCounters.NODE_CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.AUTHENTICATOR_SUPPLIER_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.CLUSTER_CLOCK_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.COMMIT_POSITION_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONSENSUS_MODULE_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.CONTROL_TOGGLE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.DEFAULT_AUTHORISATION_SERVICE_SUPPLIER;
import static io.aeron.cluster.ConsensusModule.Configuration.ELECTION_STATE_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.SERVICE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.SNAPSHOT_COUNTER_TYPE_ID;
import static io.aeron.cluster.ConsensusModule.Configuration.TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP;
import static io.aeron.cluster.ConsensusModule.Configuration.TIMER_SERVICE_SUPPLIER_PROP_NAME;
import static io.aeron.cluster.ConsensusModule.Configuration.TIMER_SERVICE_SUPPLIER_WHEEL;
import static io.aeron.cluster.codecs.mark.ClusterComponentType.CONSENSUS_MODULE;
import static io.aeron.cluster.service.ClusterMarkFile.ERROR_BUFFER_MIN_LENGTH;
import static io.aeron.cluster.service.ClusterMarkFile.HEADER_LENGTH;
import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.MARK_FILE_DIR_PROP_NAME;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConsensusModuleContextTest
{
    @TempDir
    File clusterDir;

    private ConsensusModule.Context context;
    private final CountersManager countersManager = Tests.newCountersManager(16 * 1024);
    private long registrationId = 0;

    @BeforeEach
    void beforeEach()
    {
        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.subscriberErrorHandler()).thenReturn(new RethrowingErrorHandler());
        when(aeronContext.aeronDirectoryName()).thenReturn("some aeron dir");
        when(aeronContext.useConductorAgentInvoker()).thenReturn(true);
        when(aeronContext.filePageSize()).thenReturn(PAGE_MIN_SIZE);
        final AgentInvoker conductorInvoker = mock(AgentInvoker.class);
        final Aeron aeron = mock(Aeron.class);
        when(aeron.addCounter(
            anyInt(), any(DirectBuffer.class), anyInt(), anyInt(), any(DirectBuffer.class), anyInt(), anyInt()))
            .thenAnswer(Tests.addCounterAnswer(countersManager, () -> registrationId++));
        when(aeron.context()).thenReturn(aeronContext);
        when(aeron.conductorAgentInvoker()).thenReturn(conductorInvoker);
        when(aeron.countersReader()).thenReturn(countersManager);

        context = TestContexts.localhostConsensusModule()
            .clusterDir(clusterDir)
            .aeron(aeron)
            .errorCounter(mock(AtomicCounter.class))
            .ingressChannel("must be specified")
            .replicationChannel("must be specified")
            .moduleStateCounter(newCounter("moduleState", CONSENSUS_MODULE_STATE_TYPE_ID))
            .electionStateCounter(newCounter("electionState", ELECTION_STATE_TYPE_ID))
            .electionCounter(newCounter("electionCount", CLUSTER_ELECTION_COUNT_TYPE_ID))
            .leadershipTermIdCounter(newCounter("leadershipTermId", CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID))
            .clusterNodeRoleCounter(newCounter("clusterNodeRole", AeronCounters.CLUSTER_NODE_ROLE_TYPE_ID))
            .commitPositionCounter(newCounter("commitPosition", COMMIT_POSITION_TYPE_ID))
            .controlToggleCounter(newCounter("controlToggle", CONTROL_TOGGLE_TYPE_ID))
            .nodeControlToggleCounter(newCounter("nodeControlToggle", NODE_CONTROL_TOGGLE_TYPE_ID))
            .snapshotCounter(newCounter("snapshot", SNAPSHOT_COUNTER_TYPE_ID))
            .timedOutClientCounter(newCounter("timedOut", AeronCounters.CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID));
    }

    private Counter newCounter(final String name, final int typeId)
    {
        final AtomicCounter atomicCounter = countersManager.newCounter(name, typeId);
        return new Counter(countersManager, ++registrationId, atomicCounter.id());
    }

    @AfterEach
    void afterEach()
    {
        context.close();
    }

    @ParameterizedTest
    @ValueSource(strings = { TIMER_SERVICE_SUPPLIER_WHEEL, TIMER_SERVICE_SUPPLIER_PRIORITY_HEAP })
    void validTimerServiceSupplier(final String supplierName)
    {
        System.setProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, supplierName);
        try
        {
            context.conclude();

            final TimerServiceSupplier supplier = context.timerServiceSupplier();
            assertNotNull(supplier);

            final TimerService.TimerHandler timerHandler = mock(TimerService.TimerHandler.class);
            final TimerService timerService = supplier.newInstance(context.clusterClock().timeUnit(), timerHandler);

            assertNotNull(timerService);
            assertEquals(supplierName, supplier.getClass().getName());
        }
        finally
        {
            System.clearProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void unknownTimerServiceSupplier()
    {
        final String supplierName = "unknown timer service supplier";
        System.setProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME, supplierName);
        try
        {
            final ClusterException exception = assertThrows(ClusterException.class, context::conclude);
            assertEquals("ERROR - invalid TimerServiceSupplier: " + supplierName, exception.getMessage());
        }
        finally
        {
            System.clearProperty(TIMER_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void defaultTimerServiceSupplier()
    {
        context.conclude();

        final TimerServiceSupplier supplier = context.timerServiceSupplier();
        assertNotNull(supplier);

        final TimerService.TimerHandler timerHandler = mock(TimerService.TimerHandler.class);
        final TimerService timerService = supplier.newInstance(context.clusterClock().timeUnit(), timerHandler);

        assertNotNull(timerService);
        assertEquals(WheelTimerService.class, timerService.getClass());
    }

    @Test
    void explicitTimerServiceSupplier()
    {
        final TimerServiceSupplier supplier = (clusterClock, timerHandler) -> null;

        context.timerServiceSupplier(supplier);
        assertSame(supplier, context.timerServiceSupplier());

        context.conclude();

        assertSame(supplier, context.timerServiceSupplier());
    }

    @Test
    void rejectInvalidLogChannelParameters()
    {
        final String channelTermId = context.logChannel() + "|" + CommonContext.TERM_ID_PARAM_NAME + "=0";
        final String channelInitialTermId =
            context.logChannel() + "|" + CommonContext.INITIAL_TERM_ID_PARAM_NAME + "=0";
        final String channelTermOffset = context.logChannel() + "|" + CommonContext.TERM_OFFSET_PARAM_NAME + "=0";

        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelTermId).conclude());
        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelInitialTermId).conclude());
        assertThrows(ConfigurationException.class, () -> context.clone().logChannel(channelTermOffset).conclude());
    }

    @Test
    void defaultAuthorisationServiceSupplierAllowsBackupAndStandby()
    {
        assertSame(AllowBackupAndStandbyAuthorisationService.INSTANCE, DEFAULT_AUTHORISATION_SERVICE_SUPPLIER.get());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsNotSet()
    {
        assertNull(context.authorisationServiceSupplier());

        context.conclude();

        System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
    }

    @Test
    void shouldUseDefaultAuthorisationServiceSupplierIfTheSystemPropertyIsSetToEmptyValue()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, "");
        try
        {
            assertNull(context.authorisationServiceSupplier());

            context.conclude();

            assertSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldInstantiateAuthorisationServiceSupplierBasedOnTheSystemProperty()
    {
        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            final AuthorisationServiceSupplier supplier = context.authorisationServiceSupplier();
            assertNotSame(DEFAULT_AUTHORISATION_SERVICE_SUPPLIER, supplier);
            assertInstanceOf(TestAuthorisationSupplier.class, supplier);
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldUseProvidedAuthorisationServiceSupplierInstance()
    {
        final AuthorisationServiceSupplier providedSupplier = mock(AuthorisationServiceSupplier.class);
        context.authorisationServiceSupplier(providedSupplier);
        assertSame(providedSupplier, context.authorisationServiceSupplier());

        System.setProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, TestAuthorisationSupplier.class.getName());
        try
        {
            context.conclude();
            assertSame(providedSupplier, context.authorisationServiceSupplier());
        }
        finally
        {
            System.clearProperty(AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldUseDefaultAuthenticatorSupplierIfTheSystemPropertyIsSetToEmptyValue()
    {
        System.setProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME, "");
        try
        {
            assertNull(context.authenticatorSupplier());

            context.conclude();

            final AuthenticatorSupplier authenticatorSupplier = context.authenticatorSupplier();
            assertSame(DefaultAuthenticatorSupplier.INSTANCE, authenticatorSupplier);
        }
        finally
        {
            System.clearProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldInstantiateAuthenticatorSupplierBasedOnTheSystemProperty()
    {
        System.setProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME, TestAuthenticatorSupplier.class.getName());
        try
        {
            context.conclude();
            final AuthenticatorSupplier supplier = context.authenticatorSupplier();
            assertInstanceOf(TestAuthenticatorSupplier.class, supplier);
        }
        finally
        {
            System.clearProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void shouldUseProvidedAAuthenticatorSupplierInstance()
    {
        final AuthenticatorSupplier providedSupplier = mock(AuthenticatorSupplier.class);
        context.authenticatorSupplier(providedSupplier);
        assertSame(providedSupplier, context.authenticatorSupplier());

        System.setProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME, TestAuthenticatorSupplier.class.getName());
        try
        {
            context.conclude();
            assertSame(providedSupplier, context.authenticatorSupplier());
        }
        finally
        {
            System.clearProperty(AUTHENTICATOR_SUPPLIER_PROP_NAME);
        }
    }

    @Test
    void writeAuthenticatorSupplierClassNameIntoTheMarkFile()
    {
        final TestAuthenticatorSupplier authenticatorSupplier = new TestAuthenticatorSupplier();
        final String authenticatorSupplierClassName = authenticatorSupplier.getClass().getName();
        context.authenticatorSupplier(authenticatorSupplier);

        context.conclude();

        final ClusterMarkFile markFile = context.clusterMarkFile();
        assertNotNull(markFile);
        final MarkFileHeaderDecoder decoder = markFile.decoder();
        decoder.sbeRewind();
        assertEquals(ClusterMarkFile.SEMANTIC_VERSION, decoder.version());
        assertEquals(CONSENSUS_MODULE, decoder.componentType());
        assertEquals(SystemUtil.getPid(), decoder.pid());
        assertEquals(SERVICE_ID, decoder.serviceId());
        assertEquals(context.aeron().context().aeronDirectoryName(), decoder.aeronDirectory());
        assertEquals(context.controlChannel(), decoder.controlChannel());
        assertEquals(context.ingressChannel(), decoder.ingressChannel());
        assertNotNull(decoder.serviceName());
        assertEquals(authenticatorSupplierClassName, decoder.authenticator());
    }

    @Test
    void shouldValidateModuleStateCounter()
    {
        context.moduleStateCounter(newCounter("moduleState", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateElectionStateCounter()
    {
        context.electionStateCounter(newCounter("electionState", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateClusterNodeRoleCounter()
    {
        context.clusterNodeRoleCounter(newCounter("clusterNodeRole", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateCommitPositionCounter()
    {
        context.commitPositionCounter(newCounter("commitPosition", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateControlToggleCounter()
    {
        context.controlToggleCounter(newCounter("controlToggle", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateSnapshotCounter()
    {
        context.snapshotCounter(newCounter("snapshot", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldValidateTimedOutClientCounter()
    {
        context.timedOutClientCounter(newCounter("timedOut", CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID));
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void shouldThrowIllegalStateExceptionIfAnActiveMarkFileExists()
    {
        final ConsensusModule.Context another = context.clone();
        context.conclude();

        final IllegalStateException exception =
            assertThrowsExactly(IllegalStateException.class, another::conclude);
        assertThat(exception.getMessage(), startsWith("active mark file detected: "));
    }

    @ParameterizedTest
    @CsvSource({ "0, 1000", "5000,5000", "2000000000, 1000000001" })
    void startupCanvassTimeoutMustBeMultiplesOfTheLeaderHeartbeatTimeout(
        final long startupCanvassTimeoutNs, final long leaderHeartbeatTimeoutNs)
    {
        context.startupCanvassTimeoutNs(startupCanvassTimeoutNs)
            .leaderHeartbeatTimeoutNs(leaderHeartbeatTimeoutNs);

        final ClusterException exception = assertThrows(ClusterException.class, context::conclude);
        assertEquals("ERROR - startupCanvassTimeoutNs=" + startupCanvassTimeoutNs +
            " must be a multiple of leaderHeartbeatTimeoutNs=" + leaderHeartbeatTimeoutNs,
            exception.getMessage());
    }

    @Test
    void startupCanvassTimeoutMustCanBeSetToBeMultiplesOfTheLeaderHeartbeatTimeout()
    {
        context.startupCanvassTimeoutNs(TimeUnit.SECONDS.toNanos(30))
            .leaderHeartbeatTimeoutNs(TimeUnit.SECONDS.toNanos(5));

        context.conclude();
    }

    @Test
    void shouldThrowIfConductorInvokerModeIsNotUsed()
    {
        when(context.aeron().context().useConductorAgentInvoker()).thenReturn(false);
        assertThrows(ClusterException.class, () -> context.conclude());
    }

    @Test
    void shouldUseCandidateTermIdFromClusterMarkFileIfNodeStateFileIsNew()
    {
        final TestClusterClock epochClock = new TestClusterClock(MILLISECONDS);
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            new File(clusterDir, ClusterMarkFile.FILENAME),
            CONSENSUS_MODULE,
            ERROR_BUFFER_MIN_LENGTH,
            epochClock,
            1_000,
            PAGE_MIN_SIZE);
        final long existingCandidateTermId = 23;

        assertEquals(Aeron.NULL_VALUE, clusterMarkFile.candidateTermId());
        clusterMarkFile.encoder().candidateTermId(existingCandidateTermId);
        context.clusterMarkFile(clusterMarkFile);

        context.conclude();

        assertEquals(existingCandidateTermId, context.nodeStateFile().candidateTerm().candidateTermId());
    }

    @Test
    void clusterDirectoryNameShouldMatchClusterDirWhenClusterDirSet() throws IOException
    {
        context.clusterDir(clusterDir);
        context.conclude();

        assertEquals(
            new File(context.clusterDirectoryName()).getCanonicalPath(), context.clusterDir().getCanonicalPath());
    }

    @Test
    void clusterDirectoryNameShouldMatchClusterDirWhenClusterDirectoryNameSet() throws IOException
    {
        context.clusterDir(null);
        context.clusterDirectoryName(clusterDir.getAbsolutePath());
        context.conclude();

        assertEquals(
            new File(context.clusterDirectoryName()).getCanonicalPath(), context.clusterDir().getCanonicalPath());
    }

    @Test
    void clusterServiceDirectoryNameShouldBeSetFromClusterDirectoryName(@TempDir final Path dir) throws IOException
    {
        final File clusterDir = dir.resolve("b/./42/../c").toFile();
        context.clusterServicesDirectoryName("");
        context.clusterDirectoryName("rubbish");
        context.clusterDir(clusterDir);

        context.conclude();

        final String resolvedPath = clusterDir.getCanonicalFile().getAbsolutePath();
        assertEquals(resolvedPath, context.clusterDirectoryName());
        assertEquals(resolvedPath, context.clusterServicesDirectoryName());
    }

    @Test
    void clusterServiceDirectoryNameShouldBeResolved(@TempDir final Path dir) throws IOException
    {
        final Path serviceDirectory = dir.resolve("m/n/././././o");
        context.clusterServicesDirectoryName(serviceDirectory.toString());
        context.clusterDirectoryName("something else");
        context.clusterDir(dir.resolve("b/./42/../c").toFile());

        context.conclude();

        assertEquals(context.clusterDir().getAbsolutePath(), context.clusterDirectoryName());
        assertEquals(serviceDirectory.toFile().getCanonicalPath(), context.clusterServicesDirectoryName());
    }

    @Test
    void concludeShouldCreateMarkFileDirSetViaSystemProperty(final @TempDir File tempDir) throws IOException
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark/file/./.././dir");
        assertFalse(markFileDir.exists());

        System.setProperty(MARK_FILE_DIR_PROP_NAME, markFileDir.getPath());
        try
        {
            assertSame(null, context.markFileDir());

            context.conclude();

            assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
            assertTrue(markFileDir.getCanonicalFile().exists());
            assertTrue(new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME).exists());
        }
        finally
        {
            System.clearProperty(MARK_FILE_DIR_PROP_NAME);
        }
    }

    @Test
    void concludeShouldCreateMarkFileDirSetDirectly(final @TempDir File tempDir) throws IOException
    {
        final File rootDir = new File(tempDir, "root");
        final File markFileDir = new File(rootDir, "mark-file-dir");
        assertFalse(markFileDir.exists());
        context.markFileDir(markFileDir);

        context.conclude();

        assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
        assertTrue(markFileDir.getCanonicalFile().exists());
        assertTrue(new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME).exists());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldRemoveLinkIfMarkFileIsInClusterDir(final boolean isSet) throws IOException
    {
        final File markFileDir = isSet ? context.clusterDir() : null;

        context.markFileDir(markFileDir);
        final File oldLinkFile = new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME);
        assertTrue(oldLinkFile.createNewFile());
        assertTrue(oldLinkFile.exists());

        context.conclude();

        assertFalse(oldLinkFile.exists());
    }

    @Test
    void concludeShouldCreateLinkPointingToTheParentDirectoryOfTheMarkFile(
        final @TempDir File clusterDir,
        final @TempDir File markFileDir,
        final @TempDir File otherDir) throws IOException
    {
        final ClusterMarkFile clusterMarkFile = new ClusterMarkFile(
            new File(otherDir, "test.me"),
            CONSENSUS_MODULE,
            ERROR_BUFFER_MIN_LENGTH,
            SystemEpochClock.INSTANCE,
            10,
            PAGE_MIN_SIZE);
        context
            .clusterDir(clusterDir)
            .markFileDir(markFileDir)
            .clusterMarkFile(clusterMarkFile);

        context.conclude();

        assertEquals(clusterDir.getCanonicalFile(), context.clusterDir());
        assertEquals(markFileDir.getCanonicalFile(), context.markFileDir());
        assertEquals(otherDir, context.clusterMarkFile().parentDirectory());
        final File linkFile = new File(context.clusterDir(), ClusterMarkFile.LINK_FILENAME);
        assertTrue(linkFile.exists());
        assertEquals(otherDir.getCanonicalPath(), new String(Files.readAllBytes(linkFile.toPath()), US_ASCII));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "io.aeron.cluster.MillisecondClusterClock",
        "io.aeron.cluster.NanosecondClusterClock",
        "io.aeron.test.cluster.TestClusterClock" })
    void shouldSetClusterClockViaSystemProperty(final String clockClassName)
    {
        System.setProperty(CLUSTER_CLOCK_PROP_NAME, clockClassName);
        try
        {
            context.clusterClock(null);

            context.conclude();

            final ClusterClock clusterClock = context.clusterClock();
            assertNotNull(clusterClock);
            assertEquals(clockClassName, clusterClock.getClass().getName());
        }
        finally
        {
            System.clearProperty(CLUSTER_CLOCK_PROP_NAME);
        }
    }

    @Test
    void shouldThrowClusterExceptionIfClockCannotBeCreated()
    {
        final String clockClassName = String.class.getName();
        System.setProperty(CLUSTER_CLOCK_PROP_NAME, clockClassName);
        try
        {
            context.clusterClock(null);

            final ClusterException clusterException =
                assertThrowsExactly(ClusterException.class, context::conclude);
            assertEquals("ERROR - failed to instantiate ClusterClock " + clockClassName, clusterException.getMessage());
            final Throwable cause = clusterException.getCause();
            assertInstanceOf(ClassCastException.class, cause);
        }
        finally
        {
            System.clearProperty(CLUSTER_CLOCK_PROP_NAME);
        }
    }

    @Test
    void shouldUseExplicitlyAssignedClockInstance()
    {
        final TestClusterClock clock = new TestClusterClock(TimeUnit.NANOSECONDS);
        System.setProperty(CLUSTER_CLOCK_PROP_NAME, String.class.getName());
        try
        {
            context.clusterClock(clock);

            context.conclude();

            assertSame(clock, context.clusterClock());
        }
        finally
        {
            System.clearProperty(CLUSTER_CLOCK_PROP_NAME);
        }
    }

    @Test
    void shouldAllowElectionCounterToBeExplicitlySet()
    {
        final Counter electionCounter = newCounter("x", CLUSTER_ELECTION_COUNT_TYPE_ID);
        context.electionCounter(electionCounter);
        assertSame(electionCounter, context.electionCounter());

        context.conclude();

        assertSame(electionCounter, context.electionCounter());
    }

    @Test
    void shouldThrowConfigurationExceptionIfElectionCounterHasWrongType()
    {
        final Counter electionCounter = newCounter("wrong type id", 1);
        context.electionCounter(electionCounter);
        assertSame(electionCounter, context.electionCounter());

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - The type for counterId=" + electionCounter.id() +
            ", typeId=1 does not match the expected=" + CLUSTER_ELECTION_COUNT_TYPE_ID,
            exception.getMessage());
    }

    @Test
    void shouldCreateElectionCounter()
    {
        context.electionCounter(null);

        context.conclude();

        final Counter electionCounter = context.electionCounter();
        assertNotNull(electionCounter);
        assertEquals(CLUSTER_ELECTION_COUNT_TYPE_ID, countersManager.getCounterTypeId(electionCounter.id()));
    }

    @Test
    void shouldAllowLeadershipTermIdCounterToBeExplicitlySet()
    {
        final Counter counter = newCounter("x", CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID);
        context.leadershipTermIdCounter(counter);
        assertSame(counter, context.leadershipTermIdCounter());

        context.conclude();

        assertSame(counter, context.leadershipTermIdCounter());
    }

    @Test
    void shouldThrowConfigurationExceptionIfLeadershipTermIdCounterHasWrongType()
    {
        final Counter counter = newCounter("wrong type id", 5);
        context.leadershipTermIdCounter(counter);
        assertSame(counter, context.leadershipTermIdCounter());

        final ConfigurationException exception = assertThrows(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - The type for counterId=" + counter.id() +
            ", typeId=5 does not match the expected=" + CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID,
            exception.getMessage());
    }

    @Test
    void shouldCreateLeadershipTermIdCounter()
    {
        context.leadershipTermIdCounter(null);

        context.conclude();

        final Counter counter = context.leadershipTermIdCounter();
        assertNotNull(counter);
        assertEquals(CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, countersManager.getCounterTypeId(counter.id()));
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldGenerateAgentRoleNameIfNotSet(final String emptyAgentRoleName)
    {
        context.clusterId(19).clusterMemberId(7).agentRoleName(emptyAgentRoleName);

        context.conclude();

        assertEquals("consensus-module-19-7", context.agentRoleName());
    }

    @Test
    void shouldUseSpecifiedAgentRoleName()
    {
        context.clusterId(42).clusterMemberId(3).agentRoleName("test name");

        context.conclude();

        assertEquals("test name", context.agentRoleName());
    }

    @Test
    void shouldNotSetClientNameOnTheExplicitlyAssignedAeronClient()
    {
        context.agentRoleName("test");

        context.conclude();

        verify(context.aeron().context(), never()).clientName(anyString());
    }

    @Test
    void shouldUseExplicitlyAssignArchiveContext()
    {
        final AeronArchive.Context archiveContext = new AeronArchive.Context()
            .controlRequestChannel("aeron:ipc")
            .controlResponseChannel("aeron:ipc");
        context.archiveContext(archiveContext);
        assertSame(archiveContext, context.archiveContext());

        try
        {
            context.conclude();

            assertSame(archiveContext, context.archiveContext());
            assertSame(context.aeron(), archiveContext.aeron());
            assertFalse(archiveContext.ownsAeronClient());
            assertSame(context.countedErrorHandler(), archiveContext.errorHandler());
            assertSame(NoOpLock.INSTANCE, archiveContext.lock());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
        }
    }

    @Test
    void shouldCreateArchiveContextUsingLocalChannelConfiguration()
    {
        final String controlChannel = "aeron:ipc?alias=test";
        final int localControlStreamId = 8;
        System.setProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME, controlChannel);
        System.setProperty(
            AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME, Integer.toString(localControlStreamId));
        context.archiveContext(null);
        assertNull(context.archiveContext());

        try
        {
            context.conclude();

            final AeronArchive.Context archiveContext = context.archiveContext();
            assertNotNull(archiveContext);
            assertSame(context.aeron(), archiveContext.aeron());
            assertFalse(archiveContext.ownsAeronClient());
            assertSame(context.countedErrorHandler(), archiveContext.errorHandler());
            assertSame(NoOpLock.INSTANCE, archiveContext.lock());
            assertEquals(controlChannel, archiveContext.controlRequestChannel());
            assertEquals(controlChannel, archiveContext.controlResponseChannel());
            assertEquals(localControlStreamId, archiveContext.controlRequestStreamId());
            assertNotEquals(localControlStreamId, archiveContext.controlResponseStreamId());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME);
        }
    }

    @ParameterizedTest
    @CsvSource({ "19,20", "0,222" })
    void shouldCreateAliasForControlStreams(final int clusterId, final int controlResponseStreamId)
    {
        final String controlChannel = "aeron:ipc?term-length=64k";
        final int localControlStreamId = 10;
        System.setProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME, controlChannel);
        System.setProperty(
            AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME, Integer.toString(localControlStreamId));
        System.setProperty(
            AeronArchive.Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME, Integer.toString(controlResponseStreamId));
        context.archiveContext(null).clusterId(clusterId);
        assertNull(context.archiveContext());

        try
        {
            context.conclude();

            final AeronArchive.Context archiveContext = context.archiveContext();
            assertNotNull(archiveContext);
            assertThat(
                archiveContext.controlRequestChannel(),
                Matchers.containsString("alias=cm-archive-ctrl-req-cluster-" + clusterId));
            assertThat(
                archiveContext.controlResponseChannel(),
                Matchers.containsString("alias=cm-archive-ctrl-resp-cluster-" + clusterId));
            assertEquals(localControlStreamId, archiveContext.controlRequestStreamId());
            assertEquals(clusterId * 100 + 100 + controlResponseStreamId, archiveContext.controlResponseStreamId());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_CHANNEL_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.LOCAL_CONTROL_STREAM_ID_PROP_NAME);
            System.clearProperty(AeronArchive.Configuration.CONTROL_RESPONSE_STREAM_ID_PROP_NAME);
        }
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:ipc,aeron:ipc?term-length=64k|mtu=8k," +
            "aeron:ipc?alias=cm-archive-ctrl-req-cluster--65-member-0," +
            "aeron:ipc?term-length=64k|mtu=8k|alias=cm-archive-ctrl-resp-cluster--65-member-0",
        "aeron:ipc?alias=x,aeron:ipc?alias=y,aeron:ipc?alias=x,aeron:ipc?alias=y"
    })
    void shouldCreateAliasForControlStreamsEvenWhenArchiveContextAssignedExplicitly(
        final String controlRequestChannel,
        final String controlResponseChannel,
        final String expectedControlRequestChannel,
        final String expectedControlResponseChannel)
    {
        final AeronArchive.Context archiveContext = new AeronArchive.Context()
            .controlRequestChannel(controlRequestChannel)
            .controlResponseChannel(controlResponseChannel)
            .controlRequestStreamId(42)
            .controlResponseStreamId(18);
        context.archiveContext(archiveContext).clusterId(-65);

        try
        {
            context.conclude();

            assertEquals(
                ChannelUri.parse(archiveContext.controlRequestChannel()),
                ChannelUri.parse(expectedControlRequestChannel));
            assertEquals(
                ChannelUri.parse(archiveContext.controlResponseChannel()),
                ChannelUri.parse(expectedControlResponseChannel));
            assertEquals(42, archiveContext.controlRequestStreamId());
            assertEquals(18, archiveContext.controlResponseStreamId());
        }
        finally
        {
            CloseHelper.quietClose(context::close);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = { 8192, 32 * 1024 })
    void shouldAlignMarkFileToTheAeronClientFilePageSize(final int filePageSize)
    {
        final Aeron.Context aeronContext = context.aeron().context();
        when(aeronContext.filePageSize()).thenReturn(filePageSize);

        try
        {
            context.conclude();

            final File file = new File(context.markFileDir(), ClusterMarkFile.FILENAME);
            assertTrue(file.exists());
            assertEquals(BitUtil.align(context.errorBufferLength() + HEADER_LENGTH, filePageSize), file.length());

            verify(aeronContext).filePageSize();
        }
        finally
        {
            context.close();
        }
    }

    @Test
    void shouldAlignMarkFileBasedOnTheMediaDriverFilePageSize() throws IOException
    {
        final Path aeronDir = Paths.get(CommonContext.generateRandomDirName());
        Files.createDirectories(aeronDir);

        final int filePageSize = 1024 * 1024;
        try (MediaDriver driver = MediaDriver.launch(new MediaDriver.Context()
            .aeronDirectoryName(aeronDir.toString())
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .filePageSize(filePageSize)))
        {
            final ConsensusModule.Context ctx = TestContexts.localhostConsensusModule()
                .clusterDir(clusterDir)
                .aeronDirectoryName(driver.aeronDirectoryName())
                .ingressChannel("aeron:ipc")
                .egressChannel("aeron:udp?endpoint=localhost:0")
                .replicationChannel("aeron:udp?endpoint=localhost:0")
                .errorBufferLength(1919191);
            try
            {
                ctx.conclude();

                final File file = new File(ctx.markFileDir(), ClusterMarkFile.FILENAME);
                assertTrue(file.exists());
                assertEquals(BitUtil.align(context.errorBufferLength() + HEADER_LENGTH, filePageSize), file.length());
            }
            finally
            {
                ctx.close();
            }
        }
    }

    public static class TestAuthorisationSupplier implements AuthorisationServiceSupplier
    {
        public AuthorisationService get()
        {
            return new TestAuthorisationService();
        }
    }

    static class TestAuthorisationService implements AuthorisationService
    {
        public boolean isAuthorised(
            final int protocolId, final int actionId, final Object type, final byte[] encodedPrincipal)
        {
            return false;
        }
    }

    public static class TestAuthenticatorSupplier implements AuthenticatorSupplier
    {
        public Authenticator get()
        {
            return new TestAuthenticator();
        }
    }

    static class TestAuthenticator implements Authenticator
    {
        public void onConnectRequest(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
        }

        public void onChallengeResponse(final long sessionId, final byte[] encodedCredentials, final long nowMs)
        {
        }

        public void onConnectedSession(final SessionProxy sessionProxy, final long nowMs)
        {
        }

        public void onChallengedSession(final SessionProxy sessionProxy, final long nowMs)
        {
        }
    }
}
