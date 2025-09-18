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
package io.aeron.test.driver;

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.CommonContext;
import io.aeron.driver.*;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.test.SystemTestConfig;
import io.aeron.test.Tests;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.SystemUtil;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.Object2ObjectHashMap;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class CTestMediaDriver implements TestMediaDriver
{
    private static final String UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR = "AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS";
    private static final String UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR = "AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS";

    private static final File NULL_FILE = SystemUtil.isWindows() ? new File("NUL") : new File("/dev/null");
    private static final Map<Class<? extends FlowControlSupplier>, String> C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE =
        new IdentityHashMap<>();
    private static final ThreadLocal<Map<MediaDriver.Context, Map<String, String>>> C_DRIVER_ADDITIONAL_ENV_VARS =
        ThreadLocal.withInitial(IdentityHashMap::new);
    private static final Collection<String> JOINABLE_ENV_VARS = Arrays.asList(
        UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR, UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR);

    static
    {
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            DefaultMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            MaxMulticastFlowControlSupplier.class, "aeron_max_multicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            MinMulticastFlowControlSupplier.class, "aeron_min_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            DefaultUnicastFlowControlSupplier.class, "aeron_unicast_flow_control_strategy_supplier");
        C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.put(
            TaggedMulticastFlowControlSupplier.class, "aeron_tagged_flow_control_strategy_supplier");
    }

    private final Process aeronMediaDriverProcess;
    private final MediaDriver.Context context;
    private final DriverOutputConsumer driverOutputConsumer;
    private final File stdoutFile;
    private final File stderrFile;
    private Aeron.Context aeronContext;
    private CountersReader countersReader;
    private boolean isClosed = false;

    private CTestMediaDriver(
        final Process aeronMediaDriverProcess,
        final MediaDriver.Context context,
        final DriverOutputConsumer driverOutputConsumer,
        final File stdoutFile,
        final File stderrFile)
    {
        this.aeronMediaDriverProcess = aeronMediaDriverProcess;
        this.context = context;
        this.driverOutputConsumer = driverOutputConsumer;
        this.stdoutFile = stdoutFile;
        this.stderrFile = stderrFile;
    }

    public void close()
    {
        if (isClosed)
        {
            return;
        }

        awaitSendersAndReceiversClosed();

        isClosed = true;

        Exception error = null;
        try
        {
            if (null != aeronContext)
            {
                aeronContext.close();
            }
        }
        catch (final Exception ex)
        {
            error = ex;
        }

        try
        {
            final ExitStatus exitStatus = terminateDriver();
            if (null != driverOutputConsumer)
            {
                driverOutputConsumer.exitCode(
                    context.aeronDirectoryName(), exitStatus.exitCode, exitStatus.exitMessage);
            }
        }
        catch (final Exception ex)
        {
            if (null == error)
            {
                error = ex;
            }
            else
            {
                error.addSuppressed(ex);
            }
        }

        if (null != error)
        {
            LangUtil.rethrowUnchecked(error);
        }
    }

    private void awaitSendersAndReceiversClosed()
    {
        if (!SystemTestConfig.DRIVER_AWAIT_COUNTER_CLOSE)
        {
            return;
        }

        final MutableInteger counterCount = new MutableInteger();
        final CountersReader.MetaData metaData = (counterId, typeId, keyBuffer, label) ->
        {
            if (AeronCounters.DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID == typeId ||
                AeronCounters.DRIVER_SEND_CHANNEL_STATUS_TYPE_ID == typeId)
            {
                counterCount.increment();
            }
        };

        final long deadlineMs = System.currentTimeMillis() + 15_000;
        do
        {
            counterCount.set(0);
            counters().forEach(metaData);

            Tests.checkInterruptStatus();
            Tests.yield();
        }
        while (0 != counterCount.get() && System.currentTimeMillis() < deadlineMs);
    }

    public void cleanup()
    {
        if (NULL_FILE != stdoutFile)
        {
            IoUtil.delete(stdoutFile, true);
        }

        if (NULL_FILE != stderrFile)
        {
            IoUtil.delete(stderrFile, true);
        }
    }

    public CountersReader counters()
    {
        if (null == countersReader)
        {
            aeronContext = new Aeron.Context()
                .aeronDirectoryName(context.aeronDirectoryName())
                .keepAliveIntervalNs(TimeUnit.MILLISECONDS.toNanos(100))
                .conclude();
            countersReader = new CountersReader(
                aeronContext.countersMetaDataBuffer(), aeronContext.countersValuesBuffer(), StandardCharsets.US_ASCII);
        }

        return countersReader;
    }

    @SuppressWarnings("methodlength")
    public static CTestMediaDriver launch(
        final MediaDriver.Context context,
        final boolean withAeronDir,
        final DriverOutputConsumer driverOutputConsumer)
    {
        final String aeronMediaDriverPath = System.getProperty(TestMediaDriver.AERONMD_PATH_PROP_NAME);
        final File aeronBinary = new File(aeronMediaDriverPath);

        if (!aeronBinary.exists())
        {
            throw new RuntimeException("Unable to find native media driver binary: " + aeronBinary.getAbsolutePath());
        }

        context.concludeAeronDirectory();
        IoUtil.ensureDirectoryExists(context.aeronDirectory().getParentFile(), "Aeron C Media Driver directory");

        final HashMap<String, String> environment = new HashMap<>();

        if (withAeronDir)
        {
            environment.put("AERON_DIR", context.aeronDirectoryName());
        }
        environment.put("AERON_CLIENT_LIVENESS_TIMEOUT", String.valueOf(context.clientLivenessTimeoutNs()));
        environment.put("AERON_IMAGE_LIVENESS_TIMEOUT", String.valueOf(context.imageLivenessTimeoutNs()));
        environment.put("AERON_DRIVER_TERMINATION_VALIDATOR", "allow");
        environment.put("AERON_DIR_DELETE_ON_START", Boolean.toString(context.dirDeleteOnStart()));
        environment.put("AERON_DIR_DELETE_ON_SHUTDOWN", Boolean.toString(context.dirDeleteOnShutdown()));
        environment.put("AERON_TERM_BUFFER_SPARSE_FILE", Boolean.toString(context.termBufferSparseFile()));
        environment.put("AERON_TERM_BUFFER_LENGTH", String.valueOf(context.publicationTermBufferLength()));
        environment.put("AERON_IPC_TERM_BUFFER_LENGTH", String.valueOf(context.ipcTermBufferLength()));
        environment.put("AERON_FILE_PAGE_SIZE", String.valueOf(context.filePageSize()));
        environment.put(
            "AERON_PUBLICATION_UNBLOCK_TIMEOUT", String.valueOf(context.publicationUnblockTimeoutNs()));
        environment.put(
            "AERON_PUBLICATION_CONNECTION_TIMEOUT", String.valueOf(context.publicationConnectionTimeoutNs()));
        environment.put("AERON_PUBLICATION_LINGER_TIMEOUT", String.valueOf(context.publicationLingerTimeoutNs()));
        environment.put("AERON_SPIES_SIMULATE_CONNECTION", Boolean.toString(context.spiesSimulateConnection()));
        environment.put("AERON_RCV_STATUS_MESSAGE_TIMEOUT", String.valueOf(context.statusMessageTimeoutNs()));
        environment.put("AERON_PERFORM_STORAGE_CHECKS", Boolean.toString(context.performStorageChecks()));
        if (null != context.threadingMode())
        {
            environment.put("AERON_THREADING_MODE", context.threadingMode().name());
        }
        environment.put("AERON_TIMER_INTERVAL", String.valueOf(context.timerIntervalNs()));
        environment.put(
            "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT", String.valueOf(context.untetheredWindowLimitTimeoutNs()));
        final long untetheredLingerTimeoutNs = context.untetheredLingerTimeoutNs();
        if (Aeron.NULL_VALUE != untetheredLingerTimeoutNs)
        {
            environment.put("AERON_UNTETHERED_LINGER_TIMEOUT", String.valueOf(untetheredLingerTimeoutNs));
        }
        environment.put("AERON_UNTETHERED_RESTING_TIMEOUT", String.valueOf(context.untetheredRestingTimeoutNs()));

        if (null != context.receiverGroupTag())
        {
            environment.put("AERON_RECEIVER_GROUP_TAG", context.receiverGroupTag().toString());
        }
        environment.put("AERON_FLOW_CONTROL_GROUP_TAG", String.valueOf(context.flowControlGroupTag()));
        environment.put(
            "AERON_FLOW_CONTROL_GROUP_MIN_SIZE", String.valueOf(context.flowControlGroupMinSize()));
        environment.put("AERON_PRINT_CONFIGURATION", "true");

        if (null != context.resolverName())
        {
            environment.put("AERON_DRIVER_RESOLVER_NAME", context.resolverName());
        }
        if (null != context.resolverInterface())
        {
            environment.put("AERON_DRIVER_RESOLVER_INTERFACE", context.resolverInterface());
            environment.put("AERON_NAME_RESOLVER_SUPPLIER", "driver");
        }
        if (null != context.resolverBootstrapNeighbor())
        {
            environment.put("AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR", context.resolverBootstrapNeighbor());
        }
        environment.put("AERON_SOCKET_SO_RCVBUF", String.valueOf(context.socketRcvbufLength()));
        environment.put("AERON_SOCKET_SO_SNDBUF", String.valueOf(context.socketSndbufLength()));
        environment.put("AERON_RCV_INITIAL_WINDOW_LENGTH", String.valueOf(context.initialWindowLength()));
        environment.put("AERON_PUBLICATION_UNBLOCK_TIMEOUT", String.valueOf(context.publicationUnblockTimeoutNs()));
        final NameResolver nameResolver = context.nameResolver();
        if (nameResolver instanceof RedirectingNameResolver)
        {
            final String csvConfiguration = ((RedirectingNameResolver)nameResolver).csvConfiguration();
            environment.put("AERON_NAME_RESOLVER_SUPPLIER", "csv_table");
            environment.put("AERON_NAME_RESOLVER_INIT_ARGS", csvConfiguration);
        }
        environment.put("AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD", String.valueOf(context.conductorCycleThresholdNs()));
        environment.put("AERON_DRIVER_SENDER_CYCLE_THRESHOLD", String.valueOf(context.senderCycleThresholdNs()));
        environment.put("AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD", String.valueOf(context.receiverCycleThresholdNs()));
        environment.put("AERON_DRIVER_NAME_RESOLVER_THRESHOLD", String.valueOf(context.nameResolverThresholdNs()));
        environment.put("AERON_DRIVER_ASYNC_EXECUTOR_THREADS", String.valueOf(context.asyncTaskExecutorThreads()));
        final String senderWildcardPortRange = context.senderWildcardPortRange();
        if (null != senderWildcardPortRange)
        {
            environment.put("AERON_SENDER_WILDCARD_PORT_RANGE", senderWildcardPortRange);
        }
        final String receiverWildcardPortRange = context.receiverWildcardPortRange();
        if (null != receiverWildcardPortRange)
        {
            environment.put("AERON_RECEIVER_WILDCARD_PORT_RANGE", receiverWildcardPortRange);
        }

        environment.put("AERON_ENABLE_EXPERIMENTAL_FEATURES", String.valueOf(context.enableExperimentalFeatures()));

        environment.put("AERON_DRIVER_STREAM_SESSION_LIMIT", String.valueOf(context.streamSessionLimit()));

        setFlowControlStrategy(environment, context);
        setLogging(environment);
        setTransportSecurity(environment);
        setAdditionalEnvVars(environment, getAdditionalEnvVarsMap(context));

        try
        {
            File stdoutFile = NULL_FILE;
            File stderrFile = NULL_FILE;

            final ProcessBuilder pb = new ProcessBuilder(aeronBinary.getAbsolutePath());
            if (null != driverOutputConsumer)
            {
                stdoutFile = Files.createTempFile(context.aeronDirectory().getName() + "-driver-", ".out").toFile();
                final String tmpName = stdoutFile.getName().substring(0, stdoutFile.getName().length() - 4) + ".err";
                stderrFile = new File(stdoutFile.getParent(), tmpName);
                driverOutputConsumer.outputFiles(context.aeronDirectoryName(), stdoutFile, stderrFile);
                driverOutputConsumer.environmentVariables(context.aeronDirectoryName(), environment);
            }

            pb.environment().putAll(environment);
            pb.redirectOutput(stdoutFile).redirectError(stderrFile);
            final Process process = pb.start();
            Thread.yield();

            return new CTestMediaDriver(process, context, driverOutputConsumer, stdoutFile, stderrFile);
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
            return null;
        }
    }

    public MediaDriver.Context context()
    {
        return context;
    }

    public String aeronDirectoryName()
    {
        return context.aeronDirectoryName();
    }

    public AgentInvoker sharedAgentInvoker()
    {
        throw new UnsupportedOperationException("Not supported in C media driver");
    }

    public static void enableRandomLossOnReceive(
        final MediaDriver.Context context,
        final double rate,
        final long seed,
        final boolean loseDataMessages,
        final boolean loseControlMessages)
    {
        int receiveMessageTypeMask = 0;
        receiveMessageTypeMask |= loseDataMessages ? 1 << HeaderFlyweight.HDR_TYPE_DATA : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_SM : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_NAK : 0;
        receiveMessageTypeMask |= loseControlMessages ? 1 << HeaderFlyweight.HDR_TYPE_RTTM : 0;

        final Object2ObjectHashMap<String, String> lossTransportEnv = new Object2ObjectHashMap<>();

        final String interceptor = "loss";
        final String lossArgs = "rate=" + rate +
            "|seed=" + seed +
            "|recv-msg-mask=0x" + Integer.toHexString(receiveMessageTypeMask);

        lossTransportEnv.put(UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR, interceptor);
        lossTransportEnv.put("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_LOSS_ARGS", lossArgs);

        getAdditionalEnvVarsMap(context).putAll(lossTransportEnv);
    }

    public static void enableFixedLossOnReceive(
        final MediaDriver.Context context,
        final int termId,
        final int termOffset,
        final int length)
    {
        final Object2ObjectHashMap<String, String> fixedLossTransportEnv = new Object2ObjectHashMap<>();

        final String interceptor = "fixed-loss";
        final String fixedLossArgs = "term-id=" + termId +
            "|term-offset=" + termOffset +
            "|length=" + length;

        fixedLossTransportEnv.put(UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR, interceptor);
        fixedLossTransportEnv.put("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_FIXED_LOSS_ARGS", fixedLossArgs);

        getAdditionalEnvVarsMap(context).putAll(fixedLossTransportEnv);
    }

    public static void enableMultiGapLossOnReceive(
        final MediaDriver.Context context,
        final int termId,
        final int gapRadix,
        final int gapLength,
        final int totalGaps)
    {
        final Object2ObjectHashMap<String, String> multiGapLossTransportEnv = new Object2ObjectHashMap<>();

        final String interceptor = "multi-gap-loss";
        final String multiGapLossArgs = "term-id=" + termId +
            "|gap-radix=" + gapRadix +
            "|gap-length=" + gapLength +
            "|total-gaps=" + totalGaps;

        multiGapLossTransportEnv.put(UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR, interceptor);
        multiGapLossTransportEnv.put("AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MULTI_GAP_LOSS_ARGS", multiGapLossArgs);

        getAdditionalEnvVarsMap(context).putAll(multiGapLossTransportEnv);
    }

    public static void dontCoalesceNaksOnReceiverByDefault(final MediaDriver.Context context)
    {
        getAdditionalEnvVarsMap(context).put("AERON_NAK_UNICAST_DELAY", "0");
    }

    public static Map<String, String> getAdditionalEnvVarsMap(final MediaDriver.Context context)
    {
        return C_DRIVER_ADDITIONAL_ENV_VARS.get().computeIfAbsent(context, c -> new IdentityHashMap<>());
    }

    private static void setLogging(final Map<String, String> environment)
    {
        environment.put("AERON_EVENT_LOG", System.getProperty(
            "aeron.event.log",
            "admin"));
        environment.put("AERON_EVENT_LOG_DISABLE", System.getProperty("aeron.event.log.disable", ""));

        final String driverAgentPath = System.getProperty(DRIVER_AGENT_PATH_PROP_NAME);
        if (null == driverAgentPath)
        {
            return;
        }

        final File driverAgent = new File(driverAgentPath);
        if (!driverAgent.exists())
        {
            throw new RuntimeException(
                "Unable to find driver agent file at: " + DRIVER_AGENT_PATH_PROP_NAME + "=" + driverAgentPath);
        }
    }

    private static void setTransportSecurity(final HashMap<String, String> environment)
    {
        final String atsLibPath = (String)System.getProperties().get(ATS_LIBRARY_PATH_PROP_NAME);
        if (null != atsLibPath && !atsLibPath.isEmpty())
        {
            IoUtil.checkFileExists(new File(atsLibPath), ATS_LIBRARY_PATH_PROP_NAME);

            environment.put("AERON_DRIVER_DYNAMIC_LIBRARIES", atsLibPath);
            environment.put(
                UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR, "aeron_transport_security_channel_interceptor_load");
            environment.put(
                UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR, "aeron_transport_security_channel_interceptor_load");

            final String atsConfDir = System.getProperty(ATS_LIBRARY_CONF_PATH_PROP_NAME);
            if (null != atsConfDir)
            {
                environment.put("AERON_TRANSPORT_SECURITY_CONF_DIR", atsConfDir);
            }
            final String atsConfFile = System.getProperty(ATS_LIBRARY_CONF_FILE_PROP_NAME);
            if (null != atsConfFile)
            {
                environment.put("AERON_TRANSPORT_SECURITY_CONF_FILE", atsConfFile);
            }
        }
    }

    private static void setAdditionalEnvVars(
        final HashMap<String, String> environment,
        final Map<String, String> additionalEnvVars)
    {
        additionalEnvVars.forEach(
            (k, v) ->
            {
                final String existingValue = environment.putIfAbsent(k, v);
                if (null != existingValue)
                {
                    if (JOINABLE_ENV_VARS.contains(k))
                    {
                        environment.put(k, existingValue + "," + v);
                    }
                    else
                    {
                        throw new RuntimeException(
                            "Variable: " + k + " is already specified as: " + existingValue + " cannot set to: " + v);
                    }
                }
            });
    }

    private static void setFlowControlStrategy(final Map<String, String> environment, final MediaDriver.Context context)
    {
        final FlowControlSupplier multicastFlowControlSupplier = context.multicastFlowControlSupplier();
        final String multicastFlowControlStrategyName = getFlowControlStrategyName(multicastFlowControlSupplier);
        if (null != multicastFlowControlStrategyName)
        {
            environment.put("AERON_MULTICAST_FLOWCONTROL_SUPPLIER", multicastFlowControlStrategyName);
        }
        else if (null != multicastFlowControlSupplier)
        {
            throw new RuntimeException("No equivalent C multicast flow control strategy for: " +
                multicastFlowControlSupplier.getClass());
        }

        final FlowControlSupplier unicastFlowControlSupplier = context.unicastFlowControlSupplier();
        final String unicastFlowControlStrategyName = getFlowControlStrategyName(unicastFlowControlSupplier);
        if (null != unicastFlowControlStrategyName)
        {
            environment.put("AERON_UNICAST_FLOWCONTROL_SUPPLIER", unicastFlowControlStrategyName);
        }
        else if (null != unicastFlowControlSupplier)
        {
            throw new RuntimeException("No equivalent C unicast flow control strategy for: " +
                unicastFlowControlSupplier.getClass());
        }
    }

    private static String getFlowControlStrategyName(final FlowControlSupplier flowControlSupplier)
    {
        return null == flowControlSupplier ?
            null : C_DRIVER_FLOW_CONTROL_STRATEGY_NAME_BY_TYPE.get(flowControlSupplier.getClass());
    }

    private static final class ExitStatus
    {
        private final int exitCode;
        private final String exitMessage;

        private ExitStatus(final int exitCode, final String exitMessage)
        {
            this.exitCode = exitCode;
            this.exitMessage = exitMessage;
        }
    }

    private ExitStatus terminateDriver()
    {
        boolean isInterrupted = false;
        boolean requestTermination = true;
        try
        {
            try
            {
                final int exitCode = aeronMediaDriverProcess.exitValue();
                String exitMessage = "Process exited early";
                if (!SystemUtil.isWindows())
                {
                    final int exitSignal = (0x7F & exitCode); // Essentially the same on Linux and macOS.
                    if (0 != exitSignal)
                    {
                        exitMessage += " - signal " + exitSignal;
                    }
                }
                return new ExitStatus(exitCode, exitMessage);
            }
            catch (final IllegalThreadStateException ignore)
            {
            }

            while (true)
            {
                isInterrupted |= Thread.interrupted();
                try
                {
                    if (requestTermination)
                    {
                        requestTermination = false;
                        if (requestDriverTermination() && aeronMediaDriverProcess.waitFor(10, TimeUnit.SECONDS))
                        {
                            return new ExitStatus(aeronMediaDriverProcess.exitValue(), "Process shutdown cleanly");
                        }
                    }

                    aeronMediaDriverProcess.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                    final int exitCode = aeronMediaDriverProcess.exitValue();
                    return new ExitStatus(exitCode, "Process destroyed forcibly");
                }
                catch (final InterruptedException ex)
                {
                    isInterrupted = true;
                }
            }
        }
        finally
        {
            if (isInterrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private boolean requestDriverTermination()
    {
        try
        {
            return CommonContext.requestDriverTermination(new File(context.aeronDirectoryName()), null, 0, 0);
        }
        catch (final Exception ex)
        {
            return false;
        }
    }
}
