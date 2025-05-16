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

import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MarkFile;
import org.agrona.SemanticVersion;
import org.agrona.SystemUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.errors.DistinctErrorLog;
import org.agrona.concurrent.errors.ErrorConsumer;
import org.agrona.concurrent.errors.ErrorLogReader;
import org.agrona.concurrent.errors.LoggingErrorHandler;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.NoSuchFileException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.aeron.CncFileDescriptor.CNC_FILE;
import static io.aeron.CncFileDescriptor.cncVersionOffset;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 * This class provides the Media Driver and client with common configuration for the Aeron directory.
 * <p>
 * This class should have {@link #conclude()} called before the methods are used or at least
 * {@link #concludeAeronDirectory()} to avoid NPEs.
 * <p>
 * Properties:
 * <ul>
 * <li><code>aeron.dir</code>: Use value as directory name for Aeron buffers and status.</li>
 * </ul>
 */
public class CommonContext implements Cloneable
{
    /**
     * Condition to specify a triple state conditional of always override to be true, always override to be false,
     * or infer value.
     */
    public enum InferableBoolean
    {
        /**
         * Force the conditional to be false.
         */
        FORCE_FALSE,

        /**
         * Force the conditional to be true.
         */
        FORCE_TRUE,

        /**
         * Try to infer if true or false is most appropriate.
         */
        INFER;

        /**
         * Parse the string looking for {@code true}, {@code false}, or {@code infer}.
         *
         * @param value to be parsed.
         * @return {@link InferableBoolean} which matches the string.
         */
        public static InferableBoolean parse(final String value)
        {
            if (null == value || "infer".equals(value))
            {
                return INFER;
            }

            return "true".equals(value) ? FORCE_TRUE : FORCE_FALSE;
        }
    }

    /**
     * Should a component print its configuration on start to {@link System#out}.
     */
    @Config(
        expectedCEnvVarFieldName = "AERON_PRINT_CONFIGURATION_ON_START_ENV_VAR",
        defaultType = DefaultType.BOOLEAN,
        defaultBoolean = false)
    public static final String PRINT_CONFIGURATION_ON_START_PROP_NAME = "aeron.print.configuration";

    /**
     * Property name for driver timeout after which the driver is considered inactive.
     */
    @Config
    public static final String DRIVER_TIMEOUT_PROP_NAME = "aeron.driver.timeout";

    /**
     * Property name for the timeout to use in debug mode. By default, this is not set and the configured
     * timeouts will be used. Setting this value adjusts timeouts to make debugging easier.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 0, existsInC = false)
    public static final String DEBUG_TIMEOUT_PROP_NAME = "aeron.debug.timeout";

    /**
     * Default timeout in which the driver is expected to respond or heartbeat.
     */
    @Config(
        id = "DRIVER_TIMEOUT",
        timeUnit = TimeUnit.MILLISECONDS,
        expectedCDefaultFieldName = "AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT")
    public static final long DEFAULT_DRIVER_TIMEOUT_MS = 10_000;

    /**
     * Timeout in which the driver is expected to respond or heartbeat.
     */
    public static final long DRIVER_TIMEOUT_MS = getLong(DRIVER_TIMEOUT_PROP_NAME, DEFAULT_DRIVER_TIMEOUT_MS);

    /**
     * Value to represent a sessionId that is not to be used.
     */
    public static final int NULL_SESSION_ID = Aeron.NULL_VALUE;

    /**
     * The top level Aeron directory used for communication between a Media Driver and client.
     */
    @Config(skipCDefaultValidation = true)
    public static final String AERON_DIR_PROP_NAME = "aeron.dir";

    /**
     * The value of the top level Aeron directory unless overridden by {@link #aeronDirectoryName(String)}.
     */
    @Config(id = "AERON_DIR", defaultType = DefaultType.STRING, defaultString = "OS specific")
    public static final String AERON_DIR_PROP_DEFAULT;

    /**
     * Should new/experimental features be enabled.
     *
     * @since 1.44.0
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false, existsInC = false)
    public static final String ENABLE_EXPERIMENTAL_FEATURES_PROP_NAME = "aeron.enable.experimental.features";

    /**
     * Property name for a fallback {@link PrintStream} based logger when it is not possible to use the error logging
     * callback. Supported values are stdout, stderr, no_op (stderr is the default).
     */
    @Config(defaultType = DefaultType.STRING, defaultString = "stderr", existsInC = false)
    public static final String FALLBACK_LOGGER_PROP_NAME = "aeron.fallback.logger";

    /**
     * Media type used for IPC shared memory from {@link Publication} to {@link Subscription} channels.
     */
    public static final String IPC_MEDIA = "ipc";

    /**
     * Media type used for UDP sockets from {@link Publication} to {@link Subscription} channels.
     */
    public static final String UDP_MEDIA = "udp";

    /**
     * URI base used for IPC channels for {@link Publication}s and {@link Subscription}s.
     */
    public static final String IPC_CHANNEL = "aeron:ipc";

    /**
     * URI base used for UDP channels for {@link Publication}s and {@link Subscription}s.
     */
    public static final String UDP_CHANNEL = "aeron:udp";

    /**
     * URI used for Spy {@link Subscription}s whereby an outgoing unicast or multicast publication can be spied on
     * by IPC without receiving it again via the network.
     */
    public static final String SPY_PREFIX = "aeron-spy:";

    /**
     * The address and port used for a UDP channel. For the publisher it is the socket to send to,
     * for the subscriber it is the socket to receive from.
     */
    public static final String ENDPOINT_PARAM_NAME = "endpoint";

    /**
     * The network interface via which the socket will be routed.
     */
    public static final String INTERFACE_PARAM_NAME = "interface";

    /**
     * Initial term id to be used when creating an {@link ExclusivePublication}.
     */
    public static final String INITIAL_TERM_ID_PARAM_NAME = "init-term-id";

    /**
     * Current term id to be used when creating an {@link ExclusivePublication}.
     */
    public static final String TERM_ID_PARAM_NAME = "term-id";

    /**
     * Current term offset to be used when creating an {@link ExclusivePublication}.
     */
    public static final String TERM_OFFSET_PARAM_NAME = "term-offset";

    /**
     * The param name to be used for the term length as a channel URI param.
     */
    public static final String TERM_LENGTH_PARAM_NAME = "term-length";

    /**
     * MTU length parameter name for using as a channel URI param. If this is greater than the network MTU for UDP
     * then the packet will be fragmented and can amplify the impact of loss.
     */
    public static final String MTU_LENGTH_PARAM_NAME = "mtu";

    /**
     * Time To Live param for a multicast datagram.
     */
    public static final String TTL_PARAM_NAME = "ttl";

    /**
     * The param for the control channel IP address and port for multi-destination-cast semantics.
     */
    public static final String MDC_CONTROL_PARAM_NAME = "control";

    /**
     * Key for the mode of control that such be used for multi-destination-cast semantics.
     */
    public static final String MDC_CONTROL_MODE_PARAM_NAME = "control-mode";

    /**
     * Valid value for {@link #MDC_CONTROL_MODE_PARAM_NAME} when manual control is desired.
     */
    public static final String MDC_CONTROL_MODE_MANUAL = "manual";

    /**
     * Valid value for {@link #MDC_CONTROL_MODE_PARAM_NAME} when dynamic control is desired. Default value.
     */
    public static final String MDC_CONTROL_MODE_DYNAMIC = "dynamic";

    /**
     * Valid value for {@link #MDC_CONTROL_MODE_PARAM_NAME} when response control is desired.
     */
    public static final String CONTROL_MODE_RESPONSE = "response";

    /**
     * Key for the session id for a publication or restricted subscription.
     */
    public static final String SESSION_ID_PARAM_NAME = "session-id";

    /**
     * Key for timeout a publication to linger after draining in nanoseconds.
     */
    public static final String LINGER_PARAM_NAME = "linger";

    /**
     * Parameter name for channel URI param to indicate if a subscribed stream must be reliable or not.
     * Value is boolean with true to recover loss and false to gap fill.
     */
    public static final String RELIABLE_STREAM_PARAM_NAME = "reliable";

    /**
     * Key for the tags for a channel.
     */
    public static final String TAGS_PARAM_NAME = "tags";

    /**
     * Qualifier for a value which is a tag for reference. This prefix is use in the param value.
     */
    public static final String TAG_PREFIX = "tag:";

    /**
     * Parameter name for channel URI param to indicate if term buffers should be sparse. Value is boolean.
     */
    public static final String SPARSE_PARAM_NAME = "sparse";

    /**
     * Parameter name for channel URI param to indicate an alias for the given URI. Value not interpreted by Aeron.
     * <p>
     * This is a reserved application level param used to identify a particular channel for application purposes.
     */
    public static final String ALIAS_PARAM_NAME = "alias";

    /**
     * Parameter name for channel URI param to indicate if End of Stream (EOS) should be sent or not. Value is boolean.
     */
    public static final String EOS_PARAM_NAME = "eos";

    /**
     * Parameter name for channel URI param to indicate if a subscription should tether for local flow control.
     * Value is boolean. A tether only applies when there is more than one matching active subscription. If tether is
     * true then that subscription is included in flow control. If only one subscription then it tethers pace.
     */
    public static final String TETHER_PARAM_NAME = "tether";

    /**
     * Parameter name for channel URI param to indicate if a Subscription represents a group member or individual
     * from the perspective of message reception. This can inform loss handling and similar semantics.
     * <p>
     * When configuring a subscription for an MDC publication then should be added as this is effective multicast.
     *
     * @see CommonContext#MDC_CONTROL_MODE_PARAM_NAME
     * @see CommonContext#MDC_CONTROL_PARAM_NAME
     */
    public static final String GROUP_PARAM_NAME = "group";

    /**
     * Parameter name for Subscription URI param to indicate if Images that go unavailable should be allowed to
     * rejoin after a short cooldown or not.
     */
    public static final String REJOIN_PARAM_NAME = "rejoin";

    /**
     * Parameter name for Subscription URI param to indicate the congestion control algorithm to be used.
     * Options include {@code static} and {@code cubic}.
     */
    public static final String CONGESTION_CONTROL_PARAM_NAME = "cc";

    /**
     * Parameter name for Publication URI param to indicate the flow control strategy to be used.
     * Options include {@code min}, {@code max}, and {@code pref}.
     */
    public static final String FLOW_CONTROL_PARAM_NAME = "fc";

    /**
     * Parameter name for Subscription URI param to indicate the receiver tag to be sent in SMs.
     */
    public static final String GROUP_TAG_PARAM_NAME = "gtag";

    /**
     * Parameter name for Publication URI param to indicate whether spy subscriptions should simulate a connection.
     */
    public static final String SPIES_SIMULATE_CONNECTION_PARAM_NAME = "ssc";

    /**
     * Parameter name for the underlying OS socket send buffer length.
     */
    public static final String SOCKET_SNDBUF_PARAM_NAME = "so-sndbuf";

    /**
     * Parameter name for the underlying OS socket receive buffer length.
     */
    public static final String SOCKET_RCVBUF_PARAM_NAME = "so-rcvbuf";

    /**
     * Parameter name for the congestion control's initial receiver window length.
     */
    public static final String RECEIVER_WINDOW_LENGTH_PARAM_NAME = "rcv-wnd";

    /**
     * Parameter name of the offset for the media receive timestamp to be inserted into the incoming message on a
     * subscription. The special value of 'reserved' can be used to insert into the reserved value field. Media
     * receive timestamp is taken as the earliest possible point after the packet is received from the network. This
     * is only supported in the C media driver, the Java Media Driver will generate an error if used.
     */
    public static final String MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME = "media-rcv-ts-offset";

    /**
     * Parameter name of the offset for the channel receive timestamp to be inserted into the incoming message on a
     * subscription. The special value of 'reserved' can be used to insert into the reserved value field. Channel
     * receive timestamp is taken as soon a possible after the packet is received by Aeron receiver from the transport
     * bindings.
     */
    public static final String CHANNEL_RECEIVE_TIMESTAMP_OFFSET_PARAM_NAME = "channel-rcv-ts-offset";

    /**
     * Parameter name of the offset for the channel send timestamp to be inserted into the outgoing message
     * on a publication. The special value of 'reserved' can be used to insert into the reserved value
     * field. Channel send timestamp is taken shortly before passing the message over to the configured transport
     * bindings.
     */
    public static final String CHANNEL_SEND_TIMESTAMP_OFFSET_PARAM_NAME = "channel-snd-ts-offset";

    /**
     * Placeholder value to use in URIs to specify that a timestamp should be stored in the reserved value field.
     */
    public static final String RESERVED_OFFSET = "reserved";

    /**
     * Parameter name for the field that will be used to specify the response endpoint on a subscription and publication
     * used in a response "server".
     *
     * @since 1.44.0
     */
    public static final String RESPONSE_ENDPOINT_PARAM_NAME = "response-endpoint";

    /**
     * Parameter name for the field that will be used to specify the correlation id used on a publication to connect it
     * to a subscription's image in order to set up a response stream.
     *
     * @since 1.44.0
     */
    public static final String RESPONSE_CORRELATION_ID_PARAM_NAME = "response-correlation-id";

    /**
     * Parameter name to set explicit NAK delay (e.g. {@code nak-delay=200ms}).
     *
     * @since 1.44.0
     */
    public static final String NAK_DELAY_PARAM_NAME = "nak-delay";

    /**
     * Parameter name to set explicit untethered window limit timeout, e.g. {@code untethered-window-limit-timeout=10s}.
     *
     * @since 1.45.0
     */
    public static final String UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME = "untethered-window-limit-timeout";

    /**
     * Parameter name to set explicit untethered linger timeout, e.g. {@code untethered-linger-timeout=10s}.
     *
     * @since 1.48.0
     */
    public static final String UNTETHERED_LINGER_TIMEOUT_PARAM_NAME = "untethered-linger-timeout";

    /**
     * Parameter name to set explicit untethered resting timeout, e.g. {@code untethered-resting-timeout=10s}.
     *
     * @since 1.45.0
     */
    public static final String UNTETHERED_RESTING_TIMEOUT_PARAM_NAME = "untethered-resting-timeout";

    /**
     * Parameter name to set the max number of outstanding active retransmits for a publication.
     *
     * @since 1.45.0
     */
    public static final String MAX_RESEND_PARAM_NAME = "max-resend";

    /**
     * Parameter name to set the stream id for the channel.
     *
     * @since 1.47.0
     */
    public static final String STREAM_ID_PARAM_NAME = "stream-id";

    /**
     * Parameter name for the publication window length, i.e. how far ahead can publication accept offers.
     *
     * @since 1.47.0
     */
    public static final String PUBLICATION_WINDOW_LENGTH_PARAM_NAME = "pub-wnd";

    /**
     * Property name to use to set the secure random algorithm to be used by the Aeron component.
     */
    public static final String SECURE_RANDOM_ALGORITHM_PROP_NAME = "aeron.secure.random.algorithm";

    /**
     * The default secure random algorithm to be used.
     */
    public static final String SECURE_RANDOM_ALGORITHM_DEFAULT =
        SystemUtil.isWindows() ? "Windows-PRNG" : "NativePRNGNonBlocking";

    /**
     * Should a component's configuration be printed on start.
     *
     * @return {@code true} if the configuration should be printed on start.
     * @see #PRINT_CONFIGURATION_ON_START_PROP_NAME
     */
    public static boolean shouldPrintConfigurationOnStart()
    {
        return "true".equals(getProperty(PRINT_CONFIGURATION_ON_START_PROP_NAME));
    }

    /**
     * Get the configured value for the secure random algorithm, falling back to the default if not supplied.
     *
     * @return the secure random algorithm
     * @see #SECURE_RANDOM_ALGORITHM_PROP_NAME
     * @see #SECURE_RANDOM_ALGORITHM_DEFAULT
     */
    public static String getSecureRandomAlgorithm()
    {
        return System.getProperty(SECURE_RANDOM_ALGORITHM_PROP_NAME, SECURE_RANDOM_ALGORITHM_DEFAULT);
    }

    /**
     * Get the current fallback logger based on the supplied property.
     *
     * @return the configured PrintStream.
     */
    @Config
    public static PrintStream fallbackLogger()
    {
        final String fallbackLoggerName = getProperty(FALLBACK_LOGGER_PROP_NAME, "stderr");
        return switch (fallbackLoggerName)
        {
            case "stdout" -> System.out;
            case "no_op" -> NO_OP_LOGGER;
            default -> System.err;
        };
    }

    private static final PrintStream NO_OP_LOGGER = new PrintStream(
        new OutputStream()
        {
            public void write(final int b)
            {
                // No-op
            }
        });
    private static final Map<String, Boolean> DEBUG_FIELDS_SEEN = new ConcurrentHashMap<>();
    private static final VarHandle IS_CONCLUDED_VH;

    static
    {
        try
        {
            IS_CONCLUDED_VH = MethodHandles.lookup().findVarHandle(CommonContext.class, "isConcluded", boolean.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private volatile boolean isConcluded;
    private long driverTimeoutMs = DRIVER_TIMEOUT_MS;
    private String aeronDirectoryName = getAeronDirectoryName();
    private File aeronDirectory;
    private File cncFile;
    private UnsafeBuffer countersMetaDataBuffer;
    private UnsafeBuffer countersValuesBuffer;
    private boolean enableExperimentalFeatures = Boolean.getBoolean(ENABLE_EXPERIMENTAL_FEATURES_PROP_NAME);

    static
    {
        String baseDirName = null;

        if (SystemUtil.isLinux())
        {
            final File devShmDir = new File("/dev/shm");
            if (devShmDir.exists())
            {
                baseDirName = "/dev/shm/aeron";
            }
        }

        if (null == baseDirName)
        {
            baseDirName = SystemUtil.tmpDirName() + "aeron";
        }

        AERON_DIR_PROP_DEFAULT = baseDirName + '-' + System.getProperty("user.name", "default");
    }

    /**
     * Perform a shallow copy of the object.
     *
     * @return a shallow copy of the object.
     */
    public CommonContext clone()
    {
        try
        {
            return (CommonContext)super.clone();
        }
        catch (final CloneNotSupportedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Get the default directory name to be used if {@link #aeronDirectoryName(String)} is not set. This will take
     * the {@link #AERON_DIR_PROP_NAME} if set and if not then {@link #AERON_DIR_PROP_DEFAULT}.
     *
     * @return the default directory name to be used if {@link #aeronDirectoryName(String)} is not set.
     */
    @Config(id = "AERON_DIR")
    public static String getAeronDirectoryName()
    {
        return getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT);
    }

    /**
     * Convert the default Aeron directory name to be a random name for use with embedded drivers.
     *
     * @return random directory name with default directory name as base
     */
    public static String generateRandomDirName()
    {
        return AERON_DIR_PROP_DEFAULT + "-" + UUID.randomUUID();
    }

    /**
     * This completes initialization of the CommonContext object. It is automatically called by subclasses.
     *
     * @return this Object for method chaining.
     */
    public CommonContext conclude()
    {
        if ((boolean)IS_CONCLUDED_VH.getAndSet(this, true))
        {
            throw new ConcurrentConcludeException();
        }

        concludeAeronDirectory();

        cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        return this;
    }

    /**
     * Has the context had the {@link #conclude()} method called.
     *
     * @return true of the {@link #conclude()} method has been called.
     */
    public boolean isConcluded()
    {
        return isConcluded;
    }

    /**
     * Conclude the {@link #aeronDirectory()} so it does not need to keep being recreated.
     *
     * @return this for a fluent API.
     */
    public CommonContext concludeAeronDirectory()
    {
        if (null == aeronDirectory)
        {
            try
            {
                aeronDirectory = new File(aeronDirectoryName).getCanonicalFile();
            }
            catch (final IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        return this;
    }

    /**
     * Get the top level Aeron directory used for communication between the client and Media Driver, and
     * the location of the data buffers.
     *
     * @return The top level Aeron directory.
     */
    public String aeronDirectoryName()
    {
        return aeronDirectoryName;
    }

    /**
     * Get the directory in which the aeron config files are stored.
     * <p>
     * This is valid after a call to {@link #conclude()} or {@link #concludeAeronDirectory()}.
     *
     * @return the directory in which the aeron config files are stored.
     * @see #aeronDirectoryName()
     */
    public File aeronDirectory()
    {
        return aeronDirectory;
    }

    /**
     * Set the top level Aeron directory used for communication between the client and Media Driver, and the location
     * of the data buffers.
     *
     * @param dirName New top level Aeron directory.
     * @return this for a fluent API.
     */
    public CommonContext aeronDirectoryName(final String dirName)
    {
        this.aeronDirectoryName = dirName;
        return this;
    }

    /**
     * Create a new command and control file in the administration directory.
     *
     * @return The newly created File.
     */
    public static File newDefaultCncFile()
    {
        return new File(getProperty(AERON_DIR_PROP_NAME, AERON_DIR_PROP_DEFAULT), CncFileDescriptor.CNC_FILE);
    }

    /**
     * Create a new command and control file in the administration directory.
     *
     * @param aeronDirectoryName name of the aeronDirectory that containing the cnc file.
     * @return The newly created File.
     */
    public static File newCncFile(final String aeronDirectoryName)
    {
        return new File(aeronDirectoryName, CncFileDescriptor.CNC_FILE);
    }

    /**
     * Get the buffer containing the counter metadata. These counters are R/W for the driver, read only for all
     * other users.
     *
     * @return The buffer storing the counter metadata.
     */
    public UnsafeBuffer countersMetaDataBuffer()
    {
        return countersMetaDataBuffer;
    }

    /**
     * Set the buffer containing the counter metadata. Testing/internal purposes only.
     *
     * @param countersMetaDataBuffer The new counter metadata buffer.
     * @return this for a fluent API.
     */
    public CommonContext countersMetaDataBuffer(final UnsafeBuffer countersMetaDataBuffer)
    {
        this.countersMetaDataBuffer = countersMetaDataBuffer;
        return this;
    }

    /**
     * Get the buffer containing the counters. These counters are R/W for the driver, read only for all other users.
     *
     * @return The buffer storing the counters.
     */
    public UnsafeBuffer countersValuesBuffer()
    {
        return countersValuesBuffer;
    }

    /**
     * Set the buffer containing the counters. Testing/internal purposes only.
     *
     * @param countersValuesBuffer The new counters buffer.
     * @return this for a fluent API.
     */
    public CommonContext countersValuesBuffer(final UnsafeBuffer countersValuesBuffer)
    {
        this.countersValuesBuffer = countersValuesBuffer;
        return this;
    }

    /**
     * Get the command and control file.
     *
     * @return The command and control file.
     */
    public File cncFile()
    {
        return cncFile;
    }

    /**
     * Set the driver timeout in milliseconds.
     *
     * @param driverTimeoutMs to indicate liveness of driver
     * @return this for a fluent API.
     */
    public CommonContext driverTimeoutMs(final long driverTimeoutMs)
    {
        this.driverTimeoutMs = driverTimeoutMs;
        return this;
    }

    /**
     * Get the driver timeout in milliseconds.
     *
     * @return driver timeout in milliseconds.
     */
    @Config(id = "DRIVER_TIMEOUT")
    public long driverTimeoutMs()
    {
        return checkDebugTimeout(driverTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Should experimental features for the driver be enabled.
     *
     * @return <code>true</code> if enabled, <code>false</code> otherwise.
     * @see #enableExperimentalFeatures(boolean)
     * @since 1.44.0
     */
    @Config
    public boolean enableExperimentalFeatures()
    {
        return enableExperimentalFeatures;
    }

    /**
     * Should experimental features for the driver be enabled.
     *
     * @param enableExperimentalFeatures indicate whether experimental features for the driver should be enabled.
     * @return this for a fluent API
     * @see #ENABLE_EXPERIMENTAL_FEATURES_PROP_NAME
     * @see #enableExperimentalFeatures()
     * @since 1.44.0
     */
    public CommonContext enableExperimentalFeatures(final boolean enableExperimentalFeatures)
    {
        this.enableExperimentalFeatures = enableExperimentalFeatures;
        return this;
    }

    /**
     * Override the supplied timeout with the debug value if it has been set, and we are in debug mode.
     *
     * @param timeout  The timeout value currently in use.
     * @param timeUnit The units of the timeout value. Debug timeout is specified in ns, so will be converted to this
     *                 unit.
     * @return The debug timeout if specified, and we are being debugged or the supplied value if not. Will be in
     * timeUnit units.
     */
    public static long checkDebugTimeout(final long timeout, final TimeUnit timeUnit)
    {
        return checkDebugTimeout(timeout, timeUnit, 1.0);
    }

    /**
     * Override the supplied timeout with the debug value if it has been set, and we are in debug mode.
     *
     * @param timeout  The timeout value currently in use.
     * @param timeUnit The units of the timeout value. Debug timeout is specified in ns, so will be converted to this
     *                 unit.
     * @param factor   to multiply the debug timeout by. Required when some timeouts need to be larger than others in
     *                 order to pass validation. E.g. clientLiveness and publicationUnblock.
     * @return The debug timeout if specified, and we are being debugged or the supplied value if not. Will be in
     * timeUnit units.
     */
    public static long checkDebugTimeout(final long timeout, final TimeUnit timeUnit, final double factor)
    {
        final String debugTimeoutString = getProperty(DEBUG_TIMEOUT_PROP_NAME);
        if (null == debugTimeoutString || !SystemUtil.isDebuggerAttached())
        {
            return timeout;
        }

        try
        {
            final long debugTimeoutNs =
                (long)(factor * SystemUtil.parseDuration(DEBUG_TIMEOUT_PROP_NAME, debugTimeoutString));
            final long debugTimeout = timeUnit.convert(debugTimeoutNs, TimeUnit.NANOSECONDS);
            final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

            String debugFieldName = "<unknown>";
            for (int i = 0; i < stackTrace.length; i++)
            {
                final String methodName = stackTrace[i].getMethodName();
                if (!"checkDebugTimeout".equals(methodName) && !"getStackTrace".equals(methodName))
                {
                    final String className = stackTrace[i].getClassName();
                    debugFieldName = className + "." + methodName;
                    break;
                }
            }

            if (null == DEBUG_FIELDS_SEEN.putIfAbsent(debugFieldName, true))
            {
                final String message = "Using debug timeout [" + debugTimeout + "] for " + debugFieldName +
                    " replacing [" + timeout + "]";
                System.out.println(message);
            }

            return debugTimeout;
        }
        catch (final NumberFormatException ignore)
        {
            return timeout;
        }
    }

    /**
     * Delete the current Aeron directory, throwing errors if not possible.
     */
    public void deleteAeronDirectory()
    {
        IoUtil.delete(aeronDirectory, false);
    }

    /**
     * Map the CnC file if it exists.
     *
     * @param logger for feedback
     * @return a new mapping for the file if it exists otherwise null;
     */
    public MappedByteBuffer mapExistingCncFile(final Consumer<String> logger)
    {
        final File cncFile = new File(aeronDirectory, CncFileDescriptor.CNC_FILE);

        if (cncFile.exists() && cncFile.length() > CncFileDescriptor.META_DATA_LENGTH)
        {
            if (null != logger)
            {
                logger.accept("INFO: Aeron CnC file exists: " + cncFile);
            }

            return IoUtil.mapExistingFile(cncFile, CncFileDescriptor.CNC_FILE);
        }

        return null;
    }

    /**
     * Is a media driver active in the given directory?
     *
     * @param directory       to check.
     * @param driverTimeoutMs for the driver liveness check.
     * @param logger          for feedback as liveness checked.
     * @return true if a driver is active or false if not.
     */
    public static boolean isDriverActive(
        final File directory, final long driverTimeoutMs, final Consumer<String> logger)
    {
        final File cncFile = new File(directory, CncFileDescriptor.CNC_FILE);

        if (cncFile.exists() && cncFile.length() > CncFileDescriptor.META_DATA_LENGTH)
        {
            logger.accept("INFO: Aeron CnC file exists: " + cncFile);

            final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "CnC file");
            try
            {
                return isDriverActive(driverTimeoutMs, logger, cncByteBuffer);
            }
            finally
            {
                BufferUtil.free(cncByteBuffer);
            }
        }

        return false;
    }

    /**
     * Is a media driver active in the current Aeron directory?
     *
     * @param driverTimeoutMs for the driver liveness check.
     * @param logger          for feedback as liveness checked.
     * @return true if a driver is active or false if not.
     */
    public boolean isDriverActive(final long driverTimeoutMs, final Consumer<String> logger)
    {
        final MappedByteBuffer cncByteBuffer = mapExistingCncFile(logger);
        try
        {
            return isDriverActive(driverTimeoutMs, logger, cncByteBuffer);
        }
        finally
        {
            BufferUtil.free(cncByteBuffer);
        }
    }

    /**
     * Is a media driver active in the current mapped CnC buffer? If the driver is starting then it will wait for
     * up to the driverTimeoutMs by checking for the cncVersion being set.
     *
     * @param driverTimeoutMs for the driver liveness check.
     * @param logger          for feedback as liveness checked.
     * @param cncByteBuffer   for the existing CnC file.
     * @return true if a driver is active or false if not.
     */
    public static boolean isDriverActive(
        final long driverTimeoutMs, final Consumer<String> logger, final ByteBuffer cncByteBuffer)
    {
        if (null == cncByteBuffer)
        {
            return false;
        }

        final UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);

        final long startTimeMs = System.currentTimeMillis();
        int cncVersion;
        while (0 == (cncVersion = cncMetaDataBuffer.getIntVolatile(CncFileDescriptor.cncVersionOffset(0))))
        {
            if (System.currentTimeMillis() > (startTimeMs + driverTimeoutMs))
            {
                throw new DriverTimeoutException("CnC file is created but not initialised.");
            }

            sleep(1);
        }

        CncFileDescriptor.checkVersion(cncVersion);

        final ManyToOneRingBuffer toDriverBuffer = new ManyToOneRingBuffer(
            CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

        final long timestampMs = toDriverBuffer.consumerHeartbeatTime();
        final long nowMs = System.currentTimeMillis();
        final long timestampAgeMs = nowMs - timestampMs;

        logger.accept("INFO: Aeron toDriver consumer heartbeat age is (ms): " + timestampAgeMs);

        return timestampAgeMs <= driverTimeoutMs;
    }

    /**
     * Request a driver to run its termination hook.
     *
     * @param directory   for the driver.
     * @param tokenBuffer containing the optional token for the request.
     * @param tokenOffset within the tokenBuffer at which the token begins.
     * @param tokenLength of the token in the tokenBuffer.
     * @return true if request was sent or false if request could not be sent.
     */
    public static boolean requestDriverTermination(
        final File directory,
        final DirectBuffer tokenBuffer,
        final int tokenOffset,
        final int tokenLength)
    {
        final File cncFile = new File(directory, CncFileDescriptor.CNC_FILE);

        if (cncFile.exists() && cncFile.length() > CncFileDescriptor.META_DATA_LENGTH)
        {
            final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "CnC file");
            try
            {
                final UnsafeBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
                final int cncVersion = cncMetaDataBuffer.getIntVolatile(cncVersionOffset(0));

                if (cncVersion > 0)
                {
                    CncFileDescriptor.checkVersion(cncVersion);

                    final ManyToOneRingBuffer toDriverBuffer = new ManyToOneRingBuffer(
                        CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
                    final long clientId = toDriverBuffer.nextCorrelationId();
                    final DriverProxy driverProxy = new DriverProxy(toDriverBuffer, clientId);

                    return driverProxy.terminateDriver(tokenBuffer, tokenOffset, tokenLength);
                }
            }
            finally
            {
                BufferUtil.free(cncByteBuffer);
            }
        }

        return false;
    }

    /**
     * Read the error log to a given {@link PrintStream}.
     *
     * @param out to write the error log contents to.
     * @return the number of observations from the error log.
     */
    public int saveErrorLog(final PrintStream out)
    {
        final MappedByteBuffer cncByteBuffer = mapExistingCncFile(null);
        try
        {
            return saveErrorLog(out, cncByteBuffer);
        }
        finally
        {
            BufferUtil.free(cncByteBuffer);
        }
    }

    /**
     * Read the error log to a given {@link PrintStream}.
     *
     * @param out           to write the error log contents to.
     * @param cncByteBuffer containing the error log.
     * @return the number of observations from the error log.
     */
    public int saveErrorLog(final PrintStream out, final ByteBuffer cncByteBuffer)
    {
        if (null == cncByteBuffer)
        {
            return 0;
        }

        return printErrorLog(errorLogBuffer(cncByteBuffer), out);
    }

    /**
     * Release resources used by the CommonContext.
     */
    public void close()
    {
    }

    /**
     * Print the contents of an error log to a {@link PrintStream} in human-readable format.
     *
     * @param errorBuffer to read errors from.
     * @param out         print the errors to.
     * @return number of distinct errors observed.
     */
    public static int printErrorLog(final AtomicBuffer errorBuffer, final PrintStream out)
    {
        int distinctErrorCount = 0;

        if (ErrorLogReader.hasErrors(errorBuffer))
        {
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
            final ErrorConsumer errorConsumer =
                (count, firstTimestamp, lastTimestamp, encodedException) ->
                {
                    final String fromDate = dateFormat.format(new Date(firstTimestamp));
                    final String toDate = dateFormat.format(new Date(lastTimestamp));

                    out.println();
                    out.println(count + " observations from " + fromDate + " to " + toDate + " for:");
                    out.println(encodedException);
                };

            distinctErrorCount = ErrorLogReader.read(errorBuffer, errorConsumer);
            out.println();
            out.println(distinctErrorCount + " distinct errors observed.");
        }
        else
        {
            out.println();
            out.println("O distinct errors observed");
        }

        return distinctErrorCount;
    }

    /**
     * Save the existing errors from a {@link MarkFile} to a file in the same directory as the original {@link MarkFile}
     * and optionally print location of such file to the supplied {@link PrintStream}.
     *
     * @param markFile        which contains the error buffer.
     * @param errorBuffer     which wraps the error log.
     * @param logger          to which the existing errors will be printed.
     * @param errorFilePrefix to add to the generated error file.
     */
    public static void saveExistingErrors(
        final File markFile,
        final AtomicBuffer errorBuffer,
        final PrintStream logger,
        final String errorFilePrefix)
    {
        try
        {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final int observations = printErrorLog(errorBuffer, new PrintStream(baos, false, US_ASCII));
            if (observations > 0)
            {
                final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-SSSZ");
                final File errorLogFile = new File(
                    markFile.getParentFile(), errorFilePrefix + '-' + dateFormat.format(new Date()) + "-error.log");

                if (null != logger)
                {
                    logger.println("WARNING: existing errors saved to: " + errorLogFile);
                }

                try (FileOutputStream out = new FileOutputStream(errorLogFile))
                {
                    baos.writeTo(out);
                }
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Get an {@link AtomicBuffer} which wraps the error log in the CnC file.
     *
     * @param cncByteBuffer which contains the error log.
     * @return an {@link AtomicBuffer} which wraps the error log in the CnC file.
     */
    public static AtomicBuffer errorLogBuffer(final ByteBuffer cncByteBuffer)
    {
        final DirectBuffer cncMetaDataBuffer = CncFileDescriptor.createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaDataBuffer.getInt(cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);

        return CncFileDescriptor.createErrorLogBuffer(cncByteBuffer, cncMetaDataBuffer);
    }

    /**
     * Wrap a user ErrorHandler so that error will continue to write to the errorLog.
     *
     * @param userErrorHandler the user specified ErrorHandler, can be null.
     * @param errorLog         the configured errorLog, either the default or user supplied.
     * @return an error handler that will delegate to both the userErrorHandler and the errorLog.
     */
    public static ErrorHandler setupErrorHandler(final ErrorHandler userErrorHandler, final DistinctErrorLog errorLog)
    {
        return setupErrorHandler(userErrorHandler, errorLog, fallbackLogger());
    }

    /**
     * Connect to the media driver and extract file page size from the C'n'C file.
     *
     * @param aeronDirectory where driver is running.
     * @param clock          to use.
     * @param timeoutMs      for awaiting connection.
     * @return file page size from running media driver or {@link LogBufferDescriptor#PAGE_MIN_SIZE} if driver is old.
     * @since 1.48.0
     */
    public static int driverFilePageSize(final File aeronDirectory, final EpochClock clock, final long timeoutMs)
    {
        final UnsafeBuffer metadata =
            awaitCncFileCreation(new File(aeronDirectory, CNC_FILE), clock, clock.time() + timeoutMs);
        try
        {
            return driverFilePageSize(metadata);
        }
        finally
        {
            BufferUtil.free(metadata);
        }
    }

    static int driverFilePageSize(final DirectBuffer metadata)
    {
        final int pageSize = CncFileDescriptor.filePageSize(metadata);
        return 0 != pageSize ? pageSize : LogBufferDescriptor.PAGE_MIN_SIZE;
    }

    @SuppressWarnings("try")
    static UnsafeBuffer awaitCncFileCreation(
        final File cncFile, final EpochClock clock, final long deadlineMs)
    {
        while (true)
        {
            while (!cncFile.exists() || cncFile.length() < CncFileDescriptor.META_DATA_LENGTH)
            {
                if (clock.time() > deadlineMs)
                {
                    throw new DriverTimeoutException("CnC file not created: " + cncFile.getAbsolutePath());
                }

                sleep(Aeron.Configuration.IDLE_SLEEP_DEFAULT_MS);
            }

            try (FileChannel fileChannel = FileChannel.open(cncFile.toPath(), READ, WRITE))
            {
                final long fileSize = fileChannel.size();
                if (fileSize < CncFileDescriptor.META_DATA_LENGTH)
                {
                    if (clock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException(
                            "CnC file is created but not populated: " + cncFile.getAbsolutePath());
                    }

                    fileChannel.close();
                    sleep(Aeron.Configuration.IDLE_SLEEP_DEFAULT_MS);
                    continue;
                }

                final UnsafeBuffer metaDataBuffer =
                    CncFileDescriptor.createMetaDataBuffer(fileChannel.map(READ_WRITE, 0, fileSize));

                int cncVersion;
                while (0 == (cncVersion = metaDataBuffer.getIntVolatile(CncFileDescriptor.cncVersionOffset(0))))
                {
                    if (clock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException("CnC file is created but not initialised: " +
                            cncFile.getAbsolutePath());
                    }

                    sleep(Aeron.Configuration.AWAITING_IDLE_SLEEP_MS);
                }

                CncFileDescriptor.checkVersion(cncVersion);
                if (SemanticVersion.minor(cncVersion) < SemanticVersion.minor(CncFileDescriptor.CNC_VERSION))
                {
                    throw new AeronException("driverVersion=" + SemanticVersion.toString(cncVersion) +
                        " insufficient for clientVersion=" +
                        SemanticVersion.toString(CncFileDescriptor.CNC_VERSION));
                }

                return metaDataBuffer;
            }
            catch (final NoSuchFileException | AccessDeniedException ignore)
            {
            }
            catch (final FileSystemException ex)
            {
                // JDK exception translation does not handle `ERROR_SHARING_VIOLATION (32)` and returns
                // FileSystemException with the error "The process cannot access the file because it is being
                // used by another process.". Our current thinking is that matching by text is too brittle due
                // to error message being locale-sensitive on Windows. Therefore, we are going to retry on any
                // FileSystemException when running on Windows.
                if (SystemUtil.isWindows())
                {
                    continue;
                }

                throw new AeronException(cncFileErrorMessage(cncFile, ex), ex);
            }
            catch (final IOException ex)
            {
                throw new AeronException(cncFileErrorMessage(cncFile, ex), ex);
            }
        }
    }

    static ErrorHandler setupErrorHandler(
        final ErrorHandler userErrorHandler, final DistinctErrorLog errorLog, final PrintStream fallbackErrorStream)
    {
        final LoggingErrorHandler loggingErrorHandler = new LoggingErrorHandler(errorLog, fallbackErrorStream);
        if (null == userErrorHandler)
        {
            return loggingErrorHandler;
        }
        else
        {
            return new ErrorHandlerWrapper(loggingErrorHandler, userErrorHandler);
        }
    }

    static void sleep(final long durationMs)
    {
        try
        {
            Thread.sleep(durationMs);
        }
        catch (final InterruptedException ex)
        {
            Thread.currentThread().interrupt();
            throw new AeronException("unexpected interrupt", ex);
        }
    }

    private static String cncFileErrorMessage(final File file, final Exception ex)
    {
        return "cannot open CnC file: " + file.getAbsolutePath() + " reason=" + ex;
    }

    private static final class ErrorHandlerWrapper implements ErrorHandler, AutoCloseable
    {
        private final LoggingErrorHandler loggingErrorHandler;
        private final ErrorHandler userErrorHandler;

        private ErrorHandlerWrapper(final LoggingErrorHandler loggingErrorHandler, final ErrorHandler userErrorHandler)
        {
            this.loggingErrorHandler = loggingErrorHandler;
            this.userErrorHandler = userErrorHandler;
        }

        public void close()
        {
            loggingErrorHandler.close();
            if (userErrorHandler instanceof AutoCloseable)
            {
                CloseHelper.quietClose((AutoCloseable)userErrorHandler);
            }
        }

        public void onError(final Throwable throwable)
        {
            loggingErrorHandler.onError(throwable);
            userErrorHandler.onError(throwable);
        }
    }
}
