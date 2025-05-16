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
import io.aeron.CommonContext;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import io.aeron.driver.media.SendChannelEndpoint;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.AsciiNumberFormatException;
import org.agrona.BitUtil;
import org.agrona.LangUtil;
import org.agrona.Strings;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.*;
import org.agrona.concurrent.broadcast.BroadcastBufferDescriptor;
import org.agrona.concurrent.ringbuffer.RingBufferDescriptor;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.StatusIndicator;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.ThreadingMode.DEDICATED;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MAX_SIZE;
import static io.aeron.logbuffer.LogBufferDescriptor.PAGE_MIN_SIZE;
import static java.lang.Integer.getInteger;
import static java.lang.Long.getLong;
import static java.lang.System.getProperty;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.fromHex;
import static org.agrona.SystemUtil.*;

/**
 * Configuration options for the {@link MediaDriver}.
 */
public final class Configuration
{
    /**
     * Warn if the Aeron directory exists.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
    public static final String DIR_WARN_IF_EXISTS_PROP_NAME = "aeron.dir.warn.if.exists";

    /**
     * Should the Media Driver attempt to immediately delete the directory {@link CommonContext#AERON_DIR_PROP_NAME}
     * on start if it exists before performing any additional checks.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
    public static final String DIR_DELETE_ON_START_PROP_NAME = "aeron.dir.delete.on.start";

    /**
     * Should driver attempt to delete {@link CommonContext#AERON_DIR_PROP_NAME} on shutdown.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
    public static final String DIR_DELETE_ON_SHUTDOWN_PROP_NAME = "aeron.dir.delete.on.shutdown";

    /**
     * Should high resolution timer be used on Windows.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false, existsInC = false)
    public static final String USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME = "aeron.use.windows.high.res.timer";

    /**
     * Property name for default boolean value for if subscriptions should have a tether for local flow control.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
    public static final String TETHER_SUBSCRIPTIONS_PROP_NAME = "aeron.tether.subscriptions";

    /**
     * Property name for default boolean value for if a stream is reliable. True to NAK, false to gap fill.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
    public static final String RELIABLE_STREAM_PROP_NAME = "aeron.reliable.stream";

    /**
     * Property name for boolean value of term buffers should be created sparse.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
    public static final String TERM_BUFFER_SPARSE_FILE_PROP_NAME = "aeron.term.buffer.sparse.file";

    /**
     * Property name for default boolean value for if subscriptions should be considered a group member or individual.
     */
    public static final String GROUP_RECEIVER_CONSIDERATION_PROP_NAME = "aeron.receiver.group.consideration";

    /**
     * Property name for page size to align all files to.
     */
    @Config
    public static final String FILE_PAGE_SIZE_PROP_NAME = "aeron.file.page.size";

    /**
     * Default page size for alignment of all files.
     */
    @Config
    public static final int FILE_PAGE_SIZE_DEFAULT = 4 * 1024;

    /**
     * Property name for boolean value for if storage checks should be performed when allocating files.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
    public static final String PERFORM_STORAGE_CHECKS_PROP_NAME = "aeron.perform.storage.checks";

    /**
     * Length (in bytes) of the log buffers for UDP publication terms.
     */
    @Config(uriParam = "term-length")
    public static final String TERM_BUFFER_LENGTH_PROP_NAME = "aeron.term.buffer.length";

    /**
     * Default term buffer length.
     */
    @Config
    public static final int TERM_BUFFER_LENGTH_DEFAULT = 16 * 1024 * 1024;

    /**
     * Length (in bytes) of the log buffers for IPC publication terms.
     */
    @Config(uriParam = "term-length")
    public static final String IPC_TERM_BUFFER_LENGTH_PROP_NAME = "aeron.ipc.term.buffer.length";

    /**
     * Default IPC term buffer length.
     */
    @Config(id = "IPC_TERM_BUFFER_LENGTH")
    public static final int TERM_BUFFER_IPC_LENGTH_DEFAULT = 64 * 1024 * 1024;

    /**
     * Property name low file storage warning threshold in bytes.
     */
    @Config
    public static final String LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME = "aeron.low.file.store.warning.threshold";

    /**
     * Default value in bytes for low file storage warning threshold.
     */
    @Config
    public static final long LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT = TERM_BUFFER_LENGTH_DEFAULT * 10L;

    /**
     * Length (in bytes) of the conductor buffer for control commands from the clients to the media driver conductor.
     */
    @Config(expectedCEnvVarFieldName = "AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR")
    public static final String CONDUCTOR_BUFFER_LENGTH_PROP_NAME = "aeron.conductor.buffer.length";

    /**
     * Default buffer length for conductor buffers between the client and the media driver conductor.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT",
        skipCDefaultValidation = true,
        defaultType = DefaultType.INT,
        defaultInt = (1024 * 1024) + 768)
    public static final int CONDUCTOR_BUFFER_LENGTH_DEFAULT = (1024 * 1024) + RingBufferDescriptor.TRAILER_LENGTH;

    /**
     * Length (in bytes) of the broadcast buffers from the media driver to the clients.
     */
    @Config(expectedCEnvVarFieldName = "AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR")
    public static final String TO_CLIENTS_BUFFER_LENGTH_PROP_NAME = "aeron.clients.buffer.length";

    /**
     * Default buffer length for broadcast buffers from the media driver and the clients.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT",
        skipCDefaultValidation = true,
        defaultType = DefaultType.INT,
        defaultInt = (1024 * 1024) + 128)
    public static final int TO_CLIENTS_BUFFER_LENGTH_DEFAULT = (1024 * 1024) + BroadcastBufferDescriptor.TRAILER_LENGTH;

    /**
     * Property name for length of the buffer for the counters.
     * <p>
     * Each counter uses {@link org.agrona.concurrent.status.CountersReader#COUNTER_LENGTH} bytes.
     */
    @Config(expectedCEnvVarFieldName = "AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR")
    public static final String COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME = "aeron.counters.buffer.length";

    /**
     * Default length of the buffer for the counters file.
     */
    @Config(expectedCDefaultFieldName = "AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT")
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Maximum length of the buffer for the counters file.
     */
    public static final int COUNTERS_VALUES_BUFFER_LENGTH_MAX = 500 * 1024 * 1024;

    /**
     * Property name for length of the memory mapped buffer for the distinct error log.
     */
    @Config
    public static final String ERROR_BUFFER_LENGTH_PROP_NAME = "aeron.error.buffer.length";

    /**
     * Default buffer length for the error buffer for the media driver.
     */
    @Config
    public static final int ERROR_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Property name for length of the memory mapped buffer for the {@link io.aeron.driver.reports.LossReport}.
     */
    @Config
    public static final String LOSS_REPORT_BUFFER_LENGTH_PROP_NAME = "aeron.loss.report.buffer.length";

    /**
     * Default buffer length for the {@link io.aeron.driver.reports.LossReport}.
     */
    @Config
    public static final int LOSS_REPORT_BUFFER_LENGTH_DEFAULT = 1024 * 1024;

    /**
     * Property name for length of the initial window which must be sufficient for Bandwidth Delay Product (BDP).
     */
    @Config
    public static final String INITIAL_WINDOW_LENGTH_PROP_NAME = "aeron.rcv.initial.window.length";

    /**
     * Default initial window length for flow control sender to receiver purposes. This assumes a system free of pauses.
     * <p>
     * Length of Initial Window:
     * <p>
     * RTT (LAN) = 100 usec
     * Throughput = 10 Gbps
     * <p>
     * Buffer = Throughput * RTT
     * Buffer = (10 * 1000 * 1000 * 1000 / 8) * 0.0001 = 125000
     * Round to 128 KB
     */
    @Config
    public static final int INITIAL_WINDOW_LENGTH_DEFAULT = 128 * 1024;

    /**
     * Status message timeout in nanoseconds after which one will be sent when data flow has not triggered one.
     */
    @Config
    public static final String STATUS_MESSAGE_TIMEOUT_PROP_NAME = "aeron.rcv.status.message.timeout";

    /**
     * Max timeout between Status messages (SM)s.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 200 * 1000 * 1000,
        expectedCDefaultFieldName = "AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT")
    public static final long STATUS_MESSAGE_TIMEOUT_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(200);

    /**
     * Property name for ratio of sending data to polling status messages in the {@link Sender}.
     */
    @Config
    public static final String SEND_TO_STATUS_POLL_RATIO_PROP_NAME = "aeron.send.to.status.poll.ratio";

    /**
     * The ratio for sending data to polling status messages in the Sender. This may be reduced for smaller windows.
     */
    @Config
    public static final int SEND_TO_STATUS_POLL_RATIO_DEFAULT = 6;

    /**
     * Property name for the limit of the number of driver managed resources that can be freed in a single duty cycle.
     */
    @Config
    public static final String RESOURCE_FREE_LIMIT_PROP_NAME = "aeron.driver.resource.free.limit";

    /**
     * Default value for the limit of the number of driver managed resources that can be freed in a single duty cycle.
     */
    @Config
    public static final int RESOURCE_FREE_LIMIT_DEFAULT = 10;

    /**
     * Property name for SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     */
    @Config
    public static final String SOCKET_RCVBUF_LENGTH_PROP_NAME = "aeron.socket.so_rcvbuf";

    /**
     * Default SO_RCVBUF length.
     */
    @Config
    public static final int SOCKET_RCVBUF_LENGTH_DEFAULT = 128 * 1024;

    /**
     * Property name for SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     */
    @Config
    public static final String SOCKET_SNDBUF_LENGTH_PROP_NAME = "aeron.socket.so_sndbuf";

    /**
     * Default SO_SNDBUF length.
     */
    @Config
    public static final int SOCKET_SNDBUF_LENGTH_DEFAULT = 0;

    /**
     * Property name for IP_MULTICAST_TTL setting on UDP sockets.
     */
    @Config
    public static final String SOCKET_MULTICAST_TTL_PROP_NAME = "aeron.socket.multicast.ttl";

    /**
     * Multicast TTL value, 0 means use OS default.
     */
    @Config
    public static final int SOCKET_MULTICAST_TTL_DEFAULT = 0;

    /**
     * Property name for linger timeout after draining on {@link Publication}s so they can respond to NAKs.
     */
    @Config
    public static final String PUBLICATION_LINGER_PROP_NAME = "aeron.publication.linger.timeout";

    /**
     * Default time for {@link Publication}s to linger after draining and before cleanup in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 5L * 1000 * 1000 * 1000)
    public static final long PUBLICATION_LINGER_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Property name for {@link Aeron} client liveness timeout after which it is considered not alive.
     */
    @Config
    public static final String CLIENT_LIVENESS_TIMEOUT_PROP_NAME = "aeron.client.liveness.timeout";

    /**
     * Default timeout for client liveness timeout after which it is considered not alive.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 10L * 1000 * 1000 * 1000)
    public static final long CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * {@link Image} liveness timeout for how long it stays active without heartbeats or lingers around after being
     * drained.
     */
    @Config
    public static final String IMAGE_LIVENESS_TIMEOUT_PROP_NAME = "aeron.image.liveness.timeout";

    /**
     * Default timeout for {@link Image} liveness timeout.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 10L * 1000 * 1000 * 1000)
    public static final long IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Property name for window limit on {@link Publication} side by which the publisher can get ahead of consumers.
     */
    @Config(defaultType = DefaultType.INT, defaultInt = 0)
    public static final String PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME = "aeron.publication.term.window.length";

    /**
     * Property name for window limit for IPC publications.
     */
    @Config(defaultType = DefaultType.INT, defaultInt = 0)
    public static final String IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME =
        "aeron.ipc.publication.term.window.length";

    /**
     * {@link Publication} unblock timeout due to client crash or untimely commit.
     * <p>
     * A publication can become blocked if the client crashes while publishing or if
     * {@link io.aeron.Publication#tryClaim(int, BufferClaim)} is used without following up by calling
     * {@link BufferClaim#commit()} or {@link BufferClaim#abort()}.
     */
    @Config
    public static final String PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME = "aeron.publication.unblock.timeout";

    /**
     * Timeout for {@link Publication} unblock in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 15L * 1000 * 1000 * 1000)
    public static final long PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(15);

    /**
     * Property name for {@link Publication} timeout due to lack of status messages which indicate a connection.
     */
    @Config
    public static final String PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME = "aeron.publication.connection.timeout";

    /**
     * Timeout for {@link Publication} connection timeout in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 5L * 1000 * 1000 * 1000)
    public static final long PUBLICATION_CONNECTION_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Property name for if spy subscriptions simulate a connection to a network publication.
     * <p>
     * If true then this will override the min group size of the min and tagged flow control strategies.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = false)
    public static final String SPIES_SIMULATE_CONNECTION_PROP_NAME = "aeron.spies.simulate.connection";

    /**
     * Default idle strategy for agents.
     */
    private static final String DEFAULT_IDLE_STRATEGY = "org.agrona.concurrent.BackoffIdleStrategy";

    /**
     * Spin on no activity before backing off to yielding.
     */
    public static final long IDLE_MAX_SPINS = 10;

    /**
     * Yield the thread so others can run before backing off to parking.
     */
    public static final long IDLE_MAX_YIELDS = 20;

    /**
     * Park for the minimum period of time which is typically 50-55 microseconds on 64-bit non-virtualised Linux.
     * You will typically get 50-55 microseconds plus the number of nanoseconds requested if a core is available.
     * On Windows expect to wait for at least 16ms or 1ms if the high-res timers are enabled.
     */
    public static final long IDLE_MIN_PARK_NS = 1000;

    /**
     * Maximum back-off park time which doubles on each interval stepping up from the min park idle.
     */
    public static final long IDLE_MAX_PARK_NS = TimeUnit.MILLISECONDS.toNanos(1);

    /**
     * {@link IdleStrategy} to be used when mode can be controlled via a counter.
     */
    public static final String CONTROLLABLE_IDLE_STRATEGY = "org.agrona.concurrent.ControllableIdleStrategy";

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String SENDER_IDLE_STRATEGY_PROP_NAME = "aeron.sender.idle.strategy";

    /**
     * Default idle strategy for the sender thread.
     */
    @Config
    public static final String SENDER_IDLE_STRATEGY_DEFAULT = DEFAULT_IDLE_STRATEGY;

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String RECEIVER_IDLE_STRATEGY_PROP_NAME = "aeron.receiver.idle.strategy";

    /**
     * Default idle strategy for the receiver thread.
     */
    @Config
    public static final String RECEIVER_IDLE_STRATEGY_DEFAULT = DEFAULT_IDLE_STRATEGY;

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link DriverConductor} for
     * {@link ThreadingMode#DEDICATED} and {@link ThreadingMode#SHARED_NETWORK}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String CONDUCTOR_IDLE_STRATEGY_PROP_NAME = "aeron.conductor.idle.strategy";

    /**
     * Default idle strategy for the conductor thread.
     */
    @Config
    public static final String CONDUCTOR_IDLE_STRATEGY_DEFAULT = DEFAULT_IDLE_STRATEGY;

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME = "aeron.sharednetwork.idle.strategy";

    /**
     * Default idle strategy for the shared network thread.
     */
    @Config
    public static final String SHARED_NETWORK_IDLE_STRATEGY_DEFAULT = DEFAULT_IDLE_STRATEGY;

    /**
     * Property name for {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver},
     * and {@link DriverConductor} for {@link ThreadingMode#SHARED}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String SHARED_IDLE_STRATEGY_PROP_NAME = "aeron.shared.idle.strategy";

    /**
     * Default idle strategy for the shared thread.
     */
    @Config
    public static final String SHARED_IDLE_STRATEGY_DEFAULT = DEFAULT_IDLE_STRATEGY;

    /**
     * Property name for {@link FlowControl} to be employed for unicast channels.
     */
    @Config(existsInC = false, hasContext = false)
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME = "aeron.unicast.flow.control.strategy";

    /**
     */
    @Config
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_DEFAULT = "io.aeron.driver.UnicastFlowControl";

    /**
     * {@link FlowControl} to be employed for unicast channels.
     */
    public static final String UNICAST_FLOW_CONTROL_STRATEGY = getProperty(
        UNICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, UNICAST_FLOW_CONTROL_STRATEGY_DEFAULT);

    /**
     * Property name for {@link FlowControl} to be employed for multicast channels.
     */
    @Config(existsInC = false, hasContext = false)
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME = "aeron.multicast.flow.control.strategy";

    /**
     */
    @Config
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_DEFAULT = "io.aeron.driver.MaxMulticastFlowControl";

    /**
     * {@link FlowControl} to be employed for multicast channels.
     */
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY = getProperty(
        MULTICAST_FLOW_CONTROL_STRATEGY_PROP_NAME, MULTICAST_FLOW_CONTROL_STRATEGY_DEFAULT);

    /**
     * Property name for {@link FlowControlSupplier} to be employed for unicast channels.
     */
    @Config(
        expectedCDefault = "aeron_unicast_flow_control_strategy_supplier",
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultUnicastFlowControlSupplier")
    public static final String UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME = "aeron.unicast.FlowControl.supplier";

    /**
     * Property name for {@link FlowControlSupplier} to be employed for unicast channels.
     */
    @Config(
        expectedCDefault = "aeron_max_multicast_flow_control_strategy_supplier",
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultMulticastFlowControlSupplier")
    public static final String MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME =
        "aeron.multicast.FlowControl.supplier";

    /**
     * Maximum UDP datagram payload size for IPv4. Jumbo datagrams from IPv6 are not supported.
     * <p>
     * Max length is 65,507 bytes as 65,535 minus 8 byte UDP header then minus 20 byte IP header.
     * Then round down to the nearest multiple of {@link FrameDescriptor#FRAME_ALIGNMENT} giving 65,504.
     */
    public static final int MAX_UDP_PAYLOAD_LENGTH = 65504;

    /**
     * Length of the maximum transmission unit of the media driver's protocol. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     */
    @Config
    public static final String MTU_LENGTH_PROP_NAME = "aeron.mtu.length";

    /**
     * The default is conservative to avoid fragmentation on IPv4 or IPv6 over Ethernet with PPPoE header,
     * or for clouds such as Google, Azure, and AWS.
     * <p>
     * On networks that suffer little congestion then a larger value can be used to reduce syscall costs.
     */
    @Config
    public static final int MTU_LENGTH_DEFAULT = 1408;

    /**
     * Length of the maximum transmission unit of the media driver's protocol for IPC. This can be larger than the
     * UDP version but if recorded replay needs to be considered.
     */
    @Config
    public static final String IPC_MTU_LENGTH_PROP_NAME = "aeron.ipc.mtu.length";

    /**
     */
    @Config
    public static final int IPC_MTU_LENGTH_DEFAULT = MTU_LENGTH_DEFAULT;

    /**
     * {@link ThreadingMode} to be used by the Aeron {@link MediaDriver}.
     */
    @Config(
        expectedCDefault = "AERON_THREADING_MODE_DEDICATED",
        defaultType = DefaultType.STRING,
        defaultString = "DEDICATED")
    public static final String THREADING_MODE_PROP_NAME = "aeron.threading.mode";

    /**
     * Interval between checks for timers and timeouts.
     */
    @Config
    public static final String TIMER_INTERVAL_PROP_NAME = "aeron.timer.interval";

    /**
     * Default interval between checks for timers and timeouts.
     */
    @Config(
        id = "TIMER_INTERVAL",
        expectedCDefaultFieldName = "AERON_TIMER_INTERVAL_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 1000 * 1000 * 1000)
    public static final long DEFAULT_TIMER_INTERVAL_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Timeout between a counter being freed and being available to be reused.
     */
    @Config
    public static final String COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME = "aeron.counters.free.to.reuse.timeout";

    /**
     * Default timeout between a counter being freed and being available to be reused.
     */
    @Config(
        id = "COUNTER_FREE_TO_REUSE_TIMEOUT",
        expectedCDefaultFieldName = "AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 1000 * 1000 * 1000)
    public static final long DEFAULT_COUNTER_FREE_TO_REUSE_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Property name for {@link SendChannelEndpointSupplier}.
     */
    @Config(
        existsInC = false,
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultSendChannelEndpointSupplier")
    public static final String SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.SendChannelEndpoint.supplier";

    /**
     * Property name for {@link ReceiveChannelEndpointSupplier}.
     */
    @Config(
        existsInC = false,
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultReceiveChannelEndpointSupplier")
    public static final String RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME = "aeron.ReceiveChannelEndpoint.supplier";

    /**
     * Property name for Application Specific Feedback added to Status Messages by the driver for flow control.
     * <p>
     * Replaced by {@link #RECEIVER_GROUP_TAG_PROP_NAME}.
     */
    @Deprecated
    @Config(defaultType = DefaultType.STRING, defaultString = "", existsInC = false)
    public static final String SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME =
        "aeron.flow.control.sm.applicationSpecificFeedback";

    /**
     * Property name for {@link CongestionControlSupplier} to be employed for receivers.
     */
    @Config(
        expectedCDefault = "aeron_congestion_control_default_strategy_supplier",
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultCongestionControlSupplier")
    public static final String CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME = "aeron.CongestionControl.supplier";

    /**
     * Property name for low end of the publication reserved session-id range which will not be automatically assigned.
     */
    @Config
    public static final String PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME =
        "aeron.publication.reserved.session.id.low";

    /**
     * Low-end of the publication reserved session-id range which will not be automatically assigned.
     */
    @Config
    public static final int PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT = -1;

    /**
     * High-end of the publication reserved session-id range which will not be automatically assigned.
     */
    @Config
    public static final String PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME =
        "aeron.publication.reserved.session.id.high";

    /**
     * High-end of the publication reserved session-id range which will not be automatically assigned.
     */
    @Config
    public static final int PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT = 1000;

    /**
     * Limit for the number of commands drained in one operation.
     */
    public static final int COMMAND_DRAIN_LIMIT = 1;

    /**
     * Capacity for the command queues used between driver agents.
     */
    public static final int CMD_QUEUE_CAPACITY = 1024;

    /**
     * Timeout on cleaning up pending SETUP message state on subscriber.
     */
    public static final long PENDING_SETUPS_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * Timeout between SETUP messages for publications during initial setup phase.
     */
    public static final long PUBLICATION_SETUP_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * Timeout between heartbeats for publications.
     */
    public static final long PUBLICATION_HEARTBEAT_TIMEOUT_NS = TimeUnit.MILLISECONDS.toNanos(100);

    /**
     * Expected size of multicast receiver groups property name.
     */
    @Config
    public static final String NAK_MULTICAST_GROUP_SIZE_PROP_NAME = "aeron.nak.multicast.group.size";

    /**
     * Default multicast receiver group size estimate for NAK delay randomisation.
     */
    @Config
    public static final int NAK_MULTICAST_GROUP_SIZE_DEFAULT = 10;

    /**
     * Max backoff time for multicast NAK delay randomisation in nanoseconds.
     */
    @Config
    public static final String NAK_MULTICAST_MAX_BACKOFF_PROP_NAME = "aeron.nak.multicast.max.backoff";

    /**
     * Default max backoff for NAK delay randomisation in nanoseconds.
     */
    @Config(
        id = "NAK_MULTICAST_MAX_BACKOFF",
        expectedCDefaultFieldName = "AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 10L * 1000 * 1000)
    public static final long NAK_MAX_BACKOFF_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Unicast NAK delay in nanoseconds property name.
     */
    @Config
    public static final String NAK_UNICAST_DELAY_PROP_NAME = "aeron.nak.unicast.delay";

    /**
     * Default Unicast NAK delay in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_NAK_UNICAST_DELAY_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 100 * 1000)
    public static final long NAK_UNICAST_DELAY_DEFAULT_NS = TimeUnit.MICROSECONDS.toNanos(100);

    /**
     * Unicast NAK retry delay ratio property name.
     */
    @Config
    public static final String NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME = "aeron.nak.unicast.retry.delay.ratio";

    /**
     * Default Unicast NAK retry delay ratio.
     */
    @Config
    public static final long NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT = 100;

    /**
     * Property for setting how long to delay before sending a retransmit after receiving a NAK.
     */
    @Config
    public static final String RETRANSMIT_UNICAST_DELAY_PROP_NAME = "aeron.retransmit.unicast.delay";

    /**
     * Default delay before retransmission of data for unicast in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 0)
    public static final long RETRANSMIT_UNICAST_DELAY_DEFAULT_NS = TimeUnit.NANOSECONDS.toNanos(0);

    /**
     * Property for setting how long to linger after delay on a NAK before responding to another NAK.
     */
    @Config
    public static final String RETRANSMIT_UNICAST_LINGER_PROP_NAME = "aeron.retransmit.unicast.linger";

    /**
     * Default delay for linger for unicast in nanoseconds.
     */
    @Config(
        expectedCDefaultFieldName = "AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT",
        defaultType = DefaultType.LONG,
        defaultLong = 10L * 1000 * 1000)
    public static final long RETRANSMIT_UNICAST_LINGER_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(10);

    /**
     * Property name of the timeout for when an untethered subscription that is outside the window limit will
     * participate in local flow control.
     */
    @Config
    public static final String UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME = "aeron.untethered.window.limit.timeout";

    /**
     * Property name of the timeout for an untethered subscription to stay in the linger state.
     */
    @Config
    public static final String UNTETHERED_LINGER_TIMEOUT_PROP_NAME = "aeron.untethered.linger.timeout";

    private static final long DEFAULT_UNSET_VALUE_WINDOW_LIMIT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5);
    /**
     * Default timeout for when an untethered subscription that is outside the window limit will participate in
     * local flow control.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 5_000_000_000L,
        expectedCDefaultFieldName = "AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT")
    public static final long UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS = DEFAULT_UNSET_VALUE_WINDOW_LIMIT_TIMEOUT_NS;

    /**
     * Default timeout for an untethered subscription to stay in the linger state.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 5_000_000_000L,
        expectedCDefaultFieldName = "AERON_UNTETHERED_LINGER_TIMEOUT_NS_DEFAULT")
    public static final long UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS = DEFAULT_UNSET_VALUE_WINDOW_LIMIT_TIMEOUT_NS;

    /**
     * Property name of the timeout for when an untethered subscription is resting after not being able to keep up
     * before it is allowed to rejoin a stream.
     */
    @Config
    public static final String UNTETHERED_RESTING_TIMEOUT_PROP_NAME = "aeron.untethered.resting.timeout";

    /**
     * Default timeout for when an untethered subscription is resting after not being able to keep up
     * before it is allowed to rejoin a stream.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 10_000_000_000L,
        expectedCDefaultFieldName = "AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT")
    public static final long UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(10);

    /**
     * Property name of the max number of active retransmissions tracked for udp streams with group semantics.
     */
    @Config
    public static final String MAX_RESEND_PROP_NAME = "aeron.max.resend";

    /**
     * Default max number of active retransmissions per connected stream udp stream with group semantics.
     */
    @Config
    public static final int MAX_RESEND_DEFAULT = 16;

    /**
     * Maximum value for the active retransmissions per connected stream udp stream with group semantics.
     */
    public static final int MAX_RESEND_MAX = 256;

    /**
     * Property name for the class used to validate if a driver should terminate based on token.
     */
    @Config(
        defaultType = DefaultType.STRING,
        defaultString = "io.aeron.driver.DefaultDenyTerminationValidator",
        expectedCDefault = "aeron_driver_termination_validator_default_deny",
        skipCDefaultValidation = true)
    public static final String TERMINATION_VALIDATOR_PROP_NAME = "aeron.driver.termination.validator";

    /**
     * Property name for default boolean value for if a stream can be rejoined. True to allow stream rejoin.
     */
    @Config(defaultType = DefaultType.BOOLEAN, defaultBoolean = true)
    public static final String REJOIN_STREAM_PROP_NAME = "aeron.rejoin.stream";

    /**
     * Property name for default group tag (gtag) to send in all Status Messages.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 0,
        expectedCDefaultFieldName = "AERON_RECEIVER_GROUP_TAG_VALUE_DEFAULT",
        expectedCDefault = "-1")
    public static final String RECEIVER_GROUP_TAG_PROP_NAME = "aeron.receiver.group.tag";

    /**
     * Property name for default group tag (gtag) used by the tagged flow control strategy to group receivers.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = -1)
    public static final String FLOW_CONTROL_GROUP_TAG_PROP_NAME = "aeron.flow.control.group.tag";

    /**
     * Property name for default minimum group size used by flow control strategies to determine connectivity.
     */
    @Config(defaultType = DefaultType.INT, defaultInt = 0)
    public static final String FLOW_CONTROL_GROUP_MIN_SIZE_PROP_NAME = "aeron.flow.control.group.min.size";

    /**
     * Default value for the receiver timeout used to determine if the receiver should still be monitored for
     * flow control purposes.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 5_000_000_000L)
    public static final long FLOW_CONTROL_RECEIVER_TIMEOUT_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Property name for flow control timeout after which with no status messages the receiver is considered gone.
     */
    @Config(
        expectedCEnvVarFieldName = "AERON_MIN_MULTICAST_FLOW_CONTROL_RECEIVER_TIMEOUT_ENV_VAR",
        expectedCEnvVar = "AERON_MIN_MULTICAST_FLOW_CONTROL_RECEIVER_TIMEOUT",
        expectedCDefaultFieldName = "AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT")
    public static final String FLOW_CONTROL_RECEIVER_TIMEOUT_PROP_NAME = "aeron.flow.control.receiver.timeout";

    /**
     */
    // TODO is this supposed to be deprecated?
    @Config(defaultType = DefaultType.LONG, defaultLong = 5_000_000_000L, hasContext = false, existsInC = false)
    private static final String MIN_FLOW_CONTROL_TIMEOUT_OLD_PROP_NAME =
        "aeron.MinMulticastFlowControl.receiverTimeout";

    /**
     * Property name for resolver name of the Media Driver used in name resolution.
     */
    @Config(defaultType = DefaultType.STRING, defaultString = "", skipCDefaultValidation = true)
    public static final String RESOLVER_NAME_PROP_NAME = "aeron.driver.resolver.name";

    /**
     * Property name for resolver interface to which network connections are made.
     *
     * @see #RESOLVER_BOOTSTRAP_NEIGHBOR_PROP_NAME
     */
    @Config(defaultType = DefaultType.STRING, defaultString = "", skipCDefaultValidation = true)
    public static final String RESOLVER_INTERFACE_PROP_NAME = "aeron.driver.resolver.interface";

    /**
     * Property name for resolver bootstrap neighbors for which it can bootstrap naming, format is comma separated list
     * of {@code hostname:port} pairs.
     *
     * @see #RESOLVER_INTERFACE_PROP_NAME
     */
    @Config(defaultType = DefaultType.STRING, defaultString = "", skipCDefaultValidation = true)
    public static final String RESOLVER_BOOTSTRAP_NEIGHBOR_PROP_NAME = "aeron.driver.resolver.bootstrap.neighbor";

    /**
     * Property name for re-resolution check interval for resolving names to IP address.
     */
    @Config
    public static final String RE_RESOLUTION_CHECK_INTERVAL_PROP_NAME = "aeron.driver.reresolution.check.interval";

    /**
     * Default value for the re-resolution check interval.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 1_000_000_000L,
        expectedCDefaultFieldName = "AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT")
    public static final long RE_RESOLUTION_CHECK_INTERVAL_DEFAULT_NS = TimeUnit.SECONDS.toNanos(1);

    /**
     * Property name for threshold value for the conductor work cycle threshold to track for being exceeded.
     */
    @Config
    public static final String CONDUCTOR_CYCLE_THRESHOLD_PROP_NAME = "aeron.driver.conductor.cycle.threshold";

    /**
     * Default threshold value for the conductor work cycle threshold to track for being exceeded.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 1_000_000_000L,
        expectedCDefaultFieldName = "AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_NS_DEFAULT")
    public static final long CONDUCTOR_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * Property name for threshold value for the sender work cycle threshold to track for being exceeded.
     */
    @Config
    public static final String SENDER_CYCLE_THRESHOLD_PROP_NAME = "aeron.driver.sender.cycle.threshold";

    /**
     * Default threshold value for the sender work cycle threshold to track for being exceeded.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 1_000_000_000L,
        expectedCDefaultFieldName = "AERON_DRIVER_SENDER_CYCLE_THRESHOLD_NS_DEFAULT")
    public static final long SENDER_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * Property name for threshold value for the receiver work cycle threshold to track for being exceeded.
     */
    @Config
    public static final String RECEIVER_CYCLE_THRESHOLD_PROP_NAME = "aeron.driver.receiver.cycle.threshold";

    /**
     * Default threshold value for the receiver work cycle threshold to track for being exceeded.
     */
    @Config(
        defaultType = DefaultType.LONG,
        defaultLong = 1_000_000_000,
        expectedCDefaultFieldName = "AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_NS_DEFAULT")
    public static final long RECEIVER_CYCLE_THRESHOLD_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(1000);

    /**
     * Property name for threshold value for the name resolution threshold to track for being exceeded.
     */
    @Config(
        expectedCEnvVarFieldName = "AERON_DRIVER_NAME_RESOLVER_THRESHOLD_ENV_VAR",
        expectedCEnvVar = "AERON_DRIVER_NAME_RESOLVER_THRESHOLD",
        expectedCDefaultFieldName = "AERON_DRIVER_NAME_RESOLVER_THRESHOLD_NS_DEFAULT")
    public static final String NAME_RESOLVER_THRESHOLD_PROP_NAME = "aeron.name.resolver.threshold";

    /**
     * Default threshold value for the name resolution threshold to track for being exceeded.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 5_000_000_000L)
    public static final long NAME_RESOLVER_THRESHOLD_DEFAULT_NS = TimeUnit.SECONDS.toNanos(5);

    /**
     * Property name for wildcard port range for the Sender.
     */
    @Config(
        defaultType = DefaultType.STRING,
        defaultString = "",
        expectedCEnvVarFieldName = "AERON_DRIVER_SENDER_WILDCARD_PORT_RANGE_ENV_VAR",
        skipCDefaultValidation = true)
    public static final String SENDER_WILDCARD_PORT_RANGE_PROP_NAME = "aeron.sender.wildcard.port.range";

    /**
     * Property name for wildcard port range for the Receiver.
     */
    @Config(
        defaultType = DefaultType.STRING,
        defaultString = "",
        expectedCEnvVarFieldName = "AERON_DRIVER_RECEIVER_WILDCARD_PORT_RANGE_ENV_VAR",
        skipCDefaultValidation = true)
    public static final String RECEIVER_WILDCARD_PORT_RANGE_PROP_NAME = "aeron.receiver.wildcard.port.range";

    /**
     * Property name to configure the number of async executor threads. Defaults to {@code 1}. Negative value or zero
     * means no asynchronous threads should be created, i.e. execution will be done on the conductor thread.
     *
     * @since 1.44.0
     */
    @Config(defaultType = DefaultType.INT, defaultInt = 1)
    public static final String ASYNC_TASK_EXECUTOR_THREADS_PROP_NAME = "aeron.driver.async.executor.threads";

    /**
     * Property name to set a limit on the number sessions allowed per stream on a subscription.
     */
    @Config(defaultType = DefaultType.INT, defaultInt = Integer.MAX_VALUE)
    public static final String STREAM_SESSION_LIMIT_PROP_NAME = "aeron.driver.stream.session.limit";

    /**
     * Default number of sessions allowed per stream on a subscription. Default is to be effectively unlimited.
     */
    public static final int STREAM_SESSION_LIMIT_DEFAULT = Integer.MAX_VALUE;

    /**
     * {@link Executor} that run tasks on the caller thread.
     */
    public static final Executor CALLER_RUNS_TASK_EXECUTOR = Runnable::run;

    /**
     * Should the high-resolution timer be used when running on Windows.
     *
     * @return true if the high-resolution timer be used when running on Windows.
     * @see #USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME
     */
    public static boolean useWindowsHighResTimer()
    {
        return "true".equals(getProperty(USE_WINDOWS_HIGH_RES_TIMER_PROP_NAME));
    }

    /**
     * Should a warning be printed if the aeron directory exist when starting.
     *
     * @return true if a warning be printed if the aeron directory exist when starting.
     * @see #DIR_WARN_IF_EXISTS_PROP_NAME
     */
    public static boolean warnIfDirExists()
    {
        return "true".equals(getProperty(DIR_WARN_IF_EXISTS_PROP_NAME));
    }

    /**
     * Should driver attempt to an immediate forced delete of {@link CommonContext#AERON_DIR_PROP_NAME} on start
     * if it exists.
     *
     * @return true if the aeron directory be deleted on start without checking if active.
     * @see #DIR_DELETE_ON_START_PROP_NAME
     */
    public static boolean dirDeleteOnStart()
    {
        return "true".equals(getProperty(DIR_DELETE_ON_START_PROP_NAME));
    }

    /**
     * Should driver attempt to delete {@link CommonContext#AERON_DIR_PROP_NAME} on shutdown.
     *
     * @return true if driver should attempt to delete {@link CommonContext#AERON_DIR_PROP_NAME} on shutdown.
     * @see #DIR_DELETE_ON_SHUTDOWN_PROP_NAME
     */
    public static boolean dirDeleteOnShutdown()
    {
        return "true".equals(getProperty(DIR_DELETE_ON_SHUTDOWN_PROP_NAME));
    }

    /**
     * Should term buffers be created as sparse files. This can save space at the expense of latency when required.
     *
     * @return true if term buffers should be created as sparse files.
     * @see #TERM_BUFFER_SPARSE_FILE_PROP_NAME
     */
    public static boolean termBufferSparseFile()
    {
        return "true".equals(getProperty(TERM_BUFFER_SPARSE_FILE_PROP_NAME, "true"));
    }

    /**
     * Default for if subscriptions should be tethered.
     *
     * @return true if the default subscriptions should be tethered.
     * @see #TETHER_SUBSCRIPTIONS_PROP_NAME
     */
    public static boolean tetherSubscriptions()
    {
        return "true".equals(getProperty(TETHER_SUBSCRIPTIONS_PROP_NAME, "true"));
    }

    /**
     * Default boolean value for if a stream is reliable. True to NAK, false to gap fill.
     *
     * @return true if NAK is default or false to gap fill.
     * @see #RELIABLE_STREAM_PROP_NAME
     */
    public static boolean reliableStream()
    {
        return "true".equals(getProperty(RELIABLE_STREAM_PROP_NAME, "true"));
    }

    /**
     * Should storage checks should be performed before allocating files.
     *
     * @return true of storage checks should be performed before allocating files.
     * @see #PERFORM_STORAGE_CHECKS_PROP_NAME
     */
    public static boolean performStorageChecks()
    {
        return "true".equals(getProperty(PERFORM_STORAGE_CHECKS_PROP_NAME, "true"));
    }

    /**
     * Should spy subscriptions simulate a connection to a network publication.
     * <p>
     * If true then this will override the min group size of the min and tagged flow control strategies.
     *
     * @return true if spy subscriptions should simulate a connection to a network publication.
     * @see #SPIES_SIMULATE_CONNECTION_PROP_NAME
     */
    public static boolean spiesSimulateConnection()
    {
        return "true".equals(getProperty(SPIES_SIMULATE_CONNECTION_PROP_NAME, "false"));
    }

    /**
     * Should subscriptions should be considered a group member or individual connection, e.g. multicast vs unicast.
     *
     * @return FORCE_TRUE if subscriptions should be considered a group member or false if individual.
     * @see #GROUP_RECEIVER_CONSIDERATION_PROP_NAME
     */
    public static CommonContext.InferableBoolean receiverGroupConsideration()
    {
        return CommonContext.InferableBoolean.parse(getProperty(GROUP_RECEIVER_CONSIDERATION_PROP_NAME));
    }

    /**
     * Length (in bytes) of the conductor buffer for control commands from the clients to the media driver conductor.
     *
     * @return length (in bytes) of the conductor buffer for control commands from the clients to the media driver.
     * @see #CONDUCTOR_BUFFER_LENGTH_PROP_NAME
     */
    public static int conductorBufferLength()
    {
        return getSizeAsInt(CONDUCTOR_BUFFER_LENGTH_PROP_NAME, CONDUCTOR_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * Length (in bytes) of the broadcast buffers from the media driver to the clients.
     *
     * @return length (in bytes) of the broadcast buffers from the media driver to the clients.
     * @see #TO_CLIENTS_BUFFER_LENGTH_PROP_NAME
     */
    public static int toClientsBufferLength()
    {
        return getSizeAsInt(TO_CLIENTS_BUFFER_LENGTH_PROP_NAME, TO_CLIENTS_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * Length of the buffer for the counters.
     * <p>
     * Each counter uses {@link org.agrona.concurrent.status.CountersReader#COUNTER_LENGTH} bytes.
     *
     * @return Length of the buffer for the counters.
     * @see #COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME
     */
    public static int counterValuesBufferLength()
    {
        return getSizeAsInt(COUNTERS_VALUES_BUFFER_LENGTH_PROP_NAME, COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * Length of the memory mapped buffer for the distinct error log.
     *
     * @return length of the memory mapped buffer for the distinct error log.
     */
    public static int errorBufferLength()
    {
        return getSizeAsInt(ERROR_BUFFER_LENGTH_PROP_NAME, ERROR_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * Expected size of typical multicast receiver groups.
     *
     * @return expected size of typical multicast receiver groups.
     * @see #NAK_MULTICAST_GROUP_SIZE_PROP_NAME
     */
    public static int nakMulticastGroupSize()
    {
        return getInteger(NAK_MULTICAST_GROUP_SIZE_PROP_NAME, NAK_MULTICAST_GROUP_SIZE_DEFAULT);
    }

    /**
     * Max backoff time for multicast NAK delay randomisation in nanoseconds.
     *
     * @return max backoff time for multicast NAK delay randomisation in nanoseconds.
     * @see #NAK_MULTICAST_MAX_BACKOFF_PROP_NAME
     */
    public static long nakMulticastMaxBackoffNs()
    {
        return getDurationInNanos(NAK_MULTICAST_MAX_BACKOFF_PROP_NAME, NAK_MAX_BACKOFF_DEFAULT_NS);
    }

    /**
     * Unicast NAK delay in nanoseconds.
     *
     * @return unicast NAK delay in nanoseconds.
     * @see #NAK_UNICAST_DELAY_PROP_NAME
     */
    public static long nakUnicastDelayNs()
    {
        return getDurationInNanos(NAK_UNICAST_DELAY_PROP_NAME, NAK_UNICAST_DELAY_DEFAULT_NS);
    }

    /**
     * Unicast NAK retry delay ratio.
     *
     * @return unicast NAK delay in nanoseconds.
     * @see #NAK_UNICAST_DELAY_PROP_NAME
     */
    public static long nakUnicastRetryDelayRatio()
    {
        final long ratio = getLong(NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME, NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT);
        validateValueRange(ratio, 1, Long.MAX_VALUE, NAK_UNICAST_RETRY_DELAY_RATIO_PROP_NAME);
        return ratio;
    }

    /**
     * Interval between checks for timers and timeouts.
     *
     * @return interval between checks for timers and timeouts.
     * @see #TIMER_INTERVAL_PROP_NAME
     */
    public static long timerIntervalNs()
    {
        return getDurationInNanos(TIMER_INTERVAL_PROP_NAME, DEFAULT_TIMER_INTERVAL_NS);
    }

    /**
     * Low file storage warning threshold in bytes for when performing storage checks.
     *
     * @return Low file storage warning threshold for when performing storage checks.
     * @see #LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME
     * @see #PERFORM_STORAGE_CHECKS_PROP_NAME
     */
    public static long lowStorageWarningThreshold()
    {
        return getSizeAsLong(LOW_FILE_STORE_WARNING_THRESHOLD_PROP_NAME, LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT);
    }

    /**
     * The window limit on UDP {@link Publication} side by which the publisher can get ahead of consumers.
     *
     * @return window limit on UDP {@link Publication} side by which the publisher can get ahead of consumers.
     * @see #PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
     */
    public static int publicationTermWindowLength()
    {
        return getSizeAsInt(PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);
    }

    /**
     * The window limit on IPC {@link Publication} side by which the publisher can get ahead of consumers.
     *
     * @return window limit on IPC {@link Publication} side by which the publisher can get ahead of consumers.
     * @see #IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME
     */
    public static int ipcPublicationTermWindowLength()
    {
        return getSizeAsInt(IPC_PUBLICATION_TERM_WINDOW_LENGTH_PROP_NAME, 0);
    }

    /**
     * The timeout for when an untethered subscription that is outside the window limit will participate in local
     * flow control.
     *
     * @return the timeout for when an untethered subscription that is outside the window limit will be included.
     * @see #UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME
     * @see #UNTETHERED_RESTING_TIMEOUT_PROP_NAME
     * @see #TETHER_SUBSCRIPTIONS_PROP_NAME
     */
    public static long untetheredWindowLimitTimeoutNs()
    {
        return getDurationInNanos(
            UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME, UNTETHERED_WINDOW_LIMIT_TIMEOUT_DEFAULT_NS);
    }

    /**
     * The timeout for an untethered subscription to remain in the linger state.
     *
     * @return the timeout for an untethered subscription to remain in the linger state.
     * @see #UNTETHERED_LINGER_TIMEOUT_PROP_NAME
     */
    public static long untetheredLingerTimeoutNs()
    {
        final String propertyValue = System.getProperty(UNTETHERED_LINGER_TIMEOUT_PROP_NAME);
        if (null == propertyValue)
        {
            return untetheredWindowLimitTimeoutNs();
        }
        else
        {
            return getDurationInNanos(
                UNTETHERED_LINGER_TIMEOUT_PROP_NAME, UNTETHERED_LINGER_TIMEOUT_DEFAULT_NS);
        }
    }

    /**
     * The timeout for when an untethered subscription is resting after not being able to keep up before it is allowed
     * to rejoin a stream.
     *
     * @return The timeout for when an untethered subscription is resting before rejoining a stream.
     * @see #UNTETHERED_RESTING_TIMEOUT_PROP_NAME
     * @see #UNTETHERED_WINDOW_LIMIT_TIMEOUT_PROP_NAME
     * @see #TETHER_SUBSCRIPTIONS_PROP_NAME
     */
    public static long untetheredRestingTimeoutNs()
    {
        return getDurationInNanos(UNTETHERED_RESTING_TIMEOUT_PROP_NAME, UNTETHERED_RESTING_TIMEOUT_DEFAULT_NS);
    }

    /**
     * Max number of active retransmissions tracked for udp streams with group semantics.
     *
     * @return max retransmits
     * @see #MAX_RESEND_PROP_NAME
     */
    public static int maxResend()
    {
        return Integer.min(
            Integer.max(getInteger(MAX_RESEND_PROP_NAME, MAX_RESEND_DEFAULT), 1),
            MAX_RESEND_MAX);
    }

    /**
     * Default boolean value for if a stream can be rejoined. True to allow stream rejoin, false to not.
     *
     * @return boolean value for if a stream can be rejoined. True to allow stream rejoin, false to not.
     * @see #REJOIN_STREAM_PROP_NAME
     */
    public static boolean rejoinStream()
    {
        return "true".equalsIgnoreCase(getProperty(REJOIN_STREAM_PROP_NAME, "true"));
    }

    /**
     * Default group tag (gtag) to send in all Status Messages. If not provided then no gtag is sent.
     *
     * @return Default group tag (gtag) to send in all Status Messages.
     * @see #RECEIVER_GROUP_TAG_PROP_NAME
     */
    public static Long groupTag()
    {
        return getLong(RECEIVER_GROUP_TAG_PROP_NAME, null);
    }

    /**
     * Default group tag (gtag) used by the tagged flow control strategy to group receivers.
     *
     * @return group tag (gtag) used by the tagged flow control strategy to group receivers.
     * @see #FLOW_CONTROL_GROUP_TAG_PROP_NAME
     */
    @SuppressWarnings("deprecation")
    public static long flowControlGroupTag()
    {
        final String propertyValue = getProperty(
            PreferredMulticastFlowControl.PREFERRED_ASF_PROP_NAME, PreferredMulticastFlowControl.PREFERRED_ASF_DEFAULT);
        final long legacyAsfValue = new UnsafeBuffer(BitUtil.fromHex(propertyValue)).getLong(0, LITTLE_ENDIAN);

        return getLong(FLOW_CONTROL_GROUP_TAG_PROP_NAME, legacyAsfValue);
    }

    /**
     * Default minimum group size used by flow control strategies to determine connectivity.
     *
     * @return default minimum group size used by flow control strategies to determine connectivity.
     * @see #FLOW_CONTROL_GROUP_MIN_SIZE_PROP_NAME
     */
    public static int flowControlGroupMinSize()
    {
        return getInteger(FLOW_CONTROL_GROUP_MIN_SIZE_PROP_NAME, 0);
    }

    /**
     * Flow control timeout after which with no status messages the receiver is considered gone.
     *
     * @return flow control timeout after which with no status messages the receiver is considered gone.
     * @see #FLOW_CONTROL_RECEIVER_TIMEOUT_PROP_NAME
     */
    public static long flowControlReceiverTimeoutNs()
    {
        return getDurationInNanos(
            FLOW_CONTROL_RECEIVER_TIMEOUT_PROP_NAME,
            getDurationInNanos(MIN_FLOW_CONTROL_TIMEOUT_OLD_PROP_NAME, FLOW_CONTROL_RECEIVER_TIMEOUT_DEFAULT_NS));
    }

    /**
     * Resolver name of the Media Driver used in name resolution.
     *
     * @return resolver name of the Media Driver used in name resolution.
     * @see #RESOLVER_NAME_PROP_NAME
     */
    public static String resolverName()
    {
        return getProperty(RESOLVER_NAME_PROP_NAME);
    }

    /**
     * Property name for resolver interface to which network connections are made, format is hostname:port.
     *
     * @return resolver interface to which network connections are made, format is hostname:port.
     * @see #RESOLVER_INTERFACE_PROP_NAME
     */
    public static String resolverInterface()
    {
        return getProperty(RESOLVER_INTERFACE_PROP_NAME);
    }

    /**
     * Resolver bootstrap neighbor for which it can bootstrap naming, format is hostname:port.
     *
     * @return resolver bootstrap neighbor for which it can bootstrap naming, format is hostname:port.
     * @see #RESOLVER_BOOTSTRAP_NEIGHBOR_PROP_NAME
     * @see #RESOLVER_INTERFACE_PROP_NAME
     */
    public static String resolverBootstrapNeighbor()
    {
        return getProperty(RESOLVER_BOOTSTRAP_NEIGHBOR_PROP_NAME);
    }

    /**
     * Re-resolution check interval for resolving names to IP address when they may have changed.
     *
     * @return re-resolution check interval for resolving names to IP address when they may have changed.
     * @see #RE_RESOLUTION_CHECK_INTERVAL_PROP_NAME
     */
    public static long reResolutionCheckIntervalNs()
    {
        return getDurationInNanos(RE_RESOLUTION_CHECK_INTERVAL_PROP_NAME, RE_RESOLUTION_CHECK_INTERVAL_DEFAULT_NS);
    }

    /**
     * How far ahead a producer can get from a consumer position.
     *
     * @param termBufferLength        for when default is not set and considering an appropriate minimum.
     * @param defaultTermWindowLength to take priority.
     * @return the length to be used for the producer window.
     */
    public static int producerWindowLength(final int termBufferLength, final int defaultTermWindowLength)
    {
        int termWindowLength = termBufferLength >> 1;

        if (0 != defaultTermWindowLength)
        {
            termWindowLength = Math.min(defaultTermWindowLength, termWindowLength);
        }

        return termWindowLength;
    }

    /**
     * Length to be used for the receiver window taking into account initial window length and term buffer length.
     *
     * @param termBufferLength    for the publication image.
     * @param initialWindowLength set for the channel.
     * @return the length to be used for the receiver window.
     */
    public static int receiverWindowLength(final int termBufferLength, final int initialWindowLength)
    {
        return Math.min(initialWindowLength, termBufferLength >> 1);
    }

    /**
     * Length (in bytes) of the log buffers for UDP publication terms.
     *
     * @return length (in bytes) of the log buffers for UDP publication terms.
     * @see #TERM_BUFFER_LENGTH_PROP_NAME
     */
    public static int termBufferLength()
    {
        return getSizeAsInt(TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * Length (in bytes) of the log buffers for IPC publication terms.
     *
     * @return length (in bytes) of the log buffers for IPC publication terms.
     * @see #IPC_TERM_BUFFER_LENGTH_PROP_NAME
     */
    public static int ipcTermBufferLength()
    {
        return getSizeAsInt(IPC_TERM_BUFFER_LENGTH_PROP_NAME, TERM_BUFFER_IPC_LENGTH_DEFAULT);
    }

    /**
     * Length of the initial window which must be sufficient for Bandwidth Delay Product (BDP).
     *
     * @return length of the initial window which must be sufficient for Bandwidth Delay Product (BDP).
     * @see #INITIAL_WINDOW_LENGTH_PROP_NAME
     */
    public static int initialWindowLength()
    {
        return getSizeAsInt(INITIAL_WINDOW_LENGTH_PROP_NAME, INITIAL_WINDOW_LENGTH_DEFAULT);
    }

    /**
     * SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     *
     * @return SO_SNDBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     * @see #SOCKET_SNDBUF_LENGTH_PROP_NAME
     */
    public static int socketSndbufLength()
    {
        return getSizeAsInt(SOCKET_SNDBUF_LENGTH_PROP_NAME, SOCKET_SNDBUF_LENGTH_DEFAULT);
    }

    /**
     * SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     *
     * @return SO_RCVBUF setting on UDP sockets which must be sufficient for Bandwidth Delay Product (BDP).
     * @see #SOCKET_RCVBUF_LENGTH_PROP_NAME
     */
    public static int socketRcvbufLength()
    {
        return getSizeAsInt(SOCKET_RCVBUF_LENGTH_PROP_NAME, SOCKET_RCVBUF_LENGTH_DEFAULT);
    }

    /**
     * Length of the maximum transmission unit of the media driver's protocol. If this is greater
     * than the network MTU for UDP then the packet will be fragmented and can amplify the impact of loss.
     *
     * @return length of the maximum transmission unit of the media driver's protocol.
     * @see #MTU_LENGTH_PROP_NAME
     */
    public static int mtuLength()
    {
        return getSizeAsInt(MTU_LENGTH_PROP_NAME, MTU_LENGTH_DEFAULT);
    }

    /**
     * Length of the maximum transmission unit of the media driver's protocol for IPC. This can be larger than the
     * UDP version but if recorded replay needs to be considered.
     *
     * @return length of the maximum transmission unit of the media driver's protocol for IPC.
     * @see #IPC_MTU_LENGTH_PROP_NAME
     */
    public static int ipcMtuLength()
    {
        return getSizeAsInt(IPC_MTU_LENGTH_PROP_NAME, IPC_MTU_LENGTH_DEFAULT);
    }

    /**
     * IP_MULTICAST_TTL setting on UDP sockets.
     *
     * @return IP_MULTICAST_TTL setting on UDP sockets.
     * @see #SOCKET_MULTICAST_TTL_PROP_NAME
     */
    public static int socketMulticastTtl()
    {
        return getInteger(SOCKET_MULTICAST_TTL_PROP_NAME, SOCKET_MULTICAST_TTL_DEFAULT);
    }

    /**
     * Page size in bytes to align all files to. The file system must support the requested size.
     *
     * @return page size in bytes to align all files to.
     * @see #FILE_PAGE_SIZE_PROP_NAME
     */
    public static int filePageSize()
    {
        return getSizeAsInt(FILE_PAGE_SIZE_PROP_NAME, FILE_PAGE_SIZE_DEFAULT);
    }

    /**
     * Low-end of the publication reserved session-id range which will not be automatically assigned.
     *
     * @return low-end of the publication reserved session-id range which will not be automatically assigned.
     * @see #PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME
     */
    public static int publicationReservedSessionIdLow()
    {
        return getInteger(PUBLICATION_RESERVED_SESSION_ID_LOW_PROP_NAME, PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT);
    }

    /**
     * High-end of the publication reserved session-id range which will not be automatically assigned.
     *
     * @return high-end of the publication reserved session-id range which will not be automatically assigned.
     * @see #PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME
     */
    public static int publicationReservedSessionIdHigh()
    {
        return getInteger(PUBLICATION_RESERVED_SESSION_ID_HIGH_PROP_NAME, PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT);
    }

    /**
     * Status message timeout in nanoseconds after which one will be sent when data flow has not triggered one.
     *
     * @return status message timeout in nanoseconds after which one will be sent.
     * @see #STATUS_MESSAGE_TIMEOUT_PROP_NAME
     */
    public static long statusMessageTimeoutNs()
    {
        return getDurationInNanos(STATUS_MESSAGE_TIMEOUT_PROP_NAME, STATUS_MESSAGE_TIMEOUT_DEFAULT_NS);
    }

    /**
     * Ratio of sending data to polling status messages in the {@link Sender}.
     *
     * @return ratio of sending data to polling status messages in the {@link Sender}.
     * @see #SEND_TO_STATUS_POLL_RATIO_PROP_NAME
     */
    public static int sendToStatusMessagePollRatio()
    {
        return getInteger(SEND_TO_STATUS_POLL_RATIO_PROP_NAME, SEND_TO_STATUS_POLL_RATIO_DEFAULT);
    }

    /**
     * Limit the number of driver managed resources that can be freed in the same duty cycle.
     *
     * @return limit of the number of resources.
     * @see #RESOURCE_FREE_LIMIT_PROP_NAME
     * @see #RESOURCE_FREE_LIMIT_DEFAULT
     */
    public static int resourceFreeLimit()
    {
        return getInteger(RESOURCE_FREE_LIMIT_PROP_NAME, RESOURCE_FREE_LIMIT_DEFAULT);
    }

    /**
     * Timeout between a counter being freed and being available to be reused.
     *
     * @return timeout between a counter being freed and being available to be reused.
     * @see #COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME
     */
    public static long counterFreeToReuseTimeoutNs()
    {
        return getDurationInNanos(COUNTER_FREE_TO_REUSE_TIMEOUT_PROP_NAME, DEFAULT_COUNTER_FREE_TO_REUSE_TIMEOUT_NS);
    }

    /**
     * {@link Aeron} client liveness timeout after which it is considered not alive.
     *
     * @return {@link Aeron} client liveness timeout after which it is considered not alive.
     * @see #CLIENT_LIVENESS_TIMEOUT_PROP_NAME
     */
    public static long clientLivenessTimeoutNs()
    {
        return getDurationInNanos(CLIENT_LIVENESS_TIMEOUT_PROP_NAME, CLIENT_LIVENESS_TIMEOUT_DEFAULT_NS);
    }

    /**
     * {@link Image} liveness timeout for how long it stays active without heartbeats or lingers around after being
     * drained.
     *
     * @return {@link Image} liveness timeout for how long it stays active without heartbeats or lingers around after
     * being drained.
     * @see #IMAGE_LIVENESS_TIMEOUT_PROP_NAME
     */
    public static long imageLivenessTimeoutNs()
    {
        return getDurationInNanos(IMAGE_LIVENESS_TIMEOUT_PROP_NAME, IMAGE_LIVENESS_TIMEOUT_DEFAULT_NS);
    }

    /**
     * {@link Publication} unblock timeout due to client crash or untimely commit.
     * <p>
     * A publication can become blocked if the client crashes while publishing or if
     * {@link io.aeron.Publication#tryClaim(int, BufferClaim)} is used without following up by calling
     * {@link BufferClaim#commit()} or {@link BufferClaim#abort()}.
     *
     * @return {@link Publication} unblock timeout due to client crash or untimely commit.
     * @see #PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME
     */
    public static long publicationUnblockTimeoutNs()
    {
        return getDurationInNanos(PUBLICATION_UNBLOCK_TIMEOUT_PROP_NAME, PUBLICATION_UNBLOCK_TIMEOUT_DEFAULT_NS);
    }

    /**
     * {@link Publication} timeout due to lack of status messages which indicate a connection.
     *
     * @return {@link Publication} timeout due to lack of status messages which indicate a connection.
     * @see #PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME
     */
    public static long publicationConnectionTimeoutNs()
    {
        return getDurationInNanos(PUBLICATION_CONNECTION_TIMEOUT_PROP_NAME, PUBLICATION_CONNECTION_TIMEOUT_DEFAULT_NS);
    }

    /**
     * Linger timeout after draining on {@link Publication}s so they can respond to NAKs.
     *
     * @return linger timeout after draining on {@link Publication}s so they can respond to NAKs.
     * @see #PUBLICATION_LINGER_PROP_NAME
     */
    public static long publicationLingerTimeoutNs()
    {
        return getDurationInNanos(PUBLICATION_LINGER_PROP_NAME, PUBLICATION_LINGER_DEFAULT_NS);
    }

    /**
     * Setting how long to delay before sending a retransmit after receiving a NAK.
     *
     * @return setting how long to delay before sending a retransmit after receiving a NAK.
     * @see #RETRANSMIT_UNICAST_DELAY_PROP_NAME
     */
    public static long retransmitUnicastDelayNs()
    {
        return getDurationInNanos(RETRANSMIT_UNICAST_DELAY_PROP_NAME, RETRANSMIT_UNICAST_DELAY_DEFAULT_NS);
    }

    /**
     * Setting how long to linger after delay on a NAK before responding to another NAK.
     *
     * @return setting how long to linger after delay on a NAK before responding to another NAK.
     * @see #RETRANSMIT_UNICAST_LINGER_PROP_NAME
     */
    public static long retransmitUnicastLingerNs()
    {
        return getDurationInNanos(RETRANSMIT_UNICAST_LINGER_PROP_NAME, RETRANSMIT_UNICAST_LINGER_DEFAULT_NS);
    }

    /**
     * Length of the memory mapped buffer for the {@link io.aeron.driver.reports.LossReport}.
     *
     * @return length of the memory mapped buffer for the {@link io.aeron.driver.reports.LossReport}.
     * @see #LOSS_REPORT_BUFFER_LENGTH_PROP_NAME
     */
    public static int lossReportBufferLength()
    {
        return getSizeAsInt(LOSS_REPORT_BUFFER_LENGTH_PROP_NAME, LOSS_REPORT_BUFFER_LENGTH_DEFAULT);
    }

    /**
     * {@link ThreadingMode} to be used by the Aeron {@link MediaDriver}. This allows for CPU resource to be traded
     * against throughput and latency.
     *
     * @return {@link ThreadingMode} to be used by the Aeron {@link MediaDriver}.
     * @see #THREADING_MODE_PROP_NAME
     */
    public static ThreadingMode threadingMode()
    {
        final String propertyValue = getProperty(THREADING_MODE_PROP_NAME);
        if (null == propertyValue)
        {
            return DEDICATED;
        }

        return ThreadingMode.valueOf(propertyValue);
    }

    /**
     * Get threshold value for the conductor work cycle threshold to track for being exceeded.
     *
     * @return threshold value in nanoseconds.
     */
    public static long conductorCycleThresholdNs()
    {
        return getDurationInNanos(CONDUCTOR_CYCLE_THRESHOLD_PROP_NAME, CONDUCTOR_CYCLE_THRESHOLD_DEFAULT_NS);
    }

    /**
     * Get threshold value for the sender work cycle threshold to track for being exceeded.
     *
     * @return threshold value in nanoseconds.
     */
    public static long senderCycleThresholdNs()
    {
        return getDurationInNanos(SENDER_CYCLE_THRESHOLD_PROP_NAME, SENDER_CYCLE_THRESHOLD_DEFAULT_NS);
    }

    /**
     * Get threshold value for the receiver work cycle threshold to track for being exceeded.
     *
     * @return threshold value in nanoseconds.
     */
    public static long receiverCycleThresholdNs()
    {
        return getDurationInNanos(RECEIVER_CYCLE_THRESHOLD_PROP_NAME, RECEIVER_CYCLE_THRESHOLD_DEFAULT_NS);
    }

    /**
     * Get threshold value for the name resolution time threshold to track for being exceeded.
     *
     * @return threshold value in nanoseconds.
     */
    public static long nameResolverThresholdNs()
    {
        return getDurationInNanos(NAME_RESOLVER_THRESHOLD_PROP_NAME, NAME_RESOLVER_THRESHOLD_DEFAULT_NS);
    }

    /**
     * Get wildcard port range in use for the Sender.
     *
     * @return port range as string with the format "low high"
     */
    public static String senderWildcardPortRange()
    {
        return getProperty(SENDER_WILDCARD_PORT_RANGE_PROP_NAME);
    }

    /**
     * Get wildcard port range in use for the Receiver.
     *
     * @return port range as string with the format "low high"
     */
    public static String receiverWildcardPortRange()
    {
        return getProperty(RECEIVER_WILDCARD_PORT_RANGE_PROP_NAME);
    }


    /**
     * Number of async executor threads.
     *
     * @return number of threads, defaults to one.
     * @see #ASYNC_TASK_EXECUTOR_THREADS_PROP_NAME
     * @since 1.44.0
     */
    public static int asyncTaskExecutorThreads()
    {
        return getInteger(ASYNC_TASK_EXECUTOR_THREADS_PROP_NAME, 1);
    }

    /**
     * Get the {@link IdleStrategy} that should be applied to {@link org.agrona.concurrent.Agent}s.
     *
     * @param strategyName       of the class to be created.
     * @param controllableStatus status indicator for what the strategy should do.
     * @return the newly created IdleStrategy.
     */
    public static IdleStrategy agentIdleStrategy(final String strategyName, final StatusIndicator controllableStatus)
    {
        IdleStrategy idleStrategy = null;

        switch (strategyName)
        {
            case NoOpIdleStrategy.ALIAS:
            case "org.agrona.concurrent.NoOpIdleStrategy":
                idleStrategy = NoOpIdleStrategy.INSTANCE;
                break;

            case BusySpinIdleStrategy.ALIAS:
            case "org.agrona.concurrent.BusySpinIdleStrategy":
                idleStrategy = BusySpinIdleStrategy.INSTANCE;
                break;

            case YieldingIdleStrategy.ALIAS:
            case "org.agrona.concurrent.YieldingIdleStrategy":
                idleStrategy = YieldingIdleStrategy.INSTANCE;
                break;

            case SleepingIdleStrategy.ALIAS:
            case "org.agrona.concurrent.SleepingIdleStrategy":
                idleStrategy = new SleepingIdleStrategy();
                break;

            case SleepingMillisIdleStrategy.ALIAS:
            case "org.agrona.concurrent.SleepingMillisIdleStrategy":
                idleStrategy = new SleepingMillisIdleStrategy();
                break;

            case BackoffIdleStrategy.ALIAS:
            case DEFAULT_IDLE_STRATEGY:
                idleStrategy = new BackoffIdleStrategy(
                    IDLE_MAX_SPINS, IDLE_MAX_YIELDS, IDLE_MIN_PARK_NS, IDLE_MAX_PARK_NS);
                break;

            case ControllableIdleStrategy.ALIAS:
            case CONTROLLABLE_IDLE_STRATEGY:
                Objects.requireNonNull(controllableStatus);
                controllableStatus.setOrdered(ControllableIdleStrategy.PARK);
                idleStrategy = new ControllableIdleStrategy(controllableStatus);
                break;

            default:
                try
                {
                    idleStrategy = (IdleStrategy)Class.forName(strategyName).getConstructor().newInstance();
                }
                catch (final Exception ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }
                break;
        }

        return idleStrategy;
    }

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     *
     * @param controllableStatus to allow control of {@link ControllableIdleStrategy}, which can be null if not used.
     * @return {@link IdleStrategy} to be employed by {@link Sender} for {@link ThreadingMode#DEDICATED}.
     * @see #SENDER_IDLE_STRATEGY_PROP_NAME
     */
    public static IdleStrategy senderIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(
            getProperty(SENDER_IDLE_STRATEGY_PROP_NAME, SENDER_IDLE_STRATEGY_DEFAULT), controllableStatus);
    }

    /**
     * {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     *
     * @param controllableStatus to allow control of {@link ControllableIdleStrategy}, which can be null if not used.
     * @return {@link IdleStrategy} to be employed by {@link Receiver} for {@link ThreadingMode#DEDICATED}.
     * @see #RECEIVER_IDLE_STRATEGY_PROP_NAME
     */
    public static IdleStrategy receiverIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(
            getProperty(RECEIVER_IDLE_STRATEGY_PROP_NAME, RECEIVER_IDLE_STRATEGY_DEFAULT), controllableStatus);
    }

    /**
     * {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED} and
     * {@link ThreadingMode#SHARED_NETWORK}.
     *
     * @param controllableStatus to allow control of {@link ControllableIdleStrategy}, which can be null if not used.
     * @return {@link IdleStrategy} to be employed by {@link DriverConductor} for {@link ThreadingMode#DEDICATED}
     * and {@link ThreadingMode#SHARED_NETWORK}..
     * @see #CONDUCTOR_IDLE_STRATEGY_PROP_NAME
     */
    public static IdleStrategy conductorIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(
            getProperty(CONDUCTOR_IDLE_STRATEGY_PROP_NAME, CONDUCTOR_IDLE_STRATEGY_DEFAULT), controllableStatus);
    }

    /**
     * {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     *
     * @param controllableStatus to allow control of {@link ControllableIdleStrategy}, which can be null if not used.
     * @return {@link IdleStrategy} to be employed by {@link Sender} and {@link Receiver} for
     * {@link ThreadingMode#SHARED_NETWORK}.
     * @see #SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME
     */
    public static IdleStrategy sharedNetworkIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(getProperty(SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME,
            SHARED_NETWORK_IDLE_STRATEGY_DEFAULT), controllableStatus);
    }

    /**
     * {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor} for
     * {@link ThreadingMode#SHARED}.
     *
     * @param controllableStatus to allow control of {@link ControllableIdleStrategy}, which can be null if not used.
     * @return {@link IdleStrategy} to be employed by {@link Sender}, {@link Receiver}, and {@link DriverConductor} for
     * {@link ThreadingMode#SHARED}.
     * @see #SHARED_IDLE_STRATEGY_PROP_NAME
     */
    public static IdleStrategy sharedIdleStrategy(final StatusIndicator controllableStatus)
    {
        return agentIdleStrategy(
            getProperty(SHARED_IDLE_STRATEGY_PROP_NAME, SHARED_IDLE_STRATEGY_DEFAULT), controllableStatus);
    }

    /**
     * @return Application Specific Feedback added to Status Messages by the driver for flow control.
     * @see #SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME
     * @deprecated see {@link #groupTag()}.
     */
    @Deprecated
    public static byte[] applicationSpecificFeedback()
    {
        final String propertyValue = getProperty(SM_APPLICATION_SPECIFIC_FEEDBACK_PROP_NAME);
        if (null == propertyValue)
        {
            return ArrayUtil.EMPTY_BYTE_ARRAY;
        }

        return fromHex(propertyValue);
    }

    /**
     * Get the supplier of {@link SendChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when sending to the channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    public static SendChannelEndpointSupplier sendChannelEndpointSupplier()
    {
        SendChannelEndpointSupplier supplier = null;
        try
        {
            final String className = getProperty(SEND_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME);
            if (null == className)
            {
                return new DefaultSendChannelEndpointSupplier();
            }

            supplier = (SendChannelEndpointSupplier)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link ReceiveChannelEndpoint}s which can be used for
     * debugging, monitoring, or modifying the behaviour when receiving from the channel.
     *
     * @return the {@link SendChannelEndpointSupplier}.
     */
    public static ReceiveChannelEndpointSupplier receiveChannelEndpointSupplier()
    {
        ReceiveChannelEndpointSupplier supplier = null;
        try
        {
            final String className = getProperty(RECEIVE_CHANNEL_ENDPOINT_SUPPLIER_PROP_NAME);
            if (null == className)
            {
                return new DefaultReceiveChannelEndpointSupplier();
            }

            supplier = (ReceiveChannelEndpointSupplier)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link FlowControl}s which can be used for changing behavior of flow control for unicast
     * publications.
     *
     * @return the {@link FlowControlSupplier}.
     */
    public static FlowControlSupplier unicastFlowControlSupplier()
    {
        FlowControlSupplier supplier = null;
        try
        {
            final String className = getProperty(UNICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME);
            if (null == className)
            {
                return new DefaultUnicastFlowControlSupplier();
            }

            supplier = (FlowControlSupplier)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link FlowControl}s which can be used for changing behavior of flow control for multicast
     * publications.
     *
     * @return the {@link FlowControlSupplier}.
     */
    public static FlowControlSupplier multicastFlowControlSupplier()
    {
        FlowControlSupplier supplier = null;
        try
        {
            final String className = getProperty(MULTICAST_FLOW_CONTROL_STRATEGY_SUPPLIER_PROP_NAME);
            if (null == className)
            {
                return new DefaultMulticastFlowControlSupplier();
            }

            supplier = (FlowControlSupplier)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the supplier of {@link CongestionControl} implementations which can be used for receivers.
     *
     * @return the {@link CongestionControlSupplier}
     */
    public static CongestionControlSupplier congestionControlSupplier()
    {
        CongestionControlSupplier supplier = null;
        try
        {
            final String className = getProperty(CONGESTION_CONTROL_STRATEGY_SUPPLIER_PROP_NAME);
            if (null == className)
            {
                return new DefaultCongestionControlSupplier();
            }

            supplier = (CongestionControlSupplier)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return supplier;
    }

    /**
     * Get the configured limit for the number of streams per session.
     *
     * @return configured session limit
     * @throws AsciiNumberFormatException if the property referenced by {@link #STREAM_SESSION_LIMIT_PROP_NAME} is not
     * a valid number
     */
    public static int streamSessionLimit()
    {
        final String streamSessionLimitString = getProperty(STREAM_SESSION_LIMIT_PROP_NAME);
        try
        {
            return Strings.isEmpty(streamSessionLimitString) ?
                STREAM_SESSION_LIMIT_DEFAULT : Integer.parseInt(streamSessionLimitString);
        }
        catch (final NumberFormatException ex)
        {
            throw new AsciiNumberFormatException(
                "Property " + STREAM_SESSION_LIMIT_PROP_NAME + "=" + streamSessionLimitString + " is not a number");
        }
    }

    /**
     * Validate that the initial window length is greater than MTU.
     *
     * @param initialWindowLength to be validated.
     * @param mtuLength           against which to validate.
     */
    public static void validateInitialWindowLength(final int initialWindowLength, final int mtuLength)
    {
        if (mtuLength > initialWindowLength)
        {
            throw new ConfigurationException(
                "mtuLength=" + mtuLength + " > initialWindowLength=" + initialWindowLength);
        }
    }

    /**
     * Validate that the MTU is an appropriate length. MTU lengths must be a multiple of
     * {@link FrameDescriptor#FRAME_ALIGNMENT}.
     *
     * @param mtuLength to be validated.
     * @throws ConfigurationException if the MTU length is not valid.
     */
    public static void validateMtuLength(final int mtuLength)
    {
        if (mtuLength <= DataHeaderFlyweight.HEADER_LENGTH)
        {
            throw new ConfigurationException(
                "mtuLength=" + mtuLength + " <= HEADER_LENGTH=" + DataHeaderFlyweight.HEADER_LENGTH);
        }

        if (mtuLength > MAX_UDP_PAYLOAD_LENGTH)
        {
            throw new ConfigurationException(
                "mtuLength=" + mtuLength + " > MAX_UDP_PAYLOAD_LENGTH=" + MAX_UDP_PAYLOAD_LENGTH);
        }

        if ((mtuLength & (FrameDescriptor.FRAME_ALIGNMENT - 1)) != 0)
        {
            throw new ConfigurationException(
                "mtuLength=" + mtuLength + " is not a multiple of FRAME_ALIGNMENT=" + FrameDescriptor.FRAME_ALIGNMENT);
        }
    }

    /**
     * Get the {@link TerminationValidator} implementations which can be used for validating a termination request
     * sent to the driver to ensure the client has the right to terminate a driver.
     *
     * @return the {@link TerminationValidator}
     */
    public static TerminationValidator terminationValidator()
    {
        TerminationValidator validator = null;
        try
        {
            final String className = getProperty(TERMINATION_VALIDATOR_PROP_NAME);
            if (null == className)
            {
                return new DefaultDenyTerminationValidator();
            }

            validator = (TerminationValidator)Class.forName(className).getConstructor().newInstance();
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return validator;
    }

    /**
     * Validate that the socket buffer lengths are sufficient for the media driver configuration.
     *
     * @param ctx to be validated.
     */
    public static void validateSocketBufferLengths(final MediaDriver.Context ctx)
    {
        if (ctx.osMaxSocketSndbufLength() < ctx.socketSndbufLength())
        {
            System.err.println(
                "WARNING: Could not set desired SO_SNDBUF, adjust OS to allow " + SOCKET_SNDBUF_LENGTH_PROP_NAME +
                " attempted=" + ctx.socketSndbufLength() + ", actual=" + ctx.osMaxSocketSndbufLength());
        }

        if (ctx.osMaxSocketRcvbufLength() < ctx.socketRcvbufLength())
        {
            System.err.println(
                "WARNING: Could not set desired SO_RCVBUF, adjust OS to allow " + SOCKET_RCVBUF_LENGTH_PROP_NAME +
                " attempted=" + ctx.socketRcvbufLength() + ", actual=" + ctx.osMaxSocketRcvbufLength());
        }

        final int soSndBuf = 0 == ctx.socketSndbufLength() ?
            ctx.osDefaultSocketSndbufLength() : ctx.socketSndbufLength();

        if (ctx.mtuLength() > soSndBuf)
        {
            throw new ConfigurationException(
                "mtuLength=" + ctx.mtuLength() + " > SO_SNDBUF=" + soSndBuf +
                ", increase " + SOCKET_SNDBUF_LENGTH_PROP_NAME + " to match MTU");
        }

        final int soRcvBuf = 0 == ctx.socketRcvbufLength() ?
            ctx.osDefaultSocketRcvbufLength() : ctx.socketRcvbufLength();

        if (ctx.initialWindowLength() > soRcvBuf)
        {
            throw new ConfigurationException(
                "initialWindowLength=" + ctx.initialWindowLength() + " > SO_RCVBUF=" + soRcvBuf +
                ", increase " + SOCKET_RCVBUF_LENGTH_PROP_NAME + " limits to match initialWindowLength");
        }
    }

    /**
     * Validate that page size is valid and alignment is valid.
     *
     * @param pageSize to be checked.
     * @throws ConfigurationException if the size is not as expected.
     */
    public static void validatePageSize(final int pageSize)
    {
        validateValueRange(pageSize, PAGE_MIN_SIZE, PAGE_MAX_SIZE, "filePageSize");

        if (!BitUtil.isPowerOfTwo(pageSize))
        {
            throw new ConfigurationException("filePageSize not a power of 2: " + pageSize);
        }
    }

    /**
     * Validate the range of session ids based on a high and low value provided which accounts for the values wrapping.
     *
     * @param low  value in the range.
     * @param high value in the range.
     * @throws ConfigurationException if the values are not valid.
     */
    public static void validateSessionIdRange(final int low, final int high)
    {
        if (low > high)
        {
            throw new ConfigurationException("low session id value " + low + " must be <= high value " + high);
        }

        if (Math.abs((long)high - low) > Integer.MAX_VALUE)
        {
            throw new ConfigurationException("reserved session range too large");
        }
    }

    /**
     * Compute the length of the {@link org.agrona.concurrent.status.CountersManager} metadata buffer based on the
     * length of the counters value buffer length.
     *
     * @param counterValuesBufferLength to compute the metadata buffer length from as a ratio.
     * @return the length that should be used for the metadata buffer for counters.
     */
    public static int countersMetadataBufferLength(final int counterValuesBufferLength)
    {
        return counterValuesBufferLength * (CountersReader.METADATA_LENGTH / CountersReader.COUNTER_LENGTH);
    }

    /**
     * Validate that the timeouts for unblocking publications from a client are valid.
     *
     * @param publicationUnblockTimeoutNs after which an uncommitted publication will be unblocked.
     * @param clientLivenessTimeoutNs     after which a client will be considered not alive.
     * @param timerIntervalNs             interval at which the driver will check timeouts.
     * @throws ConfigurationException if the values are not valid.
     */
    public static void validateUnblockTimeout(
        final long publicationUnblockTimeoutNs, final long clientLivenessTimeoutNs, final long timerIntervalNs)
    {
        if (publicationUnblockTimeoutNs <= clientLivenessTimeoutNs)
        {
            throw new ConfigurationException(
                "publicationUnblockTimeoutNs=" + publicationUnblockTimeoutNs +
                " <= clientLivenessTimeoutNs=" + clientLivenessTimeoutNs);
        }

        if (clientLivenessTimeoutNs <= timerIntervalNs)
        {
            throw new ConfigurationException(
                "clientLivenessTimeoutNs=" + clientLivenessTimeoutNs +
                " <= timerIntervalNs=" + timerIntervalNs);
        }
    }

    /**
     * Validate that the timeouts for untethered subscriptions are greater than timer interval.
     *
     * @param untetheredWindowLimitTimeoutNs after which an untethered subscription will be lingered.
     * @param untetheredRestingTimeoutNs     after which an untethered subscription that is lingered can become active.
     * @param timerIntervalNs                interval at which the driver will check timeouts.
     * @throws ConfigurationException if the values are not valid.
     */
    public static void validateUntetheredTimeouts(
        final long untetheredWindowLimitTimeoutNs, final long untetheredRestingTimeoutNs, final long timerIntervalNs)
    {
        if (untetheredWindowLimitTimeoutNs <= timerIntervalNs)
        {
            throw new ConfigurationException(
                "untetheredWindowLimitTimeoutNs=" + untetheredWindowLimitTimeoutNs +
                " <= timerIntervalNs=" + timerIntervalNs);
        }

        if (untetheredRestingTimeoutNs <= timerIntervalNs)
        {
            throw new ConfigurationException(
                "untetheredRestingTimeoutNs=" + untetheredRestingTimeoutNs +
                " <= timerIntervalNs=" + timerIntervalNs);
        }
    }

    /**
     * Create a source identity for a given source address.
     *
     * @param srcAddress to be used for the identity.
     * @return a source identity string for a given address.
     */
    public static String sourceIdentity(final InetSocketAddress srcAddress)
    {
        return srcAddress.getHostString() + ':' + srcAddress.getPort();
    }

    static void validateValueRange(final long value, final long minValue, final long maxValue, final String name)
    {
        if (value < minValue)
        {
            throw new ConfigurationException(
                name + " less than min size of " + minValue + ": " + value);
        }

        if (value > maxValue)
        {
            throw new ConfigurationException(
                name + " greater than max size of " + maxValue + ": " + value);
        }
    }
}
