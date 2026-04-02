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

import io.aeron.config.Config;

/**
 * Configuration options for the C media driver that have no Java system property equivalent.
 * <p>
 * These options are configured in the C driver via environment variables. They are documented here
 * for completeness and to ensure generated documentation reflects the full set of C driver options.
 * Java users configure the equivalent behavior directly via {@link MediaDriver.Context} setters.
 */
@Config(existsInJava = false, hasContext = false)
public final class CDriverConfiguration
{
    private CDriverConfiguration()
    {
    }

    // ------------------------------------------------------------------------------------------------
    // Idle Strategy Init Args
    // ------------------------------------------------------------------------------------------------

    /**
     * Arguments passed to the {@link org.agrona.concurrent.IdleStrategy} factory for the sender thread.
     * Configures the idle strategy specified by {@link Configuration#SENDER_IDLE_STRATEGY_PROP_NAME}.
     * {@code null} as there is no Java system property equivalent; Java configures the idle strategy
     * directly via {@link MediaDriver.Context#senderIdleStrategy(org.agrona.concurrent.IdleStrategy)}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String SENDER_IDLE_STRATEGY_INIT_ARGS_PROP_NAME = null;

    /**
     * Default init args for the sender idle strategy: none.
     */
    @Config
    public static final String SENDER_IDLE_STRATEGY_INIT_ARGS_DEFAULT = "NULL";

    /**
     * Arguments passed to the {@link org.agrona.concurrent.IdleStrategy} factory for the conductor thread.
     * Configures the idle strategy specified by {@link Configuration#CONDUCTOR_IDLE_STRATEGY_PROP_NAME}.
     * {@code null} as there is no Java system property equivalent; Java configures the idle strategy
     * directly via {@link MediaDriver.Context#conductorIdleStrategy(org.agrona.concurrent.IdleStrategy)}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_PROP_NAME = null;

    /**
     * Default init args for the conductor idle strategy: none.
     */
    @Config
    public static final String CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_DEFAULT = "NULL";

    /**
     * Arguments passed to the {@link org.agrona.concurrent.IdleStrategy} factory for the receiver thread.
     * Configures the idle strategy specified by {@link Configuration#RECEIVER_IDLE_STRATEGY_PROP_NAME}.
     * {@code null} as there is no Java system property equivalent; Java configures the idle strategy
     * directly via {@link MediaDriver.Context#receiverIdleStrategy(org.agrona.concurrent.IdleStrategy)}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String RECEIVER_IDLE_STRATEGY_INIT_ARGS_PROP_NAME = null;

    /**
     * Default init args for the receiver idle strategy: none.
     */
    @Config
    public static final String RECEIVER_IDLE_STRATEGY_INIT_ARGS_DEFAULT = "NULL";

    /**
     * Arguments passed to the {@link org.agrona.concurrent.IdleStrategy} factory for the shared network thread
     * (sender and receiver) when running in {@link ThreadingMode#SHARED_NETWORK}.
     * Configures the idle strategy specified by {@link Configuration#SHARED_NETWORK_IDLE_STRATEGY_PROP_NAME}.
     * {@code null} as there is no Java system property equivalent; Java configures the idle strategy
     * directly via {@link MediaDriver.Context#sharedNetworkIdleStrategy(org.agrona.concurrent.IdleStrategy)}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_PROP_NAME = null;

    /**
     * Default init args for the shared network idle strategy: none.
     */
    @Config
    public static final String SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_DEFAULT = "NULL";

    /**
     * Arguments passed to the {@link org.agrona.concurrent.IdleStrategy} factory for the shared thread
     * when running in {@link ThreadingMode#SHARED}.
     * Configures the idle strategy specified by {@link Configuration#SHARED_IDLE_STRATEGY_PROP_NAME}.
     * {@code null} as there is no Java system property equivalent; Java configures the idle strategy
     * directly via {@link MediaDriver.Context#sharedIdleStrategy(org.agrona.concurrent.IdleStrategy)}.
     * <p>
     * Note: the C environment variable field name for this option uses a non-standard naming convention
     * ({@code AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR}).
     */
    @Config(
        expectedCEnvVarFieldName = "AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR",
        skipCDefaultValidation = true)
    public static final String SHARED_IDLE_STRATEGY_INIT_ARGS_PROP_NAME = null;

    /**
     * Default init args for the shared idle strategy: none.
     */
    @Config
    public static final String SHARED_IDLE_STRATEGY_INIT_ARGS_DEFAULT = "NULL";

    // ------------------------------------------------------------------------------------------------
    // Name Resolver
    // ------------------------------------------------------------------------------------------------

    /**
     * Name of the name resolver supplier to use for the media driver. The value {@code "default"} selects
     * the built-in driver name resolver. Other values load a resolver by name from a dynamic library.
     * {@code null} as there is no Java system property equivalent; Java uses
     * {@link MediaDriver.Context#nameResolver(NameResolver)} directly.
     */
    @Config(expectedCDefaultFieldName = "AERON_NAME_RESOLVER_SUPPLIER_DEFAULT")
    public static final String NAME_RESOLVER_SUPPLIER_PROP_NAME = null;

    /**
     * Default name resolver supplier: the built-in driver name resolver.
     */
    @Config(expectedCDefaultFieldName = "AERON_NAME_RESOLVER_SUPPLIER_DEFAULT")
    public static final String NAME_RESOLVER_SUPPLIER_DEFAULT = "default";

    /**
     * Initialisation arguments passed to the name resolver supplier. The format is resolver-specific.
     * {@code null} as there is no Java system property equivalent; Java configures the name resolver
     * directly via {@link MediaDriver.Context#nameResolver(NameResolver)}.
     */
    @Config(skipCDefaultValidation = true)
    public static final String NAME_RESOLVER_INIT_ARGS_PROP_NAME = null;

    /**
     * Default name resolver init args: none.
     */
    @Config
    public static final String NAME_RESOLVER_INIT_ARGS_DEFAULT = "NULL";

    // ------------------------------------------------------------------------------------------------
    // UDP Channel Transport Bindings and Interceptors
    // ------------------------------------------------------------------------------------------------

    /**
     * Name of the UDP channel transport bindings implementation to use for sender and receiver media channels.
     * The value {@code "default"} selects the standard OS socket transport. Other values load a transport
     * binding by name from a dynamic library, enabling custom network transports.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config
    public static final String UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_PROP_NAME = null;

    /**
     * Default UDP channel transport bindings: the standard OS socket transport.
     */
    @Config
    public static final String UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT = "default";

    /**
     * Name of the UDP channel transport bindings implementation to use for conductor media channels.
     * The value {@code "default"} selects the standard OS socket transport.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config(expectedCDefaultFieldName = "AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT")
    public static final String CONDUCTOR_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_PROP_NAME = null;

    /**
     * Default conductor UDP channel transport bindings: the standard OS socket transport.
     */
    @Config
    public static final String CONDUCTOR_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT = "default";

    /**
     * Comma-separated list of outgoing UDP channel interceptor bindings to apply to all media channels.
     * Interceptors are loaded by name from dynamic libraries and are applied in order.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config(skipCDefaultValidation = true)
    public static final String UDP_CHANNEL_OUTGOING_INTERCEPTORS_PROP_NAME = null;

    /**
     * Default outgoing UDP channel interceptors: none.
     */
    @Config
    public static final String UDP_CHANNEL_OUTGOING_INTERCEPTORS_DEFAULT = "null";

    /**
     * Comma-separated list of incoming UDP channel interceptor bindings to apply to all media channels.
     * Interceptors are loaded by name from dynamic libraries and are applied in order.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config(skipCDefaultValidation = true)
    public static final String UDP_CHANNEL_INCOMING_INTERCEPTORS_PROP_NAME = null;

    /**
     * Default incoming UDP channel interceptors: none.
     */
    @Config
    public static final String UDP_CHANNEL_INCOMING_INTERCEPTORS_DEFAULT = "null";

    // ------------------------------------------------------------------------------------------------
    // I/O Vector Capacity
    // ------------------------------------------------------------------------------------------------

    /**
     * Maximum number of messages batched per I/O vector when the receiver reads from the network.
     * Higher values can improve throughput at the cost of increased latency.
     * {@code null} as there is no Java system property equivalent; the Java driver uses
     * a fixed internal batch size.
     */
    @Config
    public static final String RECEIVER_IO_VECTOR_CAPACITY_PROP_NAME = null;

    /**
     * Default receiver I/O vector capacity.
     */
    @Config
    public static final int RECEIVER_IO_VECTOR_CAPACITY_DEFAULT = 4;

    /**
     * Maximum number of messages batched per I/O vector when the sender writes to the network.
     * Higher values can improve throughput at the cost of increased latency.
     * {@code null} as there is no Java system property equivalent; the Java driver uses
     * a fixed internal batch size.
     */
    @Config
    public static final String SENDER_IO_VECTOR_CAPACITY_PROP_NAME = null;

    /**
     * Default sender I/O vector capacity.
     */
    @Config
    public static final int SENDER_IO_VECTOR_CAPACITY_DEFAULT = 4;

    /**
     * Maximum number of messages sent per duty cycle for a network publication.
     * Limits the number of datagrams sent in a single sender duty cycle iteration to bound jitter.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config
    public static final String NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_PROP_NAME = null;

    /**
     * Default maximum messages per send for a network publication.
     */
    @Config
    public static final int NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_DEFAULT = 4;

    // ------------------------------------------------------------------------------------------------
    // CPU Affinity
    // ------------------------------------------------------------------------------------------------

    /**
     * CPU core number to pin the conductor thread to. A value of {@code -1} means no affinity is set
     * and the OS scheduler decides placement.
     * {@code null} as there is no Java system property equivalent; CPU affinity is a C-only capability.
     */
    @Config(expectedCDefaultFieldName = "AERON_CPU_AFFINITY_DEFAULT")
    public static final String CONDUCTOR_CPU_AFFINITY_PROP_NAME = null;

    /**
     * Default conductor CPU affinity: no affinity ({@code -1}).
     */
    @Config
    public static final int CONDUCTOR_CPU_AFFINITY_DEFAULT = -1;

    /**
     * CPU core number to pin the sender thread to. A value of {@code -1} means no affinity is set
     * and the OS scheduler decides placement.
     * {@code null} as there is no Java system property equivalent; CPU affinity is a C-only capability.
     */
    @Config(expectedCDefaultFieldName = "AERON_CPU_AFFINITY_DEFAULT")
    public static final String SENDER_CPU_AFFINITY_PROP_NAME = null;

    /**
     * Default sender CPU affinity: no affinity ({@code -1}).
     */
    @Config
    public static final int SENDER_CPU_AFFINITY_DEFAULT = -1;

    /**
     * CPU core number to pin the receiver thread to. A value of {@code -1} means no affinity is set
     * and the OS scheduler decides placement.
     * {@code null} as there is no Java system property equivalent; CPU affinity is a C-only capability.
     */
    @Config(expectedCDefaultFieldName = "AERON_CPU_AFFINITY_DEFAULT")
    public static final String RECEIVER_CPU_AFFINITY_PROP_NAME = null;

    /**
     * Default receiver CPU affinity: no affinity ({@code -1}).
     */
    @Config
    public static final int RECEIVER_CPU_AFFINITY_DEFAULT = -1;

    // ------------------------------------------------------------------------------------------------
    // Dynamic Libraries
    // ------------------------------------------------------------------------------------------------

    /**
     * Colon-separated list of dynamic library paths to load when the C driver starts. Libraries are loaded
     * in order and may register custom transport bindings, interceptors, congestion control suppliers,
     * or name resolver suppliers.
     * {@code null} as there is no Java system property equivalent; dynamic library loading is a
     * C-only capability.
     */
    @Config(skipCDefaultValidation = true)
    public static final String DRIVER_DYNAMIC_LIBRARIES_PROP_NAME = null;

    /**
     * Default dynamic libraries: none.
     */
    @Config
    public static final String DRIVER_DYNAMIC_LIBRARIES_DEFAULT = "NULL";

    // ------------------------------------------------------------------------------------------------
    // Driver Connect
    // ------------------------------------------------------------------------------------------------

    /**
     * Whether the C driver should attempt to connect to an already-running driver instance in the
     * Aeron directory, rather than starting a new one. When {@code false} the driver always starts fresh.
     * {@code null} as there is no Java system property equivalent; Java manages driver lifecycle
     * differently via {@link MediaDriver#launch(MediaDriver.Context)}.
     */
    @Config
    public static final String DRIVER_CONNECT_PROP_NAME = null;

    /**
     * Default driver connect behaviour: enabled ({@code true}).
     */
    @Config
    public static final boolean DRIVER_CONNECT_DEFAULT = true;

    // ------------------------------------------------------------------------------------------------
    // Agent On Start Function
    // ------------------------------------------------------------------------------------------------

    /**
     * Name of a C function to call when an agent thread starts. The function must have the signature
     * {@code void fn(void *state, const char *role_name)} and be accessible from a loaded dynamic library.
     * {@code null} as there is no Java system property equivalent; this is a C-only extensibility hook.
     */
    @Config(skipCDefaultValidation = true)
    public static final String AGENT_ON_START_FUNCTION_PROP_NAME = null;

    /**
     * Default agent on-start function: none.
     */
    @Config
    public static final String AGENT_ON_START_FUNCTION_DEFAULT = "NULL";

    // ------------------------------------------------------------------------------------------------
    // Cubic Congestion Control
    // ------------------------------------------------------------------------------------------------

    /**
     * Whether the Cubic congestion control implementation should actively measure round-trip time (RTT)
     * by sending RTT measurement frames. When disabled, the initial RTT estimate
     * ({@link #CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT}) is used throughout the session.
     * Only applies when {@code cc=cubic} is configured on a channel.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config
    public static final String CUBICCONGESTIONCONTROL_MEASURERTT_PROP_NAME = null;

    /**
     * Default for Cubic congestion control RTT measurement: disabled ({@code false}).
     */
    @Config
    public static final boolean CUBICCONGESTIONCONTROL_MEASURERTT_DEFAULT = false;

    /**
     * Initial RTT estimate in nanoseconds used by the Cubic congestion control algorithm before
     * any measurements are available. Only applies when {@code cc=cubic} is configured on a channel.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config(isTimeValue = Config.IsTimeValue.TRUE)
    public static final String CUBICCONGESTIONCONTROL_INITIALRTT_PROP_NAME = null;

    /**
     * Default initial RTT estimate for Cubic congestion control: 100 microseconds.
     */
    @Config
    public static final long CUBICCONGESTIONCONTROL_INITIALRTT_DEFAULT = 100_000L;

    /**
     * Whether the Cubic congestion control implementation should operate in TCP-friendly mode,
     * which limits the window growth to match what standard TCP CUBIC would achieve.
     * Only applies when {@code cc=cubic} is configured on a channel.
     * {@code null} as there is no Java system property equivalent.
     */
    @Config
    public static final String CUBICCONGESTIONCONTROL_TCPMODE_PROP_NAME = null;

    /**
     * Default for Cubic congestion control TCP-friendly mode: disabled ({@code false}).
     */
    @Config
    public static final boolean CUBICCONGESTIONCONTROL_TCPMODE_DEFAULT = false;
}
