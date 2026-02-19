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

import io.aeron.counter.AeronCounter;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.Objects;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.LABEL_OFFSET;
import static org.agrona.concurrent.status.CountersReader.MAX_LABEL_LENGTH;
import static org.agrona.concurrent.status.CountersReader.METADATA_LENGTH;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.REFERENCE_ID_OFFSET;
import static org.agrona.concurrent.status.CountersReader.counterOffset;
import static org.agrona.concurrent.status.CountersReader.metaDataOffset;

/**
 * This class serves as a registry for all counter type IDs used by Aeron.
 * <p>
 * Type IDs less than 1000 are reserved for Aeron use. Any custom counters should use a typeId of 1000 or higher.
 * <p>
 * Aeron uses the following specific ranges:
 * <ul>
 *     <li>{@code 0 - 99}: for client/driver counters.</li>
 *     <li>{@code 100 - 199}: for archive counters.</li>
 *     <li>{@code 200 - 299}: for cluster counters.</li>
 * </ul>
 */
public final class AeronCounters
{
    // System counter IDs to be accessed outside the driver.
    /**
     * Counter id for bytes sent over the network.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_BYTES_SENT = 0;

    /**
     * Counter id for bytes sent over the network.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_BYTES_RECEIVED = 1;

    /**
     * Counter id for failed offers to the receiver proxy.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS = 2;

    /**
     * Counter id for failed offers to the sender proxy.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS = 3;

    /**
     * Counter id for failed offers to the conductor proxy.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CONDUCTOR_PROXY_FAILS = 4;

    /**
     * Counter id for NAKs sent back to senders requesting re-transmits.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT = 5;

    /**
     * Counter id for NAKs received from receivers requesting re-transmits.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED = 6;

    /**
     * Counter id for status messages sent back to senders for flow control.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT = 7;

    /**
     * Counter id for status messages received from receivers for flow control.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED = 8;

    /**
     * Counter id for heartbeat data frames sent to indicate liveness in the absence of data to send.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_HEARTBEATS_SENT = 9;

    /**
     * Counter id for heartbeat data frames received to indicate liveness in the absence of data to send.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED = 10;

    /**
     * Counter id for data packets re-transmitted as a result of NAKs.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RETRANSMITS_SENT = 11;

    /**
     * Counter id for packets received which under-run the current flow control window for images.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS = 12;

    /**
     * Counter id for packets received which over-run the current flow control window for images.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS = 13;

    /**
     * Counter id for invalid packets received.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_INVALID_PACKETS = 14;

    /**
     * Counter id for errors observed by the driver and an indication to read the distinct error log.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_ERRORS = 15;

    /**
     * Counter id for socket send operation which resulted in less than the packet length being sent.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_SHORT_SENDS = 16;

    /**
     * Counter id for attempts to free log buffers no longer required by the driver which as still held by clients.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_FREE_FAILS = 17;

    /**
     * Counter id for the times a sender has entered the state of being back-pressured when it could have sent faster.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_SENDER_FLOW_CONTROL_LIMITS = 18;

    /**
     * Counter id for the times a publication has been unblocked after a client failed to complete an offer within a
     * timeout.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_UNBLOCKED_PUBLICATIONS = 19;

    /**
     * Counter id for the times a command has been unblocked after a client failed to complete an offer within a
     * timeout.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS = 20;

    /**
     * Counter id for the times the channel endpoint detected a possible TTL asymmetry between its config and new
     * connection.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_POSSIBLE_TTL_ASYMMETRY = 21;

    /**
     * Counter id for status of the {@link org.agrona.concurrent.ControllableIdleStrategy} if configured.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CONTROLLABLE_IDLE_STRATEGY = 22;

    /**
     * Counter id for the times a loss gap has been filled when NAKs have been disabled.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_LOSS_GAP_FILLS = 23;

    /**
     * Counter id for the Aeron clients that have timed out without a graceful close.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CLIENT_TIMEOUTS = 24;

    /**
     * Counter id for the times a connection endpoint has been re-resolved resulting in a change.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RESOLUTION_CHANGES = 25;

    /**
     * Counter id for the maximum time spent by the conductor between work cycles.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME = 26;

    /**
     * Counter id for the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED = 27;

    /**
     * Counter id for the maximum time spent by the sender between work cycles.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME = 28;

    /**
     * Counter id for the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED = 29;

    /**
     * Counter id for the maximum time spent by the receiver between work cycles.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME = 30;

    /**
     * Counter id for the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED = 31;

    /**
     * Counter id for the maximum time spent by the NameResolver in one of its operations.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_NAME_RESOLVER_MAX_TIME = 32;

    /**
     * Counter id for the number of times the time threshold has been exceeded by the NameResolver.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED = 33;

    /**
     * Counter id for the version of the media driver.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_AERON_VERSION = 34;

    /**
     * Counter id for the total number of bytes currently mapped in log buffers, CnC file, and loss report.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_BYTES_CURRENTLY_MAPPED = 35;

    /**
     * Counter id for the minimum bound on the number of bytes re-transmitted as a result of NAKs.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES = 36;

    /**
     * Counter id for the number of times that the retransmit pool has been overflowed.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_RETRANSMIT_OVERFLOW = 37;

    /**
     * Counter id for the number of error frames received by this driver.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_ERROR_FRAMES_RECEIVED = 38;

    /**
     * Counter id for the number of error frames sent by this driver.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_ERROR_FRAMES_SENT = 39;

    /**
     * Counter id for the number of publications that have been revoked.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED = 40;

    /**
     * Counter id for the number of publication images that have been revoked.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED = 41;

    /**
     * Counter id for the number of images that have been rejected.
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_IMAGES_REJECTED = 42;

    /**
     * Counter id for the control protocol between clients and media driver.
     *
     * @since 1.49.0
     */
    @AeronCounter
    public static final int SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION = 43;

    // Client/driver counters
    /**
     * System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
     */
    @AeronCounter
    public static final int DRIVER_SYSTEM_COUNTER_TYPE_ID = 0;

    /**
     * The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will
     * experience back pressure when this position is passed as a means of flow control.
     */
    @AeronCounter
    public static final int DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1;

    /**
     * The position the Sender has reached for sending data to the media on a session-channel-stream tuple.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_POSITION_TYPE_ID = 2;

    /**
     * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
     * It is possible the stream is not complete to this point if the stream has experienced loss.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVER_HWM_TYPE_ID = 3;
    /**
     * The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have
     * multiple
     */
    @AeronCounter(expectedCName = "SUBSCRIPTION_POSITION")
    public static final int DRIVER_SUBSCRIBER_POSITION_TYPE_ID = 4;

    /**
     * The highest position the Receiver has rebuilt up to on a session-channel-stream tuple while rebuilding the
     * stream.The stream is complete up to this point.
     */
    @AeronCounter(expectedCName = "RECEIVER_POSITION")
    public static final int DRIVER_RECEIVER_POS_TYPE_ID = 5;

    /**
     * The status of a send-channel-endpoint represented as a counter value.
     */
    @AeronCounter
    public static final int DRIVER_SEND_CHANNEL_STATUS_TYPE_ID = 6;

    /**
     * The status of a receive-channel-endpoint represented as a counter value.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

    /**
     * The position the Sender can immediately send up-to on a session-channel-stream tuple.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_LIMIT_TYPE_ID = 9;

    /**
     * A counter per Image indicating presence of the congestion control.
     */
    @AeronCounter
    public static final int DRIVER_PER_IMAGE_TYPE_ID = 10;

    /**
     * A counter for tracking the last heartbeat of an entity with a given registration id.
     */
    @AeronCounter(expectedCName = "CLIENT_HEARTBEAT_TIMESTAMP")
    public static final int DRIVER_HEARTBEAT_TYPE_ID = 11;

    /**
     * The position in bytes a publication has reached appending to the log.
     * <p>
     * <b>Note:</b> This is a not a real-time value like the other and is updated one per second for monitoring
     * purposes.
     */
    @AeronCounter(expectedCName = "PUBLISHER_POSITION")
    public static final int DRIVER_PUBLISHER_POS_TYPE_ID = 12;

    /**
     * Count of back-pressure events (BPE)s a sender has experienced on a stream.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_BPE_TYPE_ID = 13;

    /**
     * Counter used to store the status of a bind address and port for the local end of a channel.
     * <p>
     * When the value is {@link ChannelEndpointStatus#ACTIVE} then the key value and label will be updated with the
     * socket address and port which is bound.
     */
    @AeronCounter(expectedCName = "LOCAL_SOCKADDR")
    public static final int DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

    /**
     * Count of media driver neighbors for name resolution.
     */
    @AeronCounter
    public static final int NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 15;

    /**
     * Count of entries in the name resolver cache.
     */
    @AeronCounter
    public static final int NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 16;

    /**
     * Count of number of active receivers for flow control strategy.
     */
    @AeronCounter(expectedCName = "FC_NUM_RECEIVERS")
    public static final int FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID = 17;

    /**
     * Count of number of destinations for multi-destination cast channels.
     */
    @AeronCounter(expectedCName = "CHANNEL_NUM_DESTINATIONS")
    public static final int MDC_DESTINATIONS_COUNTER_TYPE_ID = 18;

    /**
     * The number of NAK messages received by the Sender.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_NAKS_RECEIVED_TYPE_ID = 19;

    /**
     * The number of NAK messages sent by the Receiver.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVER_NAKS_SENT_TYPE_ID = 20;

    // EF_VI counters
    /**
     * EF_VI_PORT_INFO_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_PORT_INFO_TYPE_ID = 50;

    /**
     * EF_VI_TRANSPORT_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TRANSPORT_TYPE_ID = 51;

    /**
     * EF_VI_TX_NOBUFS_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TX_NOBUFS_TYPE_ID = 52;

    /**
     * EF_VI_TX_EAGAIN_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TX_EAGAIN_TYPE_ID = 53;

    /**
     * EF_VI_TX_ERROR_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TX_ERROR_TYPE_ID = 54;

    /**
     * EF_VI_RX_DISCARD_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_RX_DISCARD_TYPE_ID = 55;

    /**
     * EF_VI_RX_INVALID_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_RX_INVALID_TYPE_ID = 56;

    /**
     * EF_VI_RX_PKTS_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_RX_PKTS_TYPE_ID = 57;

    /**
     * EF_VI_RX_BYTES_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_RX_BYTES_TYPE_ID = 58;

    /**
     * EF_VI_TX_PKTS_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TX_PKTS_TYPE_ID = 59;

    /**
     * EF_VI_TX_BYTES_TYPE_ID.
     */
    @AeronCounter
    public static final int EF_VI_TX_BYTES_TYPE_ID = 60;

    // VMA counters
    /**
     * VMA_TRANSPORTS_TYPE_ID.
     */
    @AeronCounter
    public static final int VMA_TRANSPORTS_TYPE_ID = 61;

    /**
     * VMA_RX_ZERO_COPY_BYTES_TYPE_ID.
     */
    @AeronCounter
    public static final int VMA_RX_ZERO_COPY_BYTES_TYPE_ID = 62;

    /**
     * VMA_RX_DATA_COPY_BYTES_TYPE_ID.
     */
    @AeronCounter
    public static final int VMA_RX_DATA_COPY_BYTES_TYPE_ID = 63;

    // ATS counters
    /**
     * ATS_TRANSPORTS_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_TRANSPORTS_TYPE_ID = 65;

    /**
     * ATS_DISCARDS_NON_ATS_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_DISCARDS_NON_ATS_TYPE_ID = 66;

    /**
     * ATS_BYTES_ENCRYPTED_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_BYTES_ENCRYPTED_TYPE_ID = 67;

    /**
     * ATS_BYTES_DECRYPTED_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_BYTES_DECRYPTED_TYPE_ID = 68;

    /**
     * ATS_AEAD_ERRORS_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_AEAD_ERRORS_TYPE_ID = 69;

    /**
     * ATS_RSA_KEY_UNKNOWN_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_RSA_KEY_UNKNOWN_TYPE_ID = 70;

    /**
     * ATS_EC_KEY_SIG_ERRORS_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_EC_KEY_SIG_ERRORS_TYPE_ID = 71;

    /**
     * ATS_UNICAST_RE_KEYINGS_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_UNICAST_RE_KEYINGS_TYPE_ID = 72;

    /**
     * ATS_UNICAST_RE_KEYING_RSA_KEY_MISMATCH_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_UNICAST_RE_KEYING_RSA_KEY_MISMATCH_TYPE_ID = 73;

    /**
     * ATS_DROPPED_SM_TYPE_ID.
     */
    @AeronCounter
    public static final int ATS_DROPPED_SM_TYPE_ID = 74;

    // DPDK counters
    /**
     * DPDK_PORT_INFO_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_PORT_INFO_TYPE_ID = 75;

    /**
     * DPDK_TRANSPORT_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_TRANSPORT_TYPE_ID = 76;

    /**
     * DPDK_NOBUFS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_NOBUFS_TYPE_ID = 77;

    /**
     * DPDK_TX_EAGAIN_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_TX_EAGAIN_TYPE_ID = 78;

    /**
     * DPDK_ERROR_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_ERROR_TYPE_ID = 79;

    /**
     * DPDK_PKTS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_PKTS_TYPE_ID = 82;

    /**
     * DPDK_BYTES_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_BYTES_TYPE_ID = 83;

    /**
     * DPDK_MISSED_PACKETS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_MISSED_PACKETS_TYPE_ID = 84;

    /**
     * DPDK_ARP_MISS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_ARP_MISS_TYPE_ID = 85;

    /**
     * DPDK_RX_SENDER_DISCARD_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_RX_SENDER_DISCARD_TYPE_ID = 86;

    /**
     * DPDK_POLLER_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_POLLER_TYPE_ID = 87;

    /**
     * DPDK_QUEUE_DROP_COUNT_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_QUEUE_DROP_COUNT_TYPE_ID = 88;

    /**
     * DPDK_CHECKSUM_FAILURE_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_CHECKSUM_FAILURE_TYPE_ID = 89;

    /**
     * DPDK_FRAGMENTED_PACKETS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_FRAGMENTED_PACKETS_TYPE_ID = 90;

    /**
     * DPDK_MEMPOOL_AVAILABLE_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_MEMPOOL_AVAILABLE_TYPE_ID = 91;

    /**
     * DPDK_EXTENDED_STATS_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_EXTENDED_STATS_TYPE_ID = 92;

    /**
     * DPDK_RX_UNSUPPORTED_ETHERNET_TYPE_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_RX_UNSUPPORTED_ETHERNET_TYPE_TYPE_ID = 93;

    /**
     * DPDK_RX_UNSUPPORTED_PROTOCOL_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_RX_UNSUPPORTED_PROTOCOL_TYPE_ID = 94;

    /**
     * DPDK_RX_RECEIVER_DISCARD_TYPE_ID.
     */
    @AeronCounter
    public static final int DPDK_RX_RECEIVER_DISCARD_TYPE_ID = 95;

    // Archive counters
    /**
     * The position a recording has reached when being archived.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDING_POSITION_TYPE_ID = 100;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    @AeronCounter
    public static final int ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of concurrent control sessions.
     */
    @AeronCounter
    public static final int ARCHIVE_CONTROL_SESSIONS_TYPE_ID = 102;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of an archive agent.
     */
    @AeronCounter
    public static final int ARCHIVE_MAX_CYCLE_TIME_TYPE_ID = 103;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * an archive agent.
     */
    @AeronCounter
    public static final int ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 104;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took recorder to write a block of
     * data to the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID = 105;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes written by the recorder
     * to the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID = 106;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the recorder spent writing data to
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID = 107;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took replayer to read a block from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID = 108;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes read by the replayer from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID = 109;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the replayer spent reading data from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID = 110;

    /**
     * The type id of the {@link Counter} used for tracking the count of active recording sessions.
     */
    @AeronCounter(existsInC = false)
    public static final int ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID = 111;

    /**
     * The type id of the {@link Counter} used for tracking the count of active replay sessions.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID = 112;

    /**
     * The type id of the {@link Counter} used for tracking Archive clients.
     *
     * @since 1.49.0.
     */
    @AeronCounter
    public static final int ARCHIVE_CONTROL_SESSION_TYPE_ID = 113;

    // Cluster counters

    /**
     * Counter type id for the consensus module state.
     */
    @AeronCounter
    public static final int CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;

    /**
     * Counter type id for the cluster node role.
     */
    @AeronCounter
    public static final int CLUSTER_NODE_ROLE_TYPE_ID = 201;

    /**
     * Counter type id for the control toggle.
     */
    @AeronCounter
    public static final int CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;

    /**
     * Counter type id of the commit position.
     */
    @AeronCounter
    public static final int CLUSTER_COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Counter representing the Recovery State for the cluster.
     */
    @AeronCounter
    public static final int CLUSTER_RECOVERY_STATE_TYPE_ID = 204;

    /**
     * Counter type id for count of snapshots taken.
     */
    @AeronCounter
    public static final int CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;

    /**
     * Type id for election state counter.
     */
    @AeronCounter
    public static final int CLUSTER_ELECTION_STATE_TYPE_ID = 207;

    /**
     * The type id of the {@link Counter} used for the backup state.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_STATE_TYPE_ID = 208;

    /**
     * The type id of the {@link Counter} used for the live log position counter.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;

    /**
     * The type id of the {@link Counter} used for the next query deadline counter.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;

    /**
     * Counter type id for the consensus module error count.
     */
    @AeronCounter
    public static final int CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

    /**
     * Counter type id for the number of cluster clients which have been timed out.
     */
    @AeronCounter
    public static final int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

    /**
     * Counter type id for the number of invalid requests which the cluster has received.
     */
    @AeronCounter
    public static final int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

    /**
     * Counter type id for the clustered service error count.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the consensus module.
     */
    @AeronCounter
    public static final int CLUSTER_MAX_CYCLE_TIME_TYPE_ID = 216;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the consensus module.
     */
    @AeronCounter
    public static final int CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 217;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the service container.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID = 218;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the service container.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 219;

    /**
     * The type id of the {@link Counter} used for the cluster standby state.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_STATE_TYPE_ID = 220;

    /**
     * Counter type id for the clustered service error count.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID = 221;

    /**
     * Counter type for responses to heartbeat request from the cluster.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID = 222;

    /**
     * Standby control toggle type id.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID = 223;

    /**
     * The type if of the {@link Counter} used for transition module state.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_STATE")
    public static final int TRANSITION_MODULE_STATE_TYPE_ID = 224;

//    public static final int TRANSITION_MODULE_CONTROL_TOGGLE_TYPE_ID = 225;

    /**
     * Counter type id for the transition module error count.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_ERROR_COUNT")
    public static final int TRANSITION_MODULE_ERROR_COUNT_TYPE_ID = 226;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the cluster standby.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID = 227;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the cluster standby.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 228;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the transition module.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_MAX_CYCLE_TIME")
    public static final int TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID = 229;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the transition module.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED")
    public static final int TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 230;

    /**
     * The type id of the {@link Counter} to make visible the memberId that the cluster standby is currently using to
     * as a source for the cluster log.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID = 231;

    /**
     * Counter type for count of standby snapshots received.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID = 232;

    /**
     * The type of the {@link Counter} used for handling node specific operations.
     */
    @AeronCounter(existsInC = false)
    public static final int NODE_CONTROL_TOGGLE_TYPE_ID = 233;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum total snapshot duration.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID = 234;

    /**
     * The type id of the {@link Counter} used for keeping track of the count total snapshot duration
     * has exceeded the threshold.
     */
    @AeronCounter
    public static final int CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 235;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum snapshot duration
     * for a given clustered service.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID = 236;

    /**
     * The type id of the {@link Counter} used for keeping track of the count snapshot duration
     * has exceeded the threshold for a given clustered service.
     */
    @AeronCounter
    public static final int CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 237;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of elections that have occurred.
     */
    @AeronCounter
    public static final int CLUSTER_ELECTION_COUNT_TYPE_ID = 238;

    /**
     * The type id of the {@link Counter} used for keeping track of the Cluster leadership term id.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID = 239;

    /**
     * The type id of the {@link Counter} used for tracking the number of snapshots downloaded.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID = 240;

    /**
     * The type id of the {@link Counter} used for tracking Cluster clients.
     *
     * @since 1.49.0.
     */
    @AeronCounter
    public static final int CLUSTER_SESSION_TYPE_ID = 241;

    /**
     * SELECTOR_CLIENTS_COUNTER_TYPE_ID.
     */
    @AeronCounter
    public static final int SELECTOR_CLIENTS_COUNTER_TYPE_ID = 300;

    /**
     * SELECTOR_SUBSCRIPTIONS_COUNTER_TYPE_ID.
     */
    @AeronCounter
    public static final int SELECTOR_SUBSCRIPTIONS_COUNTER_TYPE_ID = 301;

    /**
     * SELECTOR_MAX_CYCLE_TIME_TYPE_ID.
     */
    @AeronCounter
    public static final int SELECTOR_MAX_CYCLE_TIME_TYPE_ID = 302;

    /**
     * SELECTOR_MAX_CYCLE_TIME_TYPE_ID.
     */
    @AeronCounter
    public static final int SELECTOR_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 303;

    // ===================
    // Sequencer Counters.
    // ===================

    /**
     * Counter id for Sequencer Index.
     */
    @AeronCounter
    public static final int SEQUENCER_INDEX_COUNTER_TYPE_ID = 500;

    /**
     * Counter id for application group last message.
     */
    @AeronCounter
    public static final int SEQUENCER_GROUP_HWM_COUNTER_TYPE_ID = 501;

    /**
     * Counter id for session last message.
     */
    @AeronCounter
    public static final int SEQUENCER_SESSION_GREATEST_MESSAGE_ID_COUNTER_TYPE_ID = 502;

    /**
     * Counter id for session messages.
     */
    @AeronCounter
    public static final int SEQUENCER_SESSION_MESSAGES_COUNTER_TYPE_ID = 503;

    /**
     * Counter id for session last message timestamp.
     */
    @AeronCounter
    public static final int SEQUENCER_SESSION_GREATEST_MESSAGE_TIMESTAMP_COUNTER_TYPE_ID = 504;

    /**
     * Counter id for the next snapshot id.
     */
    @AeronCounter
    public static final int SEQUENCER_CLIENT_SNAPSHOT_ID_COUNTER_TYPE_ID = 505;

    /**
     * Counter id for sequence index.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_SEQUENCE_INDEX_COUNTER_TYPE_ID = 507;

    /**
     * Application state counter type id.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_STATE_COUNTER_TYPE_ID = 508;

    /**
     * Counter id for error count.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_ERROR_COUNT_TYPE_ID = 509;

    /**
     * Counter id for max service time.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_MAX_SERVICE_TIME_TYPE_ID = 510;

    /**
     * Counter id for the number of times the service time threshold was exceeded.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_SERVICE_TIME_THRESHOLD_EXCEEDED_COUNT_TYPE_ID = 511;

    /**
     * Counter id for the total service time during the last interval.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_INTERVAL_SERVICE_TIME_TYPE_ID = 512;

    /**
     * Counter id for the maximum individual service time during the last interval.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_INTERVAL_MAX_SERVICE_TIME_TYPE_ID = 513;

    /**
     * Counter id for the total number of invocations during the last interval.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_INTERVAL_TOTAL_INVOCATIONS_TYPE_ID = 514;

    /**
     * Counter id for the load time, in milliseconds, of a snapshot.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_SNAPSHOT_LOAD_TIME_TYPE_ID = 515;

    /**
     * Counter id for the store time, in milliseconds, of a snapshot.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_SNAPSHOT_STORE_TIME_TYPE_ID = 516;

    /**
     * Counter id for the number of 'take snapshot' failures.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_TAKE_SNAPSHOT_FAILURES_TYPE_ID = 517;

    /**
     * Counter id for the number of 'take snapshot' instances.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_TAKE_SNAPSHOT_COUNT_TYPE_ID = 518;

    /**
     * Counter id for the application service's session with the sequencer.
     */
    @AeronCounter
    public static final int SEQUENCER_APPLICATION_SESSION_ID_TYPE_ID = 519;

    /**
     * Counter id for the replay index's minimum sequence index.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_MIN_SEQUENCE_INDEX_COUNTER_TYPE_ID = 520;

    /**
     * Counter id for the replay index's minimum log position.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_MIN_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 521;

    /**
     * Counter id for the replay index's maximum sequence index.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_MAX_SEQUENCE_INDEX_COUNTER_TYPE_ID = 522;

    /**
     * Counter id for the replay index's maximum log position.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_MAX_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 523;

    /**
     * Counter id for the replay index's initial sequence index.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_INITIAL_SEQUENCE_INDEX_COUNTER_TYPE_ID = 524;

    /**
     * Counter id for the replay index's initial log position.
     */
    @AeronCounter
    public static final int SEQUENCER_REPLAY_INDEX_INITIAL_SEQUENCE_LOG_POSITION_COUNTER_TYPE_ID = 525;

    private AeronCounters()
    {
    }

    /**
     * Checks that the counter specified by {@code counterId} has the counterTypeId that matches the specified value.
     * If not it will throw a {@link io.aeron.exceptions.ConfigurationException}.
     *
     * @param countersReader        to look up the counter type id.
     * @param counterId             counter to reference.
     * @param expectedCounterTypeId the expected type id for the counter.
     * @throws io.aeron.exceptions.ConfigurationException if the type id does not match.
     * @throws IllegalArgumentException                   if the counterId is not valid.
     */
    public static void validateCounterTypeId(
        final CountersReader countersReader,
        final int counterId,
        final int expectedCounterTypeId)
    {
        final int counterTypeId = countersReader.getCounterTypeId(counterId);
        if (expectedCounterTypeId != counterTypeId)
        {
            throw new ConfigurationException(
                "The type for counterId=" + counterId +
                    ", typeId=" + counterTypeId +
                    " does not match the expected=" + expectedCounterTypeId);
        }
    }

    /**
     * Convenience overload for {@link AeronCounters#validateCounterTypeId(CountersReader, int, int)}.
     *
     * @param aeron                 to resolve a counters' reader.
     * @param counter               to be checked for the appropriate counterTypeId.
     * @param expectedCounterTypeId the expected type id for the counter.
     * @throws io.aeron.exceptions.ConfigurationException if the type id does not match.
     * @throws IllegalArgumentException                   if the counterId is not valid.
     * @see AeronCounters#validateCounterTypeId(CountersReader, int, int)
     */
    public static void validateCounterTypeId(
        final Aeron aeron,
        final Counter counter,
        final int expectedCounterTypeId)
    {
        validateCounterTypeId(aeron.countersReader(), counter.id(), expectedCounterTypeId);
    }

    /**
     * Append version information at the end of the counter's label.
     *
     * @param tempBuffer     to append label to.
     * @param offset         at which current label data ends.
     * @param fullVersion    of the component.
     * @param commitHashCode Git commit SHA.
     * @return length of the suffix appended.
     */
    public static int appendVersionInfo(
        final MutableDirectBuffer tempBuffer, final int offset, final String fullVersion, final String commitHashCode)
    {
        int length = tempBuffer.putStringWithoutLengthAscii(offset, " ");
        length += tempBuffer.putStringWithoutLengthAscii(
            offset + length, formatVersionInfo(fullVersion, commitHashCode));
        return length;
    }

    /**
     * Append specified {@code value} at the end of the counter's label as ASCII encoded value up to the
     * {@link CountersReader#MAX_LABEL_LENGTH}.
     *
     * @param metaDataBuffer containing the counter metadata.
     * @param counterId      to append version info to.
     * @param value          to be appended to the label.
     * @return number of bytes that got appended.
     * @throws IllegalArgumentException if {@code counterId} is invalid or points to non-allocated counter.
     */
    public static int appendToLabel(
        final AtomicBuffer metaDataBuffer, final int counterId, final String value)
    {
        Objects.requireNonNull(metaDataBuffer);
        validateCounterId(metaDataBuffer, counterId);

        final int counterMetaDataOffset = metaDataOffset(counterId);
        final int state = metaDataBuffer.getIntVolatile(counterMetaDataOffset);
        if (RECORD_ALLOCATED != state)
        {
            throw new IllegalArgumentException("counter id " + counterId + " is not allocated, state: " + state);
        }

        final int existingLabelLength = metaDataBuffer.getInt(counterMetaDataOffset + LABEL_OFFSET);
        final int remainingLabelLength = MAX_LABEL_LENGTH - existingLabelLength;

        final int writtenLength = metaDataBuffer.putStringWithoutLengthAscii(
            counterMetaDataOffset + LABEL_OFFSET + SIZE_OF_INT + existingLabelLength,
            value,
            0,
            remainingLabelLength);
        if (writtenLength > 0)
        {
            metaDataBuffer.putIntRelease(
                counterMetaDataOffset + LABEL_OFFSET, existingLabelLength + writtenLength);
        }

        return writtenLength;
    }

    /**
     * Format version information for display purposes.
     *
     * @param fullVersion of the component.
     * @param commitHash  Git commit SHA.
     * @return formatted String.
     */
    public static String formatVersionInfo(final String fullVersion, final String commitHash)
    {
        return "version=" + fullVersion + " commit=" + commitHash;
    }

    /**
     * Set a reference id for a given counter id.
     *
     * @param metaDataBuffer containing the counter metadata.
     * @param valuesBuffer   containing the counter values.
     * @param counterId      to be set.
     * @param referenceId    to set for the counter.
     * @see CountersReader#getCounterReferenceId(int)
     * @since 1.49.0
     */
    public static void setReferenceId(
        final AtomicBuffer metaDataBuffer, final AtomicBuffer valuesBuffer, final int counterId, final long referenceId)
    {
        Objects.requireNonNull(metaDataBuffer);
        Objects.requireNonNull(valuesBuffer);
        validateCounterId(metaDataBuffer, counterId);

        valuesBuffer.putLongRelease(counterOffset(counterId) + REFERENCE_ID_OFFSET, referenceId);
    }

    private static void validateCounterId(final AtomicBuffer metaDataBuffer, final int counterId)
    {
        if (counterId < 0)
        {
            throw new IllegalArgumentException("counter id " + counterId + " is negative");
        }

        final int maxCounterId = (metaDataBuffer.capacity() / METADATA_LENGTH) - 1;
        if (counterId > maxCounterId)
        {
            throw new IllegalArgumentException(
                "counter id " + counterId + " out of range: 0 - maxCounterId=" + maxCounterId);
        }
    }
}
