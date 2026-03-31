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
package io.aeron.driver.status;

import io.aeron.AeronCounters;
import io.aeron.driver.MediaDriverVersion;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_AERON_VERSION;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_BYTES_CURRENTLY_MAPPED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_BYTES_RECEIVED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_BYTES_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CLIENT_TIMEOUTS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONDUCTOR_PROXY_FAILS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONTROLLABLE_IDLE_STRATEGY;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_ERRORS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_ERROR_FRAMES_RECEIVED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_ERROR_FRAMES_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_FREE_FAILS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_HEARTBEATS_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_IMAGES_REJECTED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_INVALID_PACKETS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_LOSS_GAP_FILLS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_NAME_RESOLVER_MAX_TIME;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_POSSIBLE_TTL_ASYMMETRY;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RESOLUTION_CHANGES;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RETRANSMITS_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_RETRANSMIT_OVERFLOW;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_SENDER_FLOW_CONTROL_LIMITS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_SHORT_SENDS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_STATUS_MESSAGES_REJECTED;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS;
import static io.aeron.AeronCounters.SYSTEM_COUNTER_ID_UNBLOCKED_PUBLICATIONS;

/**
 * System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
 */
public enum SystemCounterDescriptor
{
    /**
     * Running total of bytes sent for data over UDP, excluding IP headers.
     */
    BYTES_SENT(SYSTEM_COUNTER_ID_BYTES_SENT, "Bytes sent"),

    /**
     * Running total of bytes received for data over UDP, excluding IP headers.
     */
    BYTES_RECEIVED(SYSTEM_COUNTER_ID_BYTES_RECEIVED, "Bytes received"),

    /**
     * Failed offers to the receiver proxy suggesting back-pressure.
     */
    RECEIVER_PROXY_FAILS(SYSTEM_COUNTER_ID_RECEIVER_PROXY_FAILS, "Failed offers to ReceiverProxy"),

    /**
     * Failed offers to the sender proxy suggesting back-pressure.
     */
    SENDER_PROXY_FAILS(SYSTEM_COUNTER_ID_SENDER_PROXY_FAILS, "Failed offers to SenderProxy"),

    /**
     * Failed offers to the driver conductor proxy suggesting back-pressure.
     */
    CONDUCTOR_PROXY_FAILS(SYSTEM_COUNTER_ID_CONDUCTOR_PROXY_FAILS, "Failed offers to DriverConductorProxy"),

    /**
     * Count of NAKs sent back to senders requesting re-transmits.
     */
    NAK_MESSAGES_SENT(SYSTEM_COUNTER_ID_NAK_MESSAGES_SENT, "NAKs sent"),

    /**
     * Count of NAKs received from receivers requesting re-transmits.
     */
    NAK_MESSAGES_RECEIVED(SYSTEM_COUNTER_ID_NAK_MESSAGES_RECEIVED, "NAKs received"),

    /**
     * Count of status messages sent back to senders for flow control.
     */
    STATUS_MESSAGES_SENT(SYSTEM_COUNTER_ID_STATUS_MESSAGES_SENT, "Status Messages sent"),

    /**
     * Count of status messages received from receivers for flow control.
     */
    STATUS_MESSAGES_RECEIVED(SYSTEM_COUNTER_ID_STATUS_MESSAGES_RECEIVED, "Status Messages received"),

    /**
     * Count of heartbeat data frames sent to indicate liveness in the absence of data to send.
     */
    HEARTBEATS_SENT(SYSTEM_COUNTER_ID_HEARTBEATS_SENT, "Heartbeats sent"),

    /**
     * Count of heartbeat data frames received to indicate liveness in the absence of data to send.
     */
    HEARTBEATS_RECEIVED(SYSTEM_COUNTER_ID_HEARTBEATS_RECEIVED, "Heartbeats received"),

    /**
     * Count of data packets re-transmitted as a result of NAKs.
     */
    RETRANSMITS_SENT(SYSTEM_COUNTER_ID_RETRANSMITS_SENT, "Retransmits sent"),

    /**
     * Count of packets received which under-run the current flow control window for images.
     */
    FLOW_CONTROL_UNDER_RUNS(SYSTEM_COUNTER_ID_FLOW_CONTROL_UNDER_RUNS, "Flow control under runs"),

    /**
     * Count of packets received which over-run the current flow control window for images.
     */
    FLOW_CONTROL_OVER_RUNS(SYSTEM_COUNTER_ID_FLOW_CONTROL_OVER_RUNS, "Flow control over runs"),

    /**
     * Count of invalid packets received.
     */
    INVALID_PACKETS(SYSTEM_COUNTER_ID_INVALID_PACKETS, "Invalid packets"),

    /**
     * Count of errors observed by the driver and an indication to read the distinct error log.
     */
    ERRORS(SYSTEM_COUNTER_ID_ERRORS,
        "Errors: " + AeronCounters.formatVersionInfo(MediaDriverVersion.VERSION, MediaDriverVersion.GIT_SHA)),

    /**
     * Count of socket send operation which resulted in less than the packet length being sent.
     */
    SHORT_SENDS(SYSTEM_COUNTER_ID_SHORT_SENDS, "Short sends"),

    /**
     * Count of attempts to free log buffers no longer required by the driver which as still held by clients.
     */
    FREE_FAILS(SYSTEM_COUNTER_ID_FREE_FAILS, "Failed attempts to free log buffers"),

    /**
     * Count of the times a sender has entered the state of being back-pressured when it could have sent faster.
     */
    SENDER_FLOW_CONTROL_LIMITS(SYSTEM_COUNTER_ID_SENDER_FLOW_CONTROL_LIMITS,
        "Sender flow control limits, i.e. back-pressure events"),

    /**
     * Count of the times a publication has been unblocked after a client failed to complete an offer within a timeout.
     */
    UNBLOCKED_PUBLICATIONS(SYSTEM_COUNTER_ID_UNBLOCKED_PUBLICATIONS, "Unblocked Publications"),

    /**
     * Count of the times a command has been unblocked after a client failed to complete an offer within a timeout.
     */
    UNBLOCKED_COMMANDS(SYSTEM_COUNTER_ID_UNBLOCKED_COMMANDS, "Unblocked Control Commands"),

    /**
     * Count of the times the channel endpoint detected a possible TTL asymmetry between its config and new connection.
     */
    POSSIBLE_TTL_ASYMMETRY(SYSTEM_COUNTER_ID_POSSIBLE_TTL_ASYMMETRY, "Possible TTL Asymmetry"),

    /**
     * Current status of the {@link org.agrona.concurrent.ControllableIdleStrategy} if configured.
     */
    CONTROLLABLE_IDLE_STRATEGY(SYSTEM_COUNTER_ID_CONTROLLABLE_IDLE_STRATEGY, "ControllableIdleStrategy status"),

    /**
     * Count of the times a loss gap has been filled when NAKs have been disabled.
     */
    LOSS_GAP_FILLS(SYSTEM_COUNTER_ID_LOSS_GAP_FILLS, "Loss gap fills"),

    /**
     * Count of the Aeron clients that have timed out without a graceful close.
     */
    CLIENT_TIMEOUTS(SYSTEM_COUNTER_ID_CLIENT_TIMEOUTS, "Client liveness timeouts"),

    /**
     * Count of the times a connection endpoint has been re-resolved resulting in a change.
     */
    RESOLUTION_CHANGES(SYSTEM_COUNTER_ID_RESOLUTION_CHANGES, "Resolution changes"),

    /**
     * The maximum time spent by the conductor between work cycles.
     */
    CONDUCTOR_MAX_CYCLE_TIME(SYSTEM_COUNTER_ID_CONDUCTOR_MAX_CYCLE_TIME,
        "Conductor max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
     */
    CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED(SYSTEM_COUNTER_ID_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED,
        "Conductor work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the sender between work cycles.
     */
    SENDER_MAX_CYCLE_TIME(SYSTEM_COUNTER_ID_SENDER_MAX_CYCLE_TIME,
        "Sender max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
     */
    SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED(SYSTEM_COUNTER_ID_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED,
        "Sender work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the receiver between work cycles.
     */
    RECEIVER_MAX_CYCLE_TIME(SYSTEM_COUNTER_ID_RECEIVER_MAX_CYCLE_TIME, "Receiver max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
     */
    RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED(SYSTEM_COUNTER_ID_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED,
        "Receiver work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the NameResolver in one of its operations.
     *
     * @since 1.41.0
     */
    NAME_RESOLVER_MAX_TIME(SYSTEM_COUNTER_ID_NAME_RESOLVER_MAX_TIME, "NameResolver max time in ns"),

    /**
     * Count of the number of times the time threshold has been exceeded by the NameResolver.
     *
     * @since 1.41.0
     */
    NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED(SYSTEM_COUNTER_ID_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED,
        "NameResolver exceeded threshold count"),

    /**
     * The version of the media driver.
     *
     * @since 1.43.0
     */
    AERON_VERSION(SYSTEM_COUNTER_ID_AERON_VERSION, "Aeron software: " +
        AeronCounters.formatVersionInfo(MediaDriverVersion.VERSION, MediaDriverVersion.GIT_SHA)),

    /**
     * The total number of bytes currently mapped in log buffers, CnC file, and loss report.
     *
     * @since 1.44.0
     */
    BYTES_CURRENTLY_MAPPED(SYSTEM_COUNTER_ID_BYTES_CURRENTLY_MAPPED, "Bytes currently mapped"),

    /**
     * A minimum bound on the number of bytes re-transmitted as a result of NAKs.
     * <p>
     * MDC retransmits are only counted once; therefore, this is a minimum bound rather than the actual number
     * of retransmitted bytes. We may change this in the future.
     * <p>
     * Note that retransmitted bytes are not included in the {@link SystemCounterDescriptor#BYTES_SENT}
     * counter value. We may change this in the future.
     *
     * @since 1.45.0
     */
    RETRANSMITTED_BYTES(SYSTEM_COUNTER_ID_RETRANSMITTED_BYTES, "Retransmitted bytes"),

    /**
     * A count of the number of times that the retransmit pool has been overflowed.
     *
     * @since 1.45.0
     */
    RETRANSMIT_OVERFLOW(SYSTEM_COUNTER_ID_RETRANSMIT_OVERFLOW, "Retransmit Pool Overflow count"),

    /**
     * A count of the number of error frames received by this driver.
     *
     * @since 1.47.0
     */
    ERROR_FRAMES_RECEIVED(SYSTEM_COUNTER_ID_ERROR_FRAMES_RECEIVED, "Error Frames received"),

    /**
     * A count of the number of error frames sent by this driver.
     *
     * @since 1.47.0
     */
    ERROR_FRAMES_SENT(SYSTEM_COUNTER_ID_ERROR_FRAMES_SENT, "Error Frames sent"),

    /**
     * A count of the number of publications that have been revoked.
     *
     * @since 1.48.0
     */
    PUBLICATIONS_REVOKED(SYSTEM_COUNTER_ID_PUBLICATIONS_REVOKED, "Publications Revoked"),

    /**
     * A count of the number of publication images that have been revoked.
     *
     * @since 1.48.0
     */
    PUBLICATION_IMAGES_REVOKED(SYSTEM_COUNTER_ID_PUBLICATION_IMAGES_REVOKED, "Publication Images Revoked"),

    /**
     * A count of the number of images that have been rejected.
     *
     * @since 1.48.0
     */
    IMAGES_REJECTED(SYSTEM_COUNTER_ID_IMAGES_REJECTED, "Images rejected"),

    /**
     * The semantic version of the control protocol between clients and media driver.
     *
     * @since 1.49.0
     */
    CONTROL_PROTOCOL_VERSION(SYSTEM_COUNTER_ID_CONTROL_PROTOCOL_VERSION, "Control protocol version"),

    /**
     * Count of status messages received from receivers for flow control.
     *
     * @since 1.51.0
     */
    STATUS_MESSAGES_REJECTED(SYSTEM_COUNTER_ID_STATUS_MESSAGES_REJECTED, "Status Messages rejected");

    /**
     * All system counters have the same type id, i.e. system counters are the same type. Other types can exist.
     */
    public static final int SYSTEM_COUNTER_TYPE_ID = AeronCounters.DRIVER_SYSTEM_COUNTER_TYPE_ID;

    private static final Int2ObjectHashMap<SystemCounterDescriptor> DESCRIPTOR_BY_ID_MAP = new Int2ObjectHashMap<>();

    static
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            final SystemCounterDescriptor other = DESCRIPTOR_BY_ID_MAP.put(descriptor.id, descriptor);
            if (null != other)
            {
                throw new IllegalStateException(descriptor + " uses the same id as " + other);
            }
        }
    }

    /**
     * Get the {@link SystemCounterDescriptor} for a given id.
     *
     * @param id for the descriptor.
     * @return the descriptor if found otherwise null.
     */
    public static SystemCounterDescriptor get(final int id)
    {
        return DESCRIPTOR_BY_ID_MAP.get(id);
    }

    private final int id;
    private final String label;

    SystemCounterDescriptor(final int id, final String label)
    {
        this.id = id;
        this.label = label;
    }

    /**
     * The unique identity for the system counter.
     *
     * @return the unique identity for the system counter.
     */
    public int id()
    {
        return id;
    }

    /**
     * The human-readable label to identify a system counter.
     *
     * @return the human-readable label to identify a system counter.
     */
    public String label()
    {
        return label;
    }

    /**
     * Create a new counter for the enumerated descriptor.
     *
     * @param countersManager for managing the underlying storage.
     * @return a new counter for the enumerated descriptor.
     */
    public AtomicCounter newCounter(final CountersManager countersManager)
    {
        final AtomicCounter counter =
            countersManager.newCounter(label, SYSTEM_COUNTER_TYPE_ID, (buffer) -> buffer.putInt(0, id));
        countersManager.setCounterRegistrationId(counter.id(), id);
        countersManager.setCounterOwnerId(counter.id(), NULL_VALUE);
        return counter;
    }
}
