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

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.driver.MediaDriverVersion;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.*;

/**
 * System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
 */
public enum SystemCounterDescriptor
{
    /**
     * Running total of bytes sent for data over UDP, excluding IP headers.
     */
    BYTES_SENT(0, "Bytes sent"),

    /**
     * Running total of bytes received for data over UDP, excluding IP headers.
     */
    BYTES_RECEIVED(1, "Bytes received"),

    /**
     * Failed offers to the receiver proxy suggesting back-pressure.
     */
    RECEIVER_PROXY_FAILS(2, "Failed offers to ReceiverProxy"),

    /**
     * Failed offers to the sender proxy suggesting back-pressure.
     */
    SENDER_PROXY_FAILS(3, "Failed offers to SenderProxy"),

    /**
     * Failed offers to the driver conductor proxy suggesting back-pressure.
     */
    CONDUCTOR_PROXY_FAILS(4, "Failed offers to DriverConductorProxy"),

    /**
     * Count of NAKs sent back to senders requesting re-transmits.
     */
    NAK_MESSAGES_SENT(5, "NAKs sent"),

    /**
     * Count of NAKs received from receivers requesting re-transmits.
     */
    NAK_MESSAGES_RECEIVED(6, "NAKs received"),

    /**
     * Count of status messages sent back to senders for flow control.
     */
    STATUS_MESSAGES_SENT(7, "Status Messages sent"),

    /**
     * Count of status messages received from receivers for flow control.
     */
    STATUS_MESSAGES_RECEIVED(8, "Status Messages received"),

    /**
     * Count of heartbeat data frames sent to indicate liveness in the absence of data to send.
     */
    HEARTBEATS_SENT(9, "Heartbeats sent"),

    /**
     * Count of heartbeat data frames received to indicate liveness in the absence of data to send.
     */
    HEARTBEATS_RECEIVED(10, "Heartbeats received"),

    /**
     * Count of data packets re-transmitted as a result of NAKs.
     */
    RETRANSMITS_SENT(11, "Retransmits sent"),

    /**
     * Count of packets received which under-run the current flow control window for images.
     */
    FLOW_CONTROL_UNDER_RUNS(12, "Flow control under runs"),

    /**
     * Count of packets received which over-run the current flow control window for images.
     */
    FLOW_CONTROL_OVER_RUNS(13, "Flow control over runs"),

    /**
     * Count of invalid packets received.
     */
    INVALID_PACKETS(14, "Invalid packets"),

    /**
     * Count of errors observed by the driver and an indication to read the distinct error log.
     */
    ERRORS(15, "Errors: " + AeronCounters.formatVersionInfo(MediaDriverVersion.VERSION, MediaDriverVersion.GIT_SHA)),

    /**
     * Count of socket send operation which resulted in less than the packet length being sent.
     */
    SHORT_SENDS(16, "Short sends"),

    /**
     * Count of attempts to free log buffers no longer required by the driver which as still held by clients.
     */
    FREE_FAILS(17, "Failed attempts to free log buffers"),

    /**
     * Count of the times a sender has entered the state of being back-pressured when it could have sent faster.
     */
    SENDER_FLOW_CONTROL_LIMITS(18, "Sender flow control limits, i.e. back-pressure events"),

    /**
     * Count of the times a publication has been unblocked after a client failed to complete an offer within a timeout.
     */
    UNBLOCKED_PUBLICATIONS(19, "Unblocked Publications"),

    /**
     * Count of the times a command has been unblocked after a client failed to complete an offer within a timeout.
     */
    UNBLOCKED_COMMANDS(20, "Unblocked Control Commands"),

    /**
     * Count of the times the channel endpoint detected a possible TTL asymmetry between its config and new connection.
     */
    POSSIBLE_TTL_ASYMMETRY(21, "Possible TTL Asymmetry"),

    /**
     * Current status of the {@link org.agrona.concurrent.ControllableIdleStrategy} if configured.
     */
    CONTROLLABLE_IDLE_STRATEGY(22, "ControllableIdleStrategy status"),

    /**
     * Count of the times a loss gap has been filled when NAKs have been disabled.
     */
    LOSS_GAP_FILLS(23, "Loss gap fills"),

    /**
     * Count of the Aeron clients that have timed out without a graceful close.
     */
    CLIENT_TIMEOUTS(24, "Client liveness timeouts"),

    /**
     * Count of the times a connection endpoint has been re-resolved resulting in a change.
     */
    RESOLUTION_CHANGES(25, "Resolution changes"),

    /**
     * The maximum time spent by the conductor between work cycles.
     */
    CONDUCTOR_MAX_CYCLE_TIME(26, "Conductor max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the conductor in its work cycle.
     */
    CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED(27, "Conductor work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the sender between work cycles.
     */
    SENDER_MAX_CYCLE_TIME(28, "Sender max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the sender in its work cycle.
     */
    SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED(29, "Sender work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the receiver between work cycles.
     */
    RECEIVER_MAX_CYCLE_TIME(30, "Receiver max cycle time doing its work in ns"),

    /**
     * Count of the number of times the cycle time threshold has been exceeded by the receiver in its work cycle.
     */
    RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED(31, "Receiver work cycle exceeded threshold count"),

    /**
     * The maximum time spent by the NameResolver in one of its operations.
     */
    NAME_RESOLVER_MAX_TIME(32, "NameResolver max time in ns"),

    /**
     * Count of the number of times the time threshold has been exceeded by the NameResolver.
     */
    NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED(33, "NameResolver exceeded threshold count"),

    /**
     * The version of the media driver.
     */
    AERON_VERSION(34, "Aeron software: " +
        AeronCounters.formatVersionInfo(MediaDriverVersion.VERSION, MediaDriverVersion.GIT_SHA)),

    /**
     * The total number of bytes currently mapped in log buffers, CnC file, and loss report.
     */
    BYTES_CURRENTLY_MAPPED(35, "Bytes currently mapped"),

    /**
     * A minimum bound on the number of bytes re-transmitted as a result of NAKs.
     * <p>
     * MDC retransmits are only counted once; therefore, this is a minimum bound rather than the actual number
     * of retransmitted bytes. We may change this in the future.
     * <p>
     * Note that retransmitted bytes are not included in the {@link SystemCounterDescriptor#BYTES_SENT}
     * counter value. We may change this in the future.
     */
    RETRANSMITTED_BYTES(36, "Retransmitted bytes"),

    /**
     * A count of the number of times that the retransmit pool has been overflowed.
     */
    RETRANSMIT_OVERFLOW(37, "Retransmit Pool Overflow count"),

    /**
     * A count of the number of error frames received by this driver.
     */
    ERROR_FRAMES_RECEIVED(38, "Error Frames received"),

    /**
     * A count of the number of error frames sent by this driver.
     */
    ERROR_FRAMES_SENT(39, "Error Frames sent"),

    /**
     * A count of the number of publications that have been revoked.
     */
    PUBLICATIONS_REVOKED(40, "Publications Revoked"),

    /**
     * A count of the number of publication images that have been revoked.
     */
    PUBLICATION_IMAGES_REVOKED(41, "Publication Images Revoked"),

    /**
     * A count of the number of images that have been rejected.
     */
    IMAGES_REJECTED(42, "Images rejected");

    /**
     * All system counters have the same type id, i.e. system counters are the same type. Other types can exist.
     */
    public static final int SYSTEM_COUNTER_TYPE_ID = AeronCounters.DRIVER_SYSTEM_COUNTER_TYPE_ID;

    private static final Int2ObjectHashMap<SystemCounterDescriptor> DESCRIPTOR_BY_ID_MAP = new Int2ObjectHashMap<>();

    static
    {
        for (final SystemCounterDescriptor descriptor : SystemCounterDescriptor.values())
        {
            if (null != DESCRIPTOR_BY_ID_MAP.put(descriptor.id, descriptor))
            {
                throw new IllegalStateException("Descriptor id already in use: " + descriptor.id);
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
        countersManager.setCounterOwnerId(counter.id(), Aeron.NULL_VALUE);
        return counter;
    }
}
