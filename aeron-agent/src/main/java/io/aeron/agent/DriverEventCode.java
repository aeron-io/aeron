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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

import java.util.Arrays;

import static io.aeron.agent.DriverEventDissector.*;

/**
 * Events and codecs for encoding/decoding events recorded to the {@link EventConfiguration#EVENT_RING_BUFFER}.
 */
public enum DriverEventCode implements EventCode
{
    /**
     * Incoming frame.
     */
    FRAME_IN(1, DriverEventDissector::dissectFrame),
    /**
     * Outgoing frame.
     */
    FRAME_OUT(2, DriverEventDissector::dissectFrame),
    /**
     * Add publication command.
     */
    CMD_IN_ADD_PUBLICATION(3, DriverEventDissector::dissectCommand),
    /**
     * Remove publication command.
     */
    CMD_IN_REMOVE_PUBLICATION(4, DriverEventDissector::dissectCommand),
    /**
     * Add subscription command.
     */
    CMD_IN_ADD_SUBSCRIPTION(5, DriverEventDissector::dissectCommand),
    /**
     * Remove subscription command.
     */
    CMD_IN_REMOVE_SUBSCRIPTION(6, DriverEventDissector::dissectCommand),
    /**
     * Publication ready response.
     */
    CMD_OUT_PUBLICATION_READY(7, DriverEventDissector::dissectCommand),
    /**
     * On available image response.
     */
    CMD_OUT_AVAILABLE_IMAGE(8, DriverEventDissector::dissectCommand),
    /**
     * Operation success response.
     */
    CMD_OUT_ON_OPERATION_SUCCESS(12, DriverEventDissector::dissectCommand),
    /**
     * Keepalive command.
     */
    CMD_IN_KEEPALIVE_CLIENT(13, DriverEventDissector::dissectCommand),
    /**
     * Cleanup publication event.
     */
    REMOVE_PUBLICATION_CLEANUP(14,
        (code, buffer, offset, builder) -> dissectRemovePublicationCleanup(buffer, offset, builder)),
    /**
     * Cleanup subscription event.
     */
    REMOVE_SUBSCRIPTION_CLEANUP(15,
        (code, buffer, offset, builder) -> dissectRemoveSubscriptionCleanup(buffer, offset, builder)),
    /**
     * Cleanup image event.
     */
    REMOVE_IMAGE_CLEANUP(16,
        (code, buffer, offset, builder) -> dissectRemoveImageCleanup(buffer, offset, builder)),
    /**
     * On unavailable image response.
     */
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17, DriverEventDissector::dissectCommand),
    /**
     * Send channel creation event.
     */
    SEND_CHANNEL_CREATION(23, DriverEventDissector::dissectString),
    /**
     * Receive channel creation event.
     */
    RECEIVE_CHANNEL_CREATION(24, DriverEventDissector::dissectString),
    /**
     * Send channel closed event.
     */
    SEND_CHANNEL_CLOSE(25, DriverEventDissector::dissectString),
    /**
     * Receive channel creation event.
     */
    RECEIVE_CHANNEL_CLOSE(26, DriverEventDissector::dissectString),
    /**
     * Add destination command.
     */
    CMD_IN_ADD_DESTINATION(30, DriverEventDissector::dissectCommand),
    /**
     * Remove destination command.
     */
    CMD_IN_REMOVE_DESTINATION(31, DriverEventDissector::dissectCommand),
    /**
     * Add exclusive publication command.
     */
    CMD_IN_ADD_EXCLUSIVE_PUBLICATION(32, DriverEventDissector::dissectCommand),
    /**
     * Exclusive publication ready.
     */
    CMD_OUT_EXCLUSIVE_PUBLICATION_READY(33, DriverEventDissector::dissectCommand),
    /**
     * Error response.
     */
    CMD_OUT_ERROR(34, DriverEventDissector::dissectCommand),
    /**
     * Add counter command.
     */
    CMD_IN_ADD_COUNTER(35, DriverEventDissector::dissectCommand),
    /**
     * Remove counter command.
     */
    CMD_IN_REMOVE_COUNTER(36, DriverEventDissector::dissectCommand),
    /**
     * Subscription ready.
     */
    CMD_OUT_SUBSCRIPTION_READY(37, DriverEventDissector::dissectCommand),
    /**
     * Counter ready.
     */
    CMD_OUT_COUNTER_READY(38, DriverEventDissector::dissectCommand),
    /**
     * On unavailable counter event.
     */
    CMD_OUT_ON_UNAVAILABLE_COUNTER(39, DriverEventDissector::dissectCommand),
    /**
     * Close client command.
     */
    CMD_IN_CLIENT_CLOSE(40, DriverEventDissector::dissectCommand),
    /**
     * Add receive destination command.
     */
    CMD_IN_ADD_RCV_DESTINATION(41, DriverEventDissector::dissectCommand),
    /**
     * Remove receive destination command.
     */
    CMD_IN_REMOVE_RCV_DESTINATION(42, DriverEventDissector::dissectCommand),
    /**
     * On client timeout.
     */
    CMD_OUT_ON_CLIENT_TIMEOUT(43, DriverEventDissector::dissectCommand),
    /**
     * Terminate driver command.
     */
    CMD_IN_TERMINATE_DRIVER(44, DriverEventDissector::dissectCommand),
    /**
     * Untethered subscription state change.
     */
    UNTETHERED_SUBSCRIPTION_STATE_CHANGE(45,
        (code, buffer, offset, builder) -> dissectUntetheredSubscriptionStateChange(buffer, offset, builder)),
    /**
     * Name resolution neighbor added.
     */
    NAME_RESOLUTION_NEIGHBOR_ADDED(46, DriverEventDissector::dissectAddress),
    /**
     * Name resolution neighbor removed.
     */
    NAME_RESOLUTION_NEIGHBOR_REMOVED(47, DriverEventDissector::dissectAddress),
    /**
     * Flow control receiver added.
     */
    FLOW_CONTROL_RECEIVER_ADDED(48, DriverEventDissector::dissectFlowControlReceiver),
    /**
     * Flow control receiver removed.
     */
    FLOW_CONTROL_RECEIVER_REMOVED(49, DriverEventDissector::dissectFlowControlReceiver),
    /**
     * Name resolution resolve.
     */
    NAME_RESOLUTION_RESOLVE(50,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectResolve(buffer, offset, builder)),
    /**
     * Free text event.
     */
    TEXT_DATA(51, DriverEventDissector::dissectString),
    /**
     * Name resolution lookup.
     */
    NAME_RESOLUTION_LOOKUP(52,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectLookup(buffer, offset, builder)),
    /**
     * Name resolution host name.
     */
    NAME_RESOLUTION_HOST_NAME(53,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectHostName(buffer, offset, builder)),

    /**
     * Nak sent.
     */
    NAK_SENT(54, DriverEventDissector::dissectNak),

    /**
     * Resend data upon Nak.
     */
    RESEND(55,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectResend(buffer, offset, builder)),

    /**
     * Remove destination by id.
     */
    CMD_IN_REMOVE_DESTINATION_BY_ID(56, DriverEventDissector::dissectCommand),

    /**
     * Reject image command received by the driver.
     */
    CMD_IN_REJECT_IMAGE(57, DriverEventDissector::dissectCommand),

    /**
     * Nak received.
     */
    NAK_RECEIVED(58, DriverEventDissector::dissectNak),

    /**
     * Publication revoked.
     */
    PUBLICATION_REVOKE(59,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectPublicationRevoke(buffer, offset, builder)),

    /**
     * Publication Image revoked.
     */
    PUBLICATION_IMAGE_REVOKE(60,
        (code, buffer, offset, builder) -> DriverEventDissector.dissectPublicationImageRevoke(buffer, offset, builder));

    static final int EVENT_CODE_TYPE = EventCodeType.DRIVER.getTypeCode();

    private static final DriverEventCode[] EVENT_CODE_BY_ID;

    private final int id;
    private final DissectFunction<DriverEventCode> dissector;

    static
    {
        final DriverEventCode[] codes = DriverEventCode.values();
        final int maxId = Arrays.stream(codes).mapToInt(DriverEventCode::id).max().orElse(0);
        EVENT_CODE_BY_ID = new DriverEventCode[maxId + 1];

        for (final DriverEventCode code : codes)
        {
            final int id = code.id();
            if (null != EVENT_CODE_BY_ID[id])
            {
                throw new IllegalArgumentException("id already in use: " + id);
            }

            EVENT_CODE_BY_ID[id] = code;
        }
    }

    DriverEventCode(final int id, final DissectFunction<DriverEventCode> dissector)
    {
        this.id = id;
        this.dissector = dissector;
    }

    /**
     * {@inheritDoc}
     */
    public int id()
    {
        return id;
    }

    static DriverEventCode get(final int id)
    {
        if (id < 0 || id >= EVENT_CODE_BY_ID.length)
        {
            throw new IllegalArgumentException("no DriverEventCode for id: " + id);
        }

        final DriverEventCode code = EVENT_CODE_BY_ID[id];

        if (null == code)
        {
            throw new IllegalArgumentException("no DriverEventCode for id: " + id);
        }

        return code;
    }

    static DriverEventCode get(final String name)
    {
        if ("SEND_NAK_MESSAGE".equals(name))
        {
            return NAK_SENT;
        }
        else
        {
            return DriverEventCode.valueOf(name);
        }
    }

    /**
     * Decode an event serialised in a buffer to a provided {@link StringBuilder}.
     *
     * @param buffer  containing the encoded event.
     * @param offset  offset at which the event begins.
     * @param builder to write the decoded event to.
     */
    public void decode(final MutableDirectBuffer buffer, final int offset, final StringBuilder builder)
    {
        dissector.dissect(this, buffer, offset, builder);
    }
}
