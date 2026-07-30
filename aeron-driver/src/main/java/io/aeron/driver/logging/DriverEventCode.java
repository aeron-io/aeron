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
package io.aeron.driver.logging;

import io.aeron.logging.EventCode;
import io.aeron.logging.EventCodeType;
import io.aeron.logging.EventConfiguration;

import java.util.Arrays;

/**
 * Events and codecs for encoding/decoding events recorded to the ring buffer held by {@link EventConfiguration#eventReader}.
 */
public enum DriverEventCode implements EventCode
{
    /**
     * Incoming frame.
     */
    FRAME_IN(1),
    /**
     * Outgoing frame.
     */
    FRAME_OUT(2),
    /**
     * Add publication command.
     */
    CMD_IN_ADD_PUBLICATION(3),
    /**
     * Remove publication command.
     */
    CMD_IN_REMOVE_PUBLICATION(4),
    /**
     * Add subscription command.
     */
    CMD_IN_ADD_SUBSCRIPTION(5),
    /**
     * Remove subscription command.
     */
    CMD_IN_REMOVE_SUBSCRIPTION(6),
    /**
     * Publication ready response.
     */
    CMD_OUT_PUBLICATION_READY(7),
    /**
     * On available image response.
     */
    CMD_OUT_AVAILABLE_IMAGE(8),
    /**
     * Operation success response.
     */
    CMD_OUT_ON_OPERATION_SUCCESS(12),
    /**
     * Keepalive command.
     */
    CMD_IN_KEEPALIVE_CLIENT(13),
    /**
     * Cleanup publication event.
     */
    REMOVE_PUBLICATION_CLEANUP(14
    ),
    /**
     * Cleanup subscription event.
     */
    REMOVE_SUBSCRIPTION_CLEANUP(15
    ),
    /**
     * Cleanup image event.
     */
    REMOVE_IMAGE_CLEANUP(16
    ),
    /**
     * On unavailable image response.
     */
    CMD_OUT_ON_UNAVAILABLE_IMAGE(17),
    /**
     * Send channel creation event.
     */
    SEND_CHANNEL_CREATION(23),
    /**
     * Receive channel creation event.
     */
    RECEIVE_CHANNEL_CREATION(24),
    /**
     * Send channel closed event.
     */
    SEND_CHANNEL_CLOSE(25),
    /**
     * Receive channel creation event.
     */
    RECEIVE_CHANNEL_CLOSE(26),
    /**
     * Add destination command.
     */
    CMD_IN_ADD_DESTINATION(30),
    /**
     * Remove destination command.
     */
    CMD_IN_REMOVE_DESTINATION(31),
    /**
     * Add exclusive publication command.
     */
    CMD_IN_ADD_EXCLUSIVE_PUBLICATION(32),
    /**
     * Exclusive publication ready.
     */
    CMD_OUT_EXCLUSIVE_PUBLICATION_READY(33),
    /**
     * Error response.
     */
    CMD_OUT_ERROR(34),
    /**
     * Add counter command.
     */
    CMD_IN_ADD_COUNTER(35),
    /**
     * Remove counter command.
     */
    CMD_IN_REMOVE_COUNTER(36),
    /**
     * Subscription ready.
     */
    CMD_OUT_SUBSCRIPTION_READY(37),
    /**
     * Counter ready.
     */
    CMD_OUT_COUNTER_READY(38),
    /**
     * On unavailable counter event.
     */
    CMD_OUT_ON_UNAVAILABLE_COUNTER(39),
    /**
     * Close client command.
     */
    CMD_IN_CLIENT_CLOSE(40),
    /**
     * Add receive destination command.
     */
    CMD_IN_ADD_RCV_DESTINATION(41),
    /**
     * Remove receive destination command.
     */
    CMD_IN_REMOVE_RCV_DESTINATION(42),
    /**
     * On client timeout.
     */
    CMD_OUT_ON_CLIENT_TIMEOUT(43),
    /**
     * Terminate driver command.
     */
    CMD_IN_TERMINATE_DRIVER(44),
    /**
     * Untethered subscription state change.
     */
    UNTETHERED_SUBSCRIPTION_STATE_CHANGE(45
    ),
    /**
     * Name resolution neighbor added.
     */
    NAME_RESOLUTION_NEIGHBOR_ADDED(46),
    /**
     * Name resolution neighbor removed.
     */
    NAME_RESOLUTION_NEIGHBOR_REMOVED(47),
    /**
     * Flow control receiver added.
     */
    FLOW_CONTROL_RECEIVER_ADDED(48),
    /**
     * Flow control receiver removed.
     */
    FLOW_CONTROL_RECEIVER_REMOVED(49),
    /**
     * Name resolution resolve.
     */
    NAME_RESOLUTION_RESOLVE(50
    ),
    /**
     * Free text event.
     */
    TEXT_DATA(51),
    /**
     * Name resolution lookup.
     */
    NAME_RESOLUTION_LOOKUP(52
    ),
    /**
     * Name resolution host name.
     */
    NAME_RESOLUTION_HOST_NAME(53
    ),

    /**
     * Nak sent.
     */
    NAK_SENT(54),

    /**
     * Resend data upon Nak.
     */
    RESEND(55
    ),

    /**
     * Remove destination by id.
     */
    CMD_IN_REMOVE_DESTINATION_BY_ID(56),

    /**
     * Reject image command received by the driver.
     */
    CMD_IN_REJECT_IMAGE(57),

    /**
     * Nak received.
     */
    NAK_RECEIVED(58),

    /**
     * Publication revoked.
     */
    PUBLICATION_REVOKE(59
    ),

    /**
     * Publication Image revoked.
     */
    PUBLICATION_IMAGE_REVOKE(60
    );

    static final int EVENT_CODE_TYPE = EventCodeType.DRIVER.getTypeCode();

    private static final DriverEventCode[] EVENT_CODE_BY_ID;

    private final int id;

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

    DriverEventCode(final int id)
    {
        this.id = id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
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

    /**
     * Get {@link DriverEventCode} from its event code id.
     *
     * @param eventCodeId to convert.
     * @return {@link DriverEventCode} from its event code id.
     */
    public static DriverEventCode fromEventCodeId(final int eventCodeId)
    {
        return get(eventCodeId - (EVENT_CODE_TYPE << 16));
    }

    /**
     * {@return the full event code for this code}
     */
    public int toEventCodeId()
    {
        return EVENT_CODE_TYPE << 16 | (0xFFFF & id());
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
}
