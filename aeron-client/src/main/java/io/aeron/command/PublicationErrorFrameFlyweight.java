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
package io.aeron.command;

import io.aeron.ErrorCode;
import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

/**
 * Control message flyweight error frames received by a publication to be reported to the client.
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                 Publication Registration Id                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                 Destination Registration Id                   |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                           Stream ID                           |
 *  +---------------------------------------------------------------+
 *  |                          Receiver ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                           Group Tag                           |
 *  |                                                               |
 *  +-------------------------------+-------------------------------+
 *  |          Address Type         |            UDP Port           |
 *  +-------------------------------+-------------------------------+
 *  |           IPv4 or IPv6 Address padded out to 16 bytes         |
 *  |                                                               |
 *  |                                                               |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Error Code                           |
 *  +---------------------------------------------------------------+
 *  |                      Error Message Length                     |
 *  +---------------------------------------------------------------+
 *  |                         Error Message                        ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 * @since 1.47.0
 */
public class PublicationErrorFrameFlyweight
{
    private static final int REGISTRATION_ID_OFFSET = 0;
    private static final int IPV6_ADDRESS_LENGTH = 16;
    private static final int IPV4_ADDRESS_LENGTH = BitUtil.SIZE_OF_INT;
    private static final int DESTINATION_REGISTRATION_ID_OFFSET = REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int SESSION_ID_OFFSET = DESTINATION_REGISTRATION_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int STREAM_ID_OFFSET = SESSION_ID_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int RECEIVER_ID_OFFSET = STREAM_ID_OFFSET + BitUtil.SIZE_OF_INT;
    private static final int GROUP_TAG_OFFSET = RECEIVER_ID_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int ADDRESS_TYPE_OFFSET = GROUP_TAG_OFFSET + BitUtil.SIZE_OF_LONG;
    private static final int ADDRESS_PORT_OFFSET = ADDRESS_TYPE_OFFSET + BitUtil.SIZE_OF_SHORT;
    private static final int ADDRESS_OFFSET = ADDRESS_PORT_OFFSET + BitUtil.SIZE_OF_SHORT;
    private static final int ERROR_CODE_OFFSET = ADDRESS_OFFSET + IPV6_ADDRESS_LENGTH;
    private static final int ERROR_MESSAGE_OFFSET = ERROR_CODE_OFFSET + BitUtil.SIZE_OF_INT;
    private static final short ADDRESS_TYPE_IPV4 = 1;
    private static final short ADDRESS_TYPE_IPV6 = 2;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public PublicationErrorFrameFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final PublicationErrorFrameFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Return registration ID of the publication that received the error frame.
     *
     * @return registration ID of the publication.
     */
    public long registrationId()
    {
        return buffer.getLong(offset + REGISTRATION_ID_OFFSET);
    }

    /**
     * Set the registration ID of the publication that received the error frame.
     *
     * @param registrationId of the publication.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_OFFSET, registrationId);
        return this;
    }

    /**
     * Return registration id of the destination that received the error frame. This will only be set if the publication
     * is using manual MDC.
     *
     * @return registration ID of the publication or {@link io.aeron.Aeron#NULL_VALUE}.
     */
    public long destinationRegistrationId()
    {
        return buffer.getLong(offset + DESTINATION_REGISTRATION_ID_OFFSET);
    }

    /**
     * Set the registration ID of the destination that received the error frame. Use {@link io.aeron.Aeron#NULL_VALUE}
     * if not set.
     *
     * @param registrationId of the destination.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight destinationRegistrationId(final long registrationId)
    {
        buffer.putLong(offset + DESTINATION_REGISTRATION_ID_OFFSET, registrationId);
        return this;
    }

    /**
     * Get the stream id field.
     *
     * @return stream id field.
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_OFFSET);
    }

    /**
     * Set the stream id field.
     *
     * @param streamId field value.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_OFFSET, streamId);

        return this;
    }

    /**
     * Get the session id field.
     *
     * @return session id field.
     */
    public int sessionId()
    {
        return buffer.getInt(offset + SESSION_ID_OFFSET);
    }

    /**
     * Set session id field.
     *
     * @param sessionId field value.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight sessionId(final int sessionId)
    {
        buffer.putInt(offset + SESSION_ID_OFFSET, sessionId);

        return this;
    }

    /**
     * Get the receiver id field.
     *
     * @return get the receiver id field.
     */
    public long receiverId()
    {
        return buffer.getLong(offset + RECEIVER_ID_OFFSET);
    }

    /**
     * Set receiver id field.
     *
     * @param receiverId field value.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight receiverId(final long receiverId)
    {
        buffer.putLong(offset + RECEIVER_ID_OFFSET, receiverId);

        return this;
    }

    /**
     * Get the group tag field.
     *
     * @return the group tag field.
     */
    public long groupTag()
    {
        return buffer.getLong(offset + GROUP_TAG_OFFSET);
    }

    /**
     * Set the group tag field.
     *
     * @param groupTag the group tag value.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight groupTag(final long groupTag)
    {
        buffer.putLong(offset + GROUP_TAG_OFFSET, groupTag);

        return this;
    }

    /**
     * Set the source address for the error frame. Store the type (IPv4 or IPv6), port and address as bytes.
     *
     * @param sourceAddress of the error frame.
     * @return this for a fluent API
     */
    public PublicationErrorFrameFlyweight sourceAddress(final InetSocketAddress sourceAddress)
    {
        final short sourcePort = (short)(sourceAddress.getPort() & 0xFFFF);
        final InetAddress address = sourceAddress.getAddress();

        buffer.putShort(offset + ADDRESS_PORT_OFFSET, sourcePort);
        buffer.putBytes(offset + ADDRESS_OFFSET, address.getAddress());
        if (address instanceof Inet4Address)
        {
            buffer.putShort(offset + ADDRESS_TYPE_OFFSET, ADDRESS_TYPE_IPV4);
            buffer.setMemory(
                offset + ADDRESS_OFFSET + IPV4_ADDRESS_LENGTH, IPV6_ADDRESS_LENGTH - IPV4_ADDRESS_LENGTH, (byte)0);
        }
        else if (address instanceof Inet6Address)
        {
            buffer.putShort(offset + ADDRESS_TYPE_OFFSET, ADDRESS_TYPE_IPV6);
        }
        else
        {
            throw new IllegalArgumentException("Unknown address type:" + address.getClass().getSimpleName());
        }

        return this;
    }

    /**
     * Get the source address of this error frame.
     *
     * @return source address of the error frame.
     */
    public InetSocketAddress sourceAddress()
    {
        final short addressType = buffer.getShort(offset + ADDRESS_TYPE_OFFSET);
        final int port = buffer.getShort(offset + ADDRESS_PORT_OFFSET) & 0xFFFF;

        final byte[] address;
        if (ADDRESS_TYPE_IPV4 == addressType)
        {
            address = new byte[IPV4_ADDRESS_LENGTH];
        }
        else if (ADDRESS_TYPE_IPV6 == addressType)
        {
            address = new byte[IPV6_ADDRESS_LENGTH];
        }
        else
        {
            throw new IllegalArgumentException("Unknown address type:" + addressType);
        }

        buffer.getBytes(offset + ADDRESS_OFFSET, address);
        try
        {
            return new InetSocketAddress(Inet4Address.getByAddress(address), port);
        }
        catch (final UnknownHostException ex)
        {
            throw new IllegalArgumentException("Unknown address type:" + addressType, ex);
        }

    }

    /**
     * Error code for the command.
     *
     * @return error code for the command.
     */
    public ErrorCode errorCode()
    {
        return ErrorCode.get(buffer.getInt(offset + ERROR_CODE_OFFSET));
    }

    /**
     * Error code value for the command.
     *
     * @return error code value for the command.
     */
    public int errorCodeValue()
    {
        return buffer.getInt(offset + ERROR_CODE_OFFSET);
    }

    /**
     * Set the error code for the command.
     *
     * @param code for the error.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight errorCode(final ErrorCode code)
    {
        buffer.putInt(offset + ERROR_CODE_OFFSET, code.value());
        return this;
    }

    /**
     * Error message associated with the error.
     *
     * @return error message.
     */
    public String errorMessage()
    {
        return buffer.getStringAscii(offset + ERROR_MESSAGE_OFFSET);
    }

    /**
     * Append the error message to an appendable without allocation.
     *
     * @param appendable to append error message to.
     * @return number bytes copied.
     */
    public int appendMessage(final Appendable appendable)
    {
        return buffer.getStringAscii(offset + ERROR_MESSAGE_OFFSET, appendable);
    }

    /**
     * Set the error message.
     *
     * @param message to associate with the error.
     * @return this for a fluent API.
     */
    public PublicationErrorFrameFlyweight errorMessage(final String message)
    {
        buffer.putStringAscii(offset + ERROR_MESSAGE_OFFSET, message);
        return this;
    }

    /**
     * Length of the error response in bytes.
     *
     * @return length of the error response in bytes.
     */
    public int length()
    {
        return ERROR_MESSAGE_OFFSET + BitUtil.SIZE_OF_INT + buffer.getInt(offset + ERROR_MESSAGE_OFFSET);
    }
}
