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
package io.aeron.protocol;

import io.aeron.logging.LogUtil;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.BitUtil.SIZE_OF_SHORT;
import static org.agrona.BitUtil.align;

/**
 * Flyweight for Resolution Entry header.
 * <p>
 * <a target="_blank"
 * href="https://github.com/aeron-io/aeron/wiki/Transport-Protocol-Specification">Protocol Specification</a>
 * wiki page.
 */
public class ResolutionEntryFlyweight extends HeaderFlyweight
{
    /**
     * Resolution type field flag for IPv4.
     */
    public static final byte RES_TYPE_NAME_TO_IP4_MD = 0x01;

    /**
     * Resolution type field flag for IPv6.
     */
    public static final byte RES_TYPE_NAME_TO_IP6_MD = 0x02;

    /**
     * Length of an IPv4 address in bytes.
     */
    public static final int ADDRESS_LENGTH_IP4 = 4;

    /**
     * Length of an IPv6 address in bytes.
     */
    public static final int ADDRESS_LENGTH_IP6 = 16;

    /**
     * (S) - Self flag.
     */
    public static final short SELF_FLAG = 0x80;

    /**
     * Offset in the frame at which the res-type (resolution type) field begins.
     */
    public static final int RES_TYPE_FIELD_OFFSET = 0;

    /**
     * Offset in the frame at which the res-flags field begins.
     */
    public static final int RES_FLAGS_FIELD_OFFSET = 1;

    /**
     * Offset in the frame at which the UDP port field begins.
     */
    public static final int UDP_PORT_FIELD_OFFSET = 2;

    /**
     * Offset in the frame at which the age field begins.
     */
    public static final int AGE_IN_MS_FIELD_OFFSET = 4;

    /**
     * Offset in the frame at which the IP address field begins.
     */
    public static final int ADDRESS_FIELD_OFFSET = 8;

    /**
     * Offset in the frame at which the name field being for IPv4 frame.
     */
    public static final int NAME_FIELD_OFFSET_IP4 = ADDRESS_FIELD_OFFSET + ADDRESS_LENGTH_IP4;

    /**
     * Offset in the frame at which the name field being for IPv6 frame.
     */
    public static final int NAME_FIELD_OFFSET_IP6 = ADDRESS_FIELD_OFFSET + ADDRESS_LENGTH_IP6;

    /**
     * Maximum length allowed for a name.
     */
    public static final int MAX_NAME_LENGTH = 512;

    /**
     * Minimum length in bytes for a valid IPv4 resolution entry, i.e. without commot header and with empty name.
     */
    public static final int MIN_IPV4_FRAME_LENGTH = NAME_FIELD_OFFSET_IP4 + SIZE_OF_SHORT;

    /**
     * Minimum length in bytes for a valid IPv6 resolution entry, i.e. without commot header and with empty name.
     */
    public static final int MIN_IPV6_FRAME_LENGTH = NAME_FIELD_OFFSET_IP6 + SIZE_OF_SHORT;

    /**
     * Default constructor which can later be used to wrap a frame.
     */
    public ResolutionEntryFlyweight()
    {
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public ResolutionEntryFlyweight(final UnsafeBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Construct the flyweight over a frame.
     *
     * @param buffer containing the frame.
     */
    public ResolutionEntryFlyweight(final ByteBuffer buffer)
    {
        super(buffer);
    }

    /**
     * Set the protocol type of the resolution.
     *
     * @param type of the resolution entry protocol.
     * @return this for a fluent API.
     * @see #RES_TYPE_NAME_TO_IP4_MD
     * @see #RES_TYPE_NAME_TO_IP4_MD
     */
    public ResolutionEntryFlyweight resType(final byte type)
    {
        putByte(RES_TYPE_FIELD_OFFSET, type);
        return this;
    }

    /**
     * Get the protocol type of the resolution entry.
     *
     * @return the protocol type of the resolution entry.
     * @see #RES_TYPE_NAME_TO_IP4_MD
     * @see #RES_TYPE_NAME_TO_IP4_MD
     */
    public byte resType()
    {
        return getByte(RES_TYPE_FIELD_OFFSET);
    }

    /**
     * Set the flags for the resolution entry.
     *
     * @param flags field value.
     * @return this for a fluent API.
     * @see #SELF_FLAG
     */
    @Override
    public ResolutionEntryFlyweight flags(final short flags)
    {
        putByte(RES_FLAGS_FIELD_OFFSET, (byte)flags);
        return this;
    }

    /**
     * Get the flags for the resolution entry.
     *
     * @return the flags for the resolution entry.
     */
    @Override
    public short flags()
    {
        return (short)(getByte(RES_FLAGS_FIELD_OFFSET) & 0xFF);
    }

    /**
     * Set the UDP port of the resolver with a given name.
     *
     * @param udpPort of the resolver with a given name.
     * @return this for a fluent API.
     */
    public ResolutionEntryFlyweight udpPort(final int udpPort)
    {
        putShort(UDP_PORT_FIELD_OFFSET, (short)udpPort, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Get the UDP port of the resolver with a given name.
     *
     * @return the UDP port of the resolver with a given name.
     */
    public int udpPort()
    {
        return getShort(UDP_PORT_FIELD_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
    }

    /**
     * Set the age of the entry based on last observed activity.
     *
     * @param ageInMs of the entry based on last observed activity.
     * @return this for a fluent API.
     */
    public ResolutionEntryFlyweight ageInMs(final int ageInMs)
    {
        putInt(AGE_IN_MS_FIELD_OFFSET, ageInMs, LITTLE_ENDIAN);
        return this;
    }

    /**
     * Get the age of the entry based on last observed activity.
     *
     * @return the age of the entry based on last observed activity.
     */
    public int ageInMs()
    {
        return getInt(AGE_IN_MS_FIELD_OFFSET, LITTLE_ENDIAN);
    }

    /**
     * Put the address into the resolution entry. It must come after calling {@link #resType(byte)}.
     *
     * @param address to be written for the resolution entry.
     * @return this for a fluent API.
     */
    public ResolutionEntryFlyweight putAddress(final byte[] address)
    {
        switch (resType())
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                if (ADDRESS_LENGTH_IP4 != address.length)
                {
                    throw new IllegalArgumentException("Invalid address length: " + address.length);
                }
                putBytes(ADDRESS_FIELD_OFFSET, address, 0, ADDRESS_LENGTH_IP4);
                break;

            case RES_TYPE_NAME_TO_IP6_MD:
                if (ADDRESS_LENGTH_IP6 != address.length)
                {
                    throw new IllegalArgumentException("Invalid address length: " + address.length);
                }
                putBytes(ADDRESS_FIELD_OFFSET, address, 0, ADDRESS_LENGTH_IP6);
                break;

            default:
                throw new IllegalStateException("unknown RES_TYPE=" + resType());
        }

        return this;
    }

    /**
     * Get the address for the entry by copying it into the destination buffer.
     *
     * @param dstBuffer into which the address will be copied.
     * @return length of the address copied.
     */
    public int getAddress(final byte[] dstBuffer)
    {
        switch (resType())
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                if (ADDRESS_LENGTH_IP4 > dstBuffer.length)
                {
                    throw new IllegalArgumentException("Insufficient length: " + dstBuffer.length);
                }
                getBytes(ADDRESS_FIELD_OFFSET, dstBuffer, 0, ADDRESS_LENGTH_IP4);
                return ADDRESS_LENGTH_IP4;

            case RES_TYPE_NAME_TO_IP6_MD:
                if (ADDRESS_LENGTH_IP6 > dstBuffer.length)
                {
                    throw new IllegalArgumentException("Insufficient length: " + dstBuffer.length);
                }
                getBytes(ADDRESS_FIELD_OFFSET, dstBuffer, 0, ADDRESS_LENGTH_IP6);
                return ADDRESS_LENGTH_IP6;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + resType());
    }

    /**
     * Append the address to the provided {@link Appendable} in ASCII format.
     *
     * @param appendable to which the address will be appended.
     */
    public void appendAddress(final Appendable appendable)
    {
        try
        {
            switch (resType())
            {
                case RES_TYPE_NAME_TO_IP4_MD:
                {
                    final int i = ADDRESS_FIELD_OFFSET;
                    appendable
                        .append(String.valueOf(getByte(i) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 1) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 2) & 0xFF))
                        .append('.')
                        .append(String.valueOf(getByte(i + 3) & 0xFF));
                    break;
                }

                case RES_TYPE_NAME_TO_IP6_MD:
                {
                    LogUtil.appendIpV6Address(appendable, this, ADDRESS_FIELD_OFFSET);
                    break;
                }

                default:
                    appendable.append("unknown RES_TYPE=").append(String.valueOf(resType()));
            }
        }
        catch (final Exception ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    /**
     * Put the name for the resolution entry into the frame.
     *
     * @param name for the resolution entry.
     * @return this for a fluent API.
     */
    public ResolutionEntryFlyweight putName(final byte[] name)
    {
        final int nameOffset = nameOffset(resType());

        putShort(nameOffset, (short)name.length, LITTLE_ENDIAN);
        putBytes(nameOffset + SIZE_OF_SHORT, name);
        return this;
    }

    /**
     * Get the name for the entry by copying it into the destination buffer.
     *
     * @param dstBuffer into which the name will be copied.
     * @return the number of bytes copied.
     */
    public int getName(final byte[] dstBuffer)
    {
        final int nameOffset = nameOffset(resType());
        final short nameLength = getShort(nameOffset, LITTLE_ENDIAN);
        if (nameLength > dstBuffer.length)
        {
            throw new IllegalArgumentException("Insufficient length: " + dstBuffer.length);
        }

        getBytes(nameOffset + SIZE_OF_SHORT, dstBuffer, 0, nameLength);

        return nameLength;
    }

    /**
     * Append the name to the provided {@link Appendable} in ASCII format.
     *
     * @param appendable to which the name will be appended.
     */
    public void appendName(final StringBuilder appendable)
    {
        final int nameOffset = nameOffset(resType());
        final short nameLength = getShort(nameOffset, LITTLE_ENDIAN);

        getStringWithoutLengthAscii(nameOffset + SIZE_OF_SHORT, nameLength, appendable);
    }

    /**
     * Total length of the entry in bytes.
     *
     * @return total length of the entry in bytes.
     */
    public int entryLength()
    {
        final int nameOffset = nameOffset(resType());

        return align(nameOffset + SIZE_OF_SHORT + getShort(nameOffset, LITTLE_ENDIAN), SIZE_OF_LONG);
    }

    /**
     * Check if the resolution frame is valid given the contents of the wrapped buffer.
     *
     * @return {@code true} if frame is valid.
     */
    public boolean isValid()
    {
        final int capacity = capacity();
        if (capacity >= MIN_IPV4_FRAME_LENGTH)
        {
            final byte resType = resType();
            if (RES_TYPE_NAME_TO_IP4_MD == resType)
            {
                final int nameLength = getShort(NAME_FIELD_OFFSET_IP4, LITTLE_ENDIAN);
                return nameLength > 0 && nameLength <= MAX_NAME_LENGTH &&
                    capacity - MIN_IPV4_FRAME_LENGTH >= nameLength;
            }
            else if (RES_TYPE_NAME_TO_IP6_MD == resType && capacity >= MIN_IPV6_FRAME_LENGTH)
            {
                final int nameLength = getShort(NAME_FIELD_OFFSET_IP6, LITTLE_ENDIAN);
                return nameLength > 0 && nameLength <= MAX_NAME_LENGTH &&
                    capacity - MIN_IPV6_FRAME_LENGTH >= nameLength;
            }
        }
        return false;
    }

    /**
     * Offset in the entry at which the name begins depending on resolution type.
     *
     * @param resType for the protocol family.
     * @return offset in the entry at which the name field begins.
     */
    public static int nameOffset(final byte resType)
    {
        switch (resType)
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                return NAME_FIELD_OFFSET_IP4;

            case RES_TYPE_NAME_TO_IP6_MD:
                return NAME_FIELD_OFFSET_IP6;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + resType);
    }

    /**
     * Calculate the length required for the entry when encoded.
     *
     * @param resType    of the entry.
     * @param nameLength of the name in the entry.
     * @return the required length of the entry in bytes.
     */
    public static int entryLengthRequired(final byte resType, final int nameLength)
    {
        return align(nameOffset(resType) + SIZE_OF_SHORT + nameLength, SIZE_OF_LONG);
    }

    /**
     * Get the length of the address given a resolution type.
     *
     * @param resType to lookup address length for.
     * @return the length of the address given a resolution type.
     */
    public static int addressLength(final byte resType)
    {
        switch (resType)
        {
            case RES_TYPE_NAME_TO_IP4_MD:
                return ADDRESS_LENGTH_IP4;

            case RES_TYPE_NAME_TO_IP6_MD:
                return ADDRESS_LENGTH_IP6;
        }

        throw new IllegalStateException("unknown RES_TYPE=" + resType);
    }

    /**
     * Is the local address a match for binding a socket to ANY IP.
     *
     * @param address       to check which can be IPv4 or IPv6.
     * @param addressLength of the address in bytes.
     * @return true if address is an ANY match otherwise false.
     */
    public static boolean isAnyLocalAddress(final byte[] address, final int addressLength)
    {
        if (addressLength == ADDRESS_LENGTH_IP4)
        {
            return 0 == address[0] && 0 == address[1] && 0 == address[2] && 0 == address[3];
        }
        else if (addressLength == ADDRESS_LENGTH_IP6)
        {
            byte val = 0;

            for (int i = 0; i < ADDRESS_LENGTH_IP6; i++)
            {
                val |= address[i];
            }

            return 0 == val;
        }

        return false;
    }
}
