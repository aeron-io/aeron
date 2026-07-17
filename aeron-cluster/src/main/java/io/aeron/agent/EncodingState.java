/*
 * Copyright 2014-2026 Real Logic Limited.
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

/**
 * Holds the state of the CBOR encoding operation.
 */
public class EncodingState
{
    private boolean reachedLimit = false;
    private MutableDirectBuffer buffer;
    private int offset;
    private int length;
    private int reservedFooterLength;

    /**
     * @param buffer to write the encoded data to.
     * @param offset to start writing at.
     * @param length of the {@code buffer}.
     */
    public void reset(final MutableDirectBuffer buffer, final int offset, final int length)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        this.reservedFooterLength = 1;
        reachedLimit = false;
    }

    /**
     * @return true if the writing has reached the buffer limit.
     */
    public boolean isReachedLimit()
    {
        return reachedLimit;
    }

    /**
     * @param value to set the reached limit flag to.
     */
    public void reachedLimit(final boolean value)
    {
        // Reserve space for the footer tag.
        // if (!reachedLimit) incrementReservedFooterLength(3);
        reachedLimit = value;
    }

    /**
     * @return the most recently written offset.
     */
    public int offset()
    {
        return offset;
    }

    /**
     * @param offset to set the most recently written offset to.
     */
    public void offset(final int offset)
    {
        this.offset = offset;
    }

    /**
     * @return the length of the buffer.
     */
    public int length()
    {
        return length;
    }

    /**
     * @return the remaining length of the buffer.
     */
    public int remaining()
    {
        return length - offset - reservedFooterLength;
    }

    /**
     * @param length to increment the offset by.
     */
    public void incrementOffset(final int length)
    {
        offset += length;
    }

    /**
     * @return the current footer length reserved.
     */
    public int reservedFooterLength()
    {
        return reservedFooterLength;
    }

    void incrementReservedFooterLength(final int length)
    {
        this.reservedFooterLength += length;
    }

    /**
     * @return the buffer.
     */
    public MutableDirectBuffer buffer()
    {
        return buffer;
    }
}
