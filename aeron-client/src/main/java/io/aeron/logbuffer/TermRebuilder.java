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
package io.aeron.logbuffer;

import org.agrona.concurrent.UnsafeBuffer;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

/**
 * Rebuild a term buffer from received frames which can be out-of-order. The resulting data structure will only
 * monotonically increase in state.
 */
public final class TermRebuilder
{
    private TermRebuilder()
    {
    }

    /**
     * Insert a packet of frames into the log at the appropriate termOffset as indicated by the term termOffset header.
     * <p>
     * If the packet has already been inserted then this is a noop.
     *
     * @param termBuffer into which the packet should be inserted.
     * @param termOffset in the term at which the packet should be inserted.
     * @param packet     containing a sequence of frames.
     * @param length     of the packet of frames in bytes.
     */
    public static void insert(
        final UnsafeBuffer termBuffer, final int termOffset, final UnsafeBuffer packet, final int length)
    {
        if (0 == termBuffer.getInt(termOffset))
        {
            termBuffer.putBytes(termOffset + HEADER_LENGTH, packet, HEADER_LENGTH, length - HEADER_LENGTH);

            termBuffer.putLong(termOffset + 24, packet.getLong(24));
            termBuffer.putLong(termOffset + 16, packet.getLong(16));
            termBuffer.putLong(termOffset + 8, packet.getLong(8));

            termBuffer.putLongRelease(termOffset, packet.getLong(0));
        }
    }
}
