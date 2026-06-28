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

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static io.aeron.logbuffer.FrameDescriptor.frameLengthVolatile;
import static io.aeron.logbuffer.FrameDescriptor.isPaddingFrame;
import static org.agrona.BitUtil.align;

/**
 * Scan a term buffer for a block of message fragments including padding. The block must include complete fragments.
 */
public final class TermBlockScanner
{
    private TermBlockScanner()
    {
    }

    /**
     * Scan a term buffer for a block of message fragments from an offset up to a limitOffset.
     * <p>
     * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
     * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
     * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
     * <p>
     * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
     * relevant length of the frame is {@link io.aeron.protocol.DataHeaderFlyweight#HEADER_LENGTH}.
     *
     * @param termBuffer  to scan for message fragments.
     * @param termOffset  at which the scan should begin.
     * @param limitOffset at which the scan should stop.
     * @return the offset at which the scan terminated.
     */
    public static int scan(final UnsafeBuffer termBuffer, final int termOffset, final int limitOffset)
    {
        int offset = termOffset;

        while (offset < limitOffset)
        {
            final int frameLength = frameLengthVolatile(termBuffer, offset);
            if (frameLength <= 0)
            {
                break;
            }

            final int alignedFrameLength = align(frameLength, FRAME_ALIGNMENT);

            if (isPaddingFrame(termBuffer, offset))
            {
                if (termOffset == offset)
                {
                    offset += alignedFrameLength;
                }

                break;
            }

            if (offset + alignedFrameLength > limitOffset)
            {
                break;
            }

            offset += alignedFrameLength;
        }

        return offset;
    }
}
