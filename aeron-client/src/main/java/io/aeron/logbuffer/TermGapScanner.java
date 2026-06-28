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
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.agrona.BitUtil.align;

/**
 * Scans for gaps in the sequence of bytes in a replicated term buffer between the completed rebuild and the
 * high-water-mark. This can be used for detecting loss and generating a NAK message to the source.
 * <p>
 * <b>Note:</b> This class is threadsafe to be used across multiple threads.
 */
public final class TermGapScanner
{
    /**
     * Handler for notifying of gaps in the log.
     */
    @FunctionalInterface
    public interface GapHandler
    {
        /**
         * Gap detected in log buffer that is being rebuilt.
         *
         * @param termId active term being scanned.
         * @param offset at which the gap begins.
         * @param length of the gap in bytes.
         */
        void onGap(int termId, int offset, int length);
    }

    private TermGapScanner()
    {
    }

    /**
     * Scan for gaps from the scanOffset up to a limit offset. Each gap will be reported to the {@link GapHandler}.
     *
     * @param termBuffer  to be scanned for a gap.
     * @param termId      of the current term buffer.
     * @param termOffset  at which to start scanning.
     * @param limitOffset at which to stop scanning.
     * @param handler     to call if a gap is found.
     * @return offset of last contiguous frame
     */
    public static int scanForGap(
        final UnsafeBuffer termBuffer,
        final int termId,
        final int termOffset,
        final int limitOffset,
        final GapHandler handler)
    {
        int offset = termOffset;
        do
        {
            final int frameLength = frameLengthVolatile(termBuffer, offset);
            if (frameLength <= 0)
            {
                break;
            }

            offset += align(frameLength, FRAME_ALIGNMENT);
        }
        while (offset < limitOffset);

        final int gapBeginOffset = offset;
        if (offset < limitOffset)
        {
            offset += HEADER_LENGTH;
            while (offset < limitOffset)
            {
                if (0 != frameLengthVolatile(termBuffer, offset))
                {
                    break;
                }
                offset += HEADER_LENGTH;
            }

            final int gapLength = offset - gapBeginOffset;
            handler.onGap(termId, gapBeginOffset, gapLength);
        }

        return gapBeginOffset;
    }
}
