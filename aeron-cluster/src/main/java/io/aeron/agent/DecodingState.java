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

import org.agrona.DirectBuffer;

class DecodingState
{
    private DirectBuffer buffer;
    private int offset;
    private int length;
    private int limit;
    private boolean terminated = false;

    DecodingState()
    {
    }

    void wrap(final DirectBuffer buffer, final int initialOffset, final int length)
    {
        this.buffer = buffer;
        this.offset = initialOffset;
        this.length = length;
        this.limit = offset + length;
    }

    void reset()
    {
        wrap(null, 0, 0);
    }

    void incrementOffset(final int increment)
    {
        this.offset += increment;
        if (this.limit < this.offset)
        {
            throw new CborDecode.InvalidMessage("Terminated prematurely");
        }
    }

    int offset()
    {
        return offset;
    }

    DirectBuffer buffer()
    {
        return this.buffer;
    }

    boolean isTerminated()
    {
        return terminated;
    }

    void terminate()
    {
        this.terminated = true;
    }
}
