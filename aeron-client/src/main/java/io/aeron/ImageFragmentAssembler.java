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
package io.aeron;

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import static io.aeron.logbuffer.FrameDescriptor.*;

/**
 * A {@link FragmentHandler} that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages. This is for a single session on an {@link Image}
 * and not for multiple session {@link Image}s in a {@link Subscription}.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The {@link Header} passed to the delegate on assembling a message will be that of the last fragment.
 */
public class ImageFragmentAssembler implements FragmentHandler
{
    private final FragmentHandler delegate;
    private final BufferBuilder builder;

    /**
     * Construct an adapter to reassemble message fragments and delegate on only whole messages.
     *
     * @param delegate onto which whole messages are forwarded.
     */
    public ImageFragmentAssembler(final FragmentHandler delegate)
    {
        this(delegate, 0, false);
    }

    /**
     * Construct an adapter to reassemble message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for the session.
     */
    public ImageFragmentAssembler(final FragmentHandler delegate, final int initialBufferLength)
    {
        this.delegate = delegate;
        this.builder = new BufferBuilder(initialBufferLength, false);
    }

    /**
     * Construct an adapter to reassemble message fragments and delegate on only whole messages.
     *
     * @param delegate            onto which whole messages are forwarded.
     * @param initialBufferLength to be used for the session.
     * @param isDirectByteBuffer  is the underlying buffer to be a direct {@link java.nio.ByteBuffer}?
     */
    public ImageFragmentAssembler(
        final FragmentHandler delegate, final int initialBufferLength, final boolean isDirectByteBuffer)
    {
        this.delegate = delegate;
        this.builder = new BufferBuilder(initialBufferLength, isDirectByteBuffer);
    }

    /**
     * Get the delegate unto which assembled messages are delegated.
     *
     * @return the delegate unto which assembled messages are delegated.
     */
    public FragmentHandler delegate()
    {
        return delegate;
    }

    /**
     * Get the {@link BufferBuilder} for resetting this assembler.
     *
     * @return the {@link BufferBuilder} for resetting this assembler.
     */
    public BufferBuilder bufferBuilder()
    {
        return builder;
    }

    /**
     * The implementation of {@link FragmentHandler} that reassembles and forwards whole messages.
     *
     * @param buffer containing the data.
     * @param offset at which the data begins.
     * @param length of the data in bytes.
     * @param header representing the metadata for the data.
     */
    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final byte flags = header.flags();

        if ((flags & UNFRAGMENTED) == UNFRAGMENTED)
        {
            delegate.onFragment(buffer, offset, length, header);
        }
        else
        {
            handleFragment(buffer, offset, length, header, flags);
        }
    }

    private void handleFragment(
        final DirectBuffer buffer, final int offset, final int length, final Header header, final byte flags)
    {
        if ((flags & BEGIN_FRAG_FLAG) == BEGIN_FRAG_FLAG)
        {
            builder.reset()
                .captureHeader(header)
                .append(buffer, offset, length)
                .nextTermOffset(header.nextTermOffset());
        }
        else if (header.termOffset() == builder.nextTermOffset())
        {
            builder.append(buffer, offset, length);

            if ((flags & END_FRAG_FLAG) == END_FRAG_FLAG)
            {
                delegate.onFragment(builder.buffer(), 0, builder.limit(), builder.completeHeader(header));
                builder.reset();
            }
            else
            {
                builder.nextTermOffset(header.nextTermOffset());
            }
        }
        else
        {
            builder.reset();
        }
    }
}
