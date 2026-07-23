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
package io.aeron.driver.logging;

import io.aeron.logging.CborEncode;
import io.aeron.logging.EncodingState;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.UINT8_TYPED_ARRAY_TAG;

/**
 * CBOR implementation of {@link DriverEventLogger}.
 */
public class CborDriverEventLogger implements DriverEventLogger
{
    private static final int MAX_BUFFER_LENGTH = 4096;

    private final ManyToOneRingBuffer ringBuffer;
    private final ThreadLocal<EncodingState> encodingStateThreadLocal = ThreadLocal.withInitial(EncodingState::new);
    private final ThreadLocal<UnsafeBuffer> bufferViewThreadLocal = ThreadLocal.withInitial(UnsafeBuffer::new);
    private final ThreadLocal<UnsafeBuffer> addressViewThreadLocal = ThreadLocal.withInitial(UnsafeBuffer::new);

    /**
     * Construct with a ring buffer to write messages to.
     *
     * @param ringBuffer to be used by the logger to write encoded events to.
     */
    public CborDriverEventLogger(final ManyToOneRingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    /**
     * {@inheritDoc}
     */
    public void logFrameOut(
        final ByteBuffer buffer, final InetSocketAddress dstAddress
    )
    {
        final long timestamp = System.nanoTime();

        final UnsafeBuffer bufferView = bufferViewThreadLocal.get();
        bufferView.wrap(buffer, buffer.position(), buffer.remaining());
        final UnsafeBuffer addressView = addressViewThreadLocal.get();
        addressView.wrap(dstAddress.getAddress().getAddress());

        int length = CborEncode.headerLength(DriverEventCode.FRAME_OUT, timestamp);
        length += CborEncode.length("buffer", UINT8_TYPED_ARRAY_TAG, bufferView);
        length += CborEncode.length("dstAddress", IPV4_TAG, addressView);
        length += CborEncode.footerLength();

        final int bufferLength = Math.min(length, MAX_BUFFER_LENGTH);
        final int index = ringBuffer.tryClaim(DriverEventCode.FRAME_OUT.toEventCodeId(), bufferLength);

        final EncodingState encodingState = encodingStateThreadLocal.get();
        encodingState.reset(ringBuffer.buffer(), index, bufferLength);

        try
        {
            CborEncode.encodeHeader(encodingState, DriverEventCode.FRAME_OUT, timestamp);
            CborEncode.encode(encodingState, "buffer", UINT8_TYPED_ARRAY_TAG, bufferView, true);
            CborEncode.encode(encodingState, "dstAddress", IPV4_TAG, addressView, false);
            CborEncode.encodeFooter(encodingState);
        }
        finally
        {
            ringBuffer.commit(index);
        }
    }
}
