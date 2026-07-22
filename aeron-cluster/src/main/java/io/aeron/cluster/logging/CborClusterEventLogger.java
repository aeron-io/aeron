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
package io.aeron.cluster.logging;

import io.aeron.logging.CborEncode;
import io.aeron.logging.EncodingState;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

/**
 * CBOR implementation of {@link ClusterEventLogger}.
 */
public class CborClusterEventLogger implements ClusterEventLogger
{
    private static final int HEADER_LENGTH = 16;
    private static final int MAX_BUFFER_LENGTH = 4096;

    private final ManyToOneRingBuffer ringBuffer;
    private final ThreadLocal<EncodingState> encodingStateThreadLocal = ThreadLocal.withInitial(EncodingState::new);

    /**
     * Construct with a ring buffer to write messages to.
     *
     * @param ringBuffer to be used by the logger to write encoded events to.
     */
    public CborClusterEventLogger(final ManyToOneRingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

    /**
     * @param memberId            on which the change has taken place.
     * @param oldState            before the change.
     * @param newState            after the change.
     * @param leaderId            of the cluster.
     * @param candidateTermId     of the node.
     * @param leadershipTermId    of the node.
     * @param logPosition         of the node.
     * @param logLeadershipTermId of the node.
     * @param appendPosition      of the node.
     * @param catchupPosition     of the node.
     * @param reason              for the state transition to occur.
     * @param <E>                 the type of the state enum.
     */
    public <E extends Enum<E>> void logElectionStateChange(
        final int memberId,
        final E oldState,
        final E newState,
        final int leaderId,
        final long candidateTermId,
        final long leadershipTermId,
        final long logPosition,
        final long logLeadershipTermId,
        final long appendPosition,
        final long catchupPosition,
        final String reason)
    {
        final long timestamp = System.nanoTime();
        int length = CborEncode.headerLength(ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        length += CborEncode.length("memberId", memberId);
        length += CborEncode.length("oldState", oldState);
        length += CborEncode.length("newState", newState);
        length += CborEncode.length("leaderId", leaderId);
        length += CborEncode.length("candidateTermId", candidateTermId);
        length += CborEncode.length("leadershipTermId", leadershipTermId);
        length += CborEncode.length("logPosition", logPosition);
        length += CborEncode.length("logLeadershipTermId", logLeadershipTermId);
        length += CborEncode.length("appendPosition", appendPosition);
        length += CborEncode.length("catchupPosition", catchupPosition);
        length += CborEncode.length("reason", reason);
        length += CborEncode.footerLength();

        final int bufferLength = Math.min(length, MAX_BUFFER_LENGTH);
        final int index = ringBuffer.tryClaim(ClusterEventCode.ELECTION_STATE_CHANGE.toEventCodeId(), bufferLength);

        final EncodingState encodingState = encodingStateThreadLocal.get();
        encodingState.reset(ringBuffer.buffer(), index, bufferLength);

        try
        {
            CborEncode.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);

            CborEncode.encode(encodingState, "memberId", memberId);
            CborEncode.encode(encodingState, "oldState", oldState.name());
            CborEncode.encode(encodingState, "newState", newState.name());
            CborEncode.encode(encodingState, "leaderId", leaderId);
            CborEncode.encode(encodingState, "candidateTermId", candidateTermId);
            CborEncode.encode(encodingState, "leadershipTermId", leadershipTermId);
            CborEncode.encode(encodingState, "logPosition", logPosition);
            CborEncode.encode(encodingState, "logLeadershipTermId", logLeadershipTermId);
            CborEncode.encode(encodingState, "appendPosition", appendPosition);
            CborEncode.encode(encodingState, "catchupPosition", catchupPosition);
            CborEncode.encode(encodingState, "reason", reason);

            CborEncode.encodeFooter(encodingState);
        }
        finally
        {
            ringBuffer.commit(index);
        }
    }
}
