package io.aeron.agent;

import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;

import javax.sound.sampled.AudioFormat;

public class CborClusterEventLogger implements ClusterEventLogger
{
    private static final int HEADER_LENGTH = 16;
    private static final int MAX_BUFFER_LENGTH = 4096;

    private final ManyToOneRingBuffer ringBuffer;
    private final ThreadLocal<EncodingState> encodingStateThreadLocal = ThreadLocal.withInitial(EncodingState::new);

    public CborClusterEventLogger(final ManyToOneRingBuffer ringBuffer)
    {
        this.ringBuffer = ringBuffer;
    }

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
        int length = CborUtil.headerLength(ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);
        length += CborUtil.length("memberId", memberId);
        length += CborUtil.length("oldState", oldState);
        length += CborUtil.length("newState", newState);
        length += CborUtil.length("leaderId", leaderId);
        length += CborUtil.length("candidateTermId", candidateTermId);
        length += CborUtil.length("leadershipTermId", leadershipTermId);
        length += CborUtil.length("logPosition", logPosition);
        length += CborUtil.length("logLeadershipTermId", logLeadershipTermId);
        length += CborUtil.length("appendPosition", appendPosition);
        length += CborUtil.length("catchupPosition", catchupPosition);
        length += CborUtil.length("reason", reason);

        final int bufferLength = Math.min(length, MAX_BUFFER_LENGTH);
        final int index = ringBuffer.tryClaim(ClusterEventCode.ELECTION_STATE_CHANGE.id(), bufferLength);

        final EncodingState encodingState = encodingStateThreadLocal.get();
        encodingState.reset(ringBuffer.buffer(), index, bufferLength);

        try
        {
            CborUtil.encodeHeader(encodingState, ClusterEventCode.ELECTION_STATE_CHANGE, timestamp);

            CborUtil.encode(encodingState, "memberId", memberId);
            CborUtil.encode(encodingState, "oldState", oldState);
            CborUtil.encode(encodingState, "newState", newState);
            CborUtil.encode(encodingState, "leaderId", leaderId);
            CborUtil.encode(encodingState, "candidateTermId", candidateTermId);
            CborUtil.encode(encodingState, "leadershipTermId", leadershipTermId);
            CborUtil.encode(encodingState, "logPosition", logPosition);
            CborUtil.encode(encodingState, "logLeadershipTermId", logLeadershipTermId);
            CborUtil.encode(encodingState, "appendPosition", appendPosition);
            CborUtil.encode(encodingState, "catchupPosition", catchupPosition);
            CborUtil.encode(encodingState, "reason", reason);

            CborUtil.encodeFooter(encodingState);
        }
        finally
        {
            ringBuffer.commit(index);
        }
    }
}
