package io.aeron.cluster;

import io.aeron.cluster.codecs.CloseReason;

import java.util.concurrent.TimeUnit;

public interface ClusterLogger
{
    default void logAppendSessionClose(
        int memberId,
        long id,
        CloseReason closeReason,
        long leadershipTermId,
        TimeUnit timeUnit)
    {
        // No-op.
    }
}
