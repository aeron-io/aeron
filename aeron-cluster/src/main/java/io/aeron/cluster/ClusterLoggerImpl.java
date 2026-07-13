package io.aeron.cluster;

import io.aeron.cluster.codecs.CloseReason;

import java.util.concurrent.TimeUnit;

public class ClusterLoggerImpl implements ClusterLogger
{
    public void logAppendSessionClose(
        final int memberId,
        final long id,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        // encode and write to ring buffer.
    }
}
