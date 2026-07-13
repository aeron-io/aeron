package io.aeron.cluster;

import io.aeron.agent.ClusterEventCode;
import io.aeron.agent.ClusterEventLogger;
import io.aeron.agent.ClusterModuleLogger;
import io.aeron.cluster.codecs.CloseReason;

import java.util.concurrent.TimeUnit;

import static io.aeron.agent.ClusterModuleLogger.isEnabled;

class ClusterLog
{
    static final boolean LOG_APPEND_SESSION_CLOSE_ENABLED = isEnabled(ClusterEventCode.APPEND_SESSION_CLOSE);

    static void logAppendSessionClose(
        final int memberId,
        final long id,
        final CloseReason closeReason,
        final long leadershipTermId,
        final long timestamp,
        final TimeUnit timeUnit)
    {
        if (!LOG_APPEND_SESSION_CLOSE_ENABLED)
        {
            return;
        }

        ClusterEventLogger.LOGGER.logAppendSessionClose(
            memberId, id, closeReason, leadershipTermId, timestamp, timeUnit);
    }
}
