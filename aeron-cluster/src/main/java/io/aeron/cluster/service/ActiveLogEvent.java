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
package io.aeron.cluster.service;

/**
 * Event to signal a change of active log to follow.
 */
class ActiveLogEvent
{
    final long logPosition;
    final long maxLogPosition;
    final int memberId;
    final int sessionId;
    final int streamId;
    final boolean isStartup;
    final Cluster.Role role;
    final String channel;
    final boolean isStandby;

    ActiveLogEvent(
        final long logPosition,
        final long maxLogPosition,
        final int memberId,
        final int sessionId,
        final int streamId,
        final boolean isStartup,
        final Cluster.Role role,
        final boolean isStandby,
        final String channel)
    {
        this.logPosition = logPosition;
        this.maxLogPosition = maxLogPosition;
        this.memberId = memberId;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.isStartup = isStartup;
        this.role = role;
        this.channel = channel;
        this.isStandby = isStandby;
    }

    public String toString()
    {
        return "ActiveLogEvent{" +
            ", logPosition=" + logPosition +
            ", maxLogPosition=" + maxLogPosition +
            ", memberId=" + memberId +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", isStartup=" + isStartup +
            ", role=" + role +
            ", isStandby=" + isStandby +
            ", channel='" + channel + '\'' +
            '}';
    }
}
