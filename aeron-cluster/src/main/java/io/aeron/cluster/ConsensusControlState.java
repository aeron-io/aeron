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
package io.aeron.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;

/**
 * The state needed to allow control of the consensus module.
 * <p>
 * This is a record object being passed to external entities.
 *
 * @see ConsensusModuleExtension
 */
public final class ConsensusControlState
{
    private final ExclusivePublication logPublication;
    private final Subscription leaderLogSubscription;
    private final long logRecordingId;
    private final long leadershipTermId;
    private final String leaderLocalLogChannel;

    /**
     * Record constructor.
     *
     * @param logPublication        publication or null.
     * @param leaderLogSubscription null if a follower, if a leader will have an image that has joined at the log
     *                              position.
     * @param logRecordingId        log recording id.
     * @param leadershipTermId      leadership term id.
     * @param leaderLocalLogChannel leader local log channel or null.
     */
    ConsensusControlState(
        final ExclusivePublication logPublication,
        final Subscription leaderLogSubscription,
        final long logRecordingId,
        final long leadershipTermId,
        final String leaderLocalLogChannel)
    {
        this.logPublication = logPublication;
        this.leaderLogSubscription = leaderLogSubscription;
        this.logRecordingId = logRecordingId;
        this.leadershipTermId = leadershipTermId;
        this.leaderLocalLogChannel = leaderLocalLogChannel;
    }

    /**
     * @return true iff we are the leader (and have the log publication).
     */
    public boolean isLeader()
    {
        return null != logPublication;
    }

    /**
     * @return log publication or null if follower.
     */
    public ExclusivePublication logPublication()
    {
        return logPublication;
    }

    /**
     * @return log recording id.
     */
    public long logRecordingId()
    {
        return logRecordingId;
    }

    /**
     * @return leadership term id.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * @return the local log channel (for services or extension). Only applicable for a leader.
     *
     * @deprecated use {@link #leaderLogSubscription()} instead.
     */
    @Deprecated
    public String leaderLocalLogChannel()
    {
        return leaderLocalLogChannel;
    }

    /**
     * @return a subscription to the log, joined at the log position of the election for a leader node, or null for a
     * follower.
     */
    public Subscription leaderLogSubscription()
    {
        return leaderLogSubscription;
    }
}
