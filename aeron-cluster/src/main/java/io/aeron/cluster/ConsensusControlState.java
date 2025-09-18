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

/**
 * The state needed to allow control of the consensus module.
 * <p>
 * This is a record object being passed to external entities.
 *
 * @param logPublication        publication or null.
 * @param logRecordingId        log recording id.
 * @param leadershipTermId      leadership term id.
 * @see ConsensusModuleExtension
 */
public record ConsensusControlState(ExclusivePublication logPublication, long logRecordingId, long leadershipTermId)
{
    /**
     * @return true iff we are the leader (and have the log publication).
     */
    public boolean isLeader()
    {
        return null != logPublication;
    }
}
