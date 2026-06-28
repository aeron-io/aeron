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

import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;

/**
 * Detail for the cluster membership from the perspective of a given member.
 */
public class ClusterMembership
{
    /**
     * Create a mutable holder for membership information. Initialise all values to null/NULL_VALUE.
     */
    public ClusterMembership()
    {
    }

    /**
     * Member id that the query is run against.
     */
    public int memberId = NULL_VALUE;

    /**
     * Current leader id from the perspective of the member doing the query.
     */
    public int leaderMemberId = NULL_VALUE;

    /**
     * Current time in nanoseconds when the query was run.
     */
    public long currentTimeNs = NULL_VALUE;

    /**
     * List of active cluster members encoded to a String.
     */
    public String activeMembersStr = null;

    /**
     * Current active members of a cluster.
     */
    public List<ClusterMember> activeMembers = null;
}
