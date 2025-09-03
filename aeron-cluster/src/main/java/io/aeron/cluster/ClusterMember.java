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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.RegistrationException;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.ChannelUri.replacePortWithWildcard;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_PARAM_NAME;
import static io.aeron.archive.client.AeronArchive.NULL_POSITION;

/**
 * Represents a member of the cluster that participates in consensus for storing state from the perspective
 * of any single member. It is not a global view of the cluster, perspectives only exist from a vantage point.
 */
public final class ClusterMember
{
    static final ClusterMember[] EMPTY_MEMBERS = new ClusterMember[0];

    private boolean isBallotSent;
    private boolean isLeader;
    private boolean hasTerminated;
    private int id;
    private long leadershipTermId = Aeron.NULL_VALUE;
    private long candidateTermId = Aeron.NULL_VALUE;
    private long catchupReplaySessionId = Aeron.NULL_VALUE;
    private long catchupReplayCorrelationId = Aeron.NULL_VALUE;
    private long changeCorrelationId = Aeron.NULL_VALUE;
    private long logPosition = NULL_POSITION;
    private long timeOfLastAppendPositionNs = Aeron.NULL_VALUE;
    private ExclusivePublication publication;
    private String consensusChannel;
    private final String consensusEndpoint;
    private final String ingressEndpoint;
    private final String logEndpoint;
    private final String catchupEndpoint;
    private final String archiveEndpoint;
    private final String archiveResponseEndpoint;
    private final String egressResponseEndpoint;
    private final String endpoints;
    private Boolean vote = null;

    /**
     * Construct a new member of the cluster.
     *
     * @param id                unique id for the member.
     * @param ingressEndpoint   address and port endpoint to which cluster clients send ingress.
     * @param consensusEndpoint address and port endpoint to which other cluster members connect.
     * @param logEndpoint       address and port endpoint to which the log is replicated.
     * @param catchupEndpoint   address and port endpoint to which a stream is replayed for catchup to the leader.
     * @param archiveEndpoint   address and port endpoint to which the archive control channel can be reached.
     * @param endpoints         comma separated list of endpoints.
     */
    public ClusterMember(
        final int id,
        final String ingressEndpoint,
        final String consensusEndpoint,
        final String logEndpoint,
        final String catchupEndpoint,
        final String archiveEndpoint,
        final String endpoints)
    {
        this(
            id,
            ingressEndpoint,
            consensusEndpoint,
            logEndpoint,
            catchupEndpoint,
            archiveEndpoint,
            null,
            null,
            endpoints);
    }

    /**
     * Construct a new member of the cluster.
     *
     * @param id                        unique id for the member.
     * @param ingressEndpoint           address and port endpoint to which cluster clients send ingress.
     * @param consensusEndpoint         address and port endpoint to which other cluster members connect.
     * @param logEndpoint               address and port endpoint to which the log is replicated.
     * @param catchupEndpoint           address and port endpoint to which a stream is replayed for catchup to the
     *                                  leader.
     * @param archiveEndpoint           address and port endpoint to which the archive control channel can be reached.
     * @param archiveResponseEndpoint   address and port endpoint to which the archive control response channel can be
     *                                  reached.
     * @param egressResponseEndpoint    address and port endpoint to which the egress response channel can be reached.
     * @param endpoints                 comma separated list of endpoints.
     */
    public ClusterMember(
        final int id,
        final String ingressEndpoint,
        final String consensusEndpoint,
        final String logEndpoint,
        final String catchupEndpoint,
        final String archiveEndpoint,
        final String archiveResponseEndpoint,
        final String egressResponseEndpoint,
        final String endpoints)
    {
        this.id = id;
        this.ingressEndpoint = ingressEndpoint;
        this.consensusEndpoint = consensusEndpoint;
        this.logEndpoint = logEndpoint;
        this.catchupEndpoint = catchupEndpoint;
        this.archiveEndpoint = archiveEndpoint;
        this.archiveResponseEndpoint = archiveResponseEndpoint;
        this.egressResponseEndpoint = egressResponseEndpoint;
        this.endpoints = endpoints;
    }

    /**
     * Reset the state of a cluster member, so it can be canvassed and reestablished.
     */
    public void reset()
    {
        isBallotSent = false;
        isLeader = false;
        hasTerminated = false;
        vote = null;
        candidateTermId = Aeron.NULL_VALUE;
        leadershipTermId = Aeron.NULL_VALUE;
        logPosition = NULL_POSITION;
    }

    /**
     * Set if this member should be leader.
     *
     * @param isLeader value.
     * @return this for a fluent API.
     */
    public ClusterMember isLeader(final boolean isLeader)
    {
        this.isLeader = isLeader;
        return this;
    }

    /**
     * Is this member currently the leader?
     *
     * @return true if this member is currently the leader otherwise false.
     */
    public boolean isLeader()
    {
        return isLeader;
    }

    /**
     * Is the ballot for the current election sent to this member?
     *
     * @param isBallotSent is the ballot for the current election sent to this member?
     * @return this for a fluent API.
     */
    public ClusterMember isBallotSent(final boolean isBallotSent)
    {
        this.isBallotSent = isBallotSent;
        return this;
    }

    /**
     * Is the ballot for the current election sent to this member?
     *
     * @return true if the ballot has been sent for this member in the current election.
     */
    public boolean isBallotSent()
    {
        return isBallotSent;
    }

    /**
     * Set if this member has terminated.
     *
     * @param hasTerminated in notification to the leader.
     * @return this for a fluent API.
     */
    public ClusterMember hasTerminated(final boolean hasTerminated)
    {
        this.hasTerminated = hasTerminated;
        return this;
    }

    /**
     * Has this member notified that it has terminated?
     *
     * @return has this member notified that it has terminated?
     */
    public boolean hasTerminated()
    {
        return hasTerminated;
    }

    /**
     * Set the unique id for this member of the cluster.
     *
     * @param id for this member of the cluster.
     * @return this for a fluent API.
     */
    public ClusterMember id(final int id)
    {
        this.id = id;
        return this;
    }

    /**
     * Unique identity for this member in the cluster.
     *
     * @return the unique identity for this member in the cluster.
     */
    public int id()
    {
        return id;
    }

    /**
     * Set the result of the vote for this member. {@link Boolean#TRUE} means they voted for this member,
     * {@link Boolean#FALSE} means they voted against this member, and null means no vote was received.
     *
     * @param vote for this member in the election.
     * @return this for a fluent API.
     */
    public ClusterMember vote(final Boolean vote)
    {
        this.vote = vote;
        return this;
    }

    /**
     * The status of the vote for this member in an election. {@link Boolean#TRUE} means they voted for this member,
     * {@link Boolean#FALSE} means they voted against this member, and null means no vote was received.
     *
     * @return the status of the vote for this member in an election.
     */
    public Boolean vote()
    {
        return vote;
    }

    /**
     * The leadership term reached for the cluster member.
     *
     * @param leadershipTermId leadership term reached for the cluster member.
     * @return this for a fluent API.
     */
    public ClusterMember leadershipTermId(final long leadershipTermId)
    {
        this.leadershipTermId = leadershipTermId;
        return this;
    }

    /**
     * The leadership term reached for the cluster member.
     *
     * @return The leadership term reached for the cluster member.
     */
    public long leadershipTermId()
    {
        return leadershipTermId;
    }

    /**
     * The log position this member has persisted.
     *
     * @param logPosition this member has persisted.
     * @return this for a fluent API.
     */
    public ClusterMember logPosition(final long logPosition)
    {
        this.logPosition = logPosition;
        return this;
    }

    /**
     * The log position this member has persisted.
     *
     * @return the log position this member has persisted.
     */
    public long logPosition()
    {
        return logPosition;
    }

    /**
     * The candidate term id used when voting.
     *
     * @param candidateTermId used when voting.
     * @return this for a fluent API.
     */
    public ClusterMember candidateTermId(final long candidateTermId)
    {
        this.candidateTermId = candidateTermId;
        return this;
    }

    /**
     * The candidate term id used when voting.
     *
     * @return the candidate term id used when voting.
     */
    public long candidateTermId()
    {
        return candidateTermId;
    }

    /**
     * The session id for the replay when catching up to the leader.
     *
     * @param replaySessionId for the replay when catching up to the leader.
     * @return this for a fluent API.
     */
    public ClusterMember catchupReplaySessionId(final long replaySessionId)
    {
        this.catchupReplaySessionId = replaySessionId;
        return this;
    }

    /**
     * The session id for the replay when catching up to the leader.
     *
     * @return the session id for the replay when catching up to the leader.
     */
    public long catchupReplaySessionId()
    {
        return catchupReplaySessionId;
    }

    /**
     * The correlation id for the replay when catching up to the leader.
     *
     * @param correlationId for the replay when catching up to the leader.
     * @return this for a fluent API.
     */
    public ClusterMember catchupReplayCorrelationId(final long correlationId)
    {
        this.catchupReplayCorrelationId = correlationId;
        return this;
    }

    /**
     * The correlation id for the replay when catching up to the leader.
     *
     * @return the correlation id for the replay when catching up to the leader.
     */
    public long catchupReplayCorrelationId()
    {
        return catchupReplayCorrelationId;
    }

    /**
     * Correlation id assigned to the current action undertaken by the cluster member.
     *
     * @param correlationId assigned to the current action undertaken by the cluster member.
     * @return this for a fluent API.
     */
    public ClusterMember correlationId(final long correlationId)
    {
        this.changeCorrelationId = correlationId;
        return this;
    }

    /**
     * Correlation id assigned to the current action undertaken by the cluster member.
     *
     * @return correlation id assigned to the current action undertaken by the cluster member.
     */
    public long correlationId()
    {
        return changeCorrelationId;
    }

    /**
     * Time (in ns) of last received appendPosition.
     *
     * @param timeNs of the last received appendPosition.
     * @return this for a fluent API.
     */
    public ClusterMember timeOfLastAppendPositionNs(final long timeNs)
    {
        this.timeOfLastAppendPositionNs = timeNs;
        return this;
    }

    /**
     * Time (in ns) of last received appendPosition.
     *
     * @return time (in ns) of last received appendPosition or {@link Aeron#NULL_VALUE} if none received.
     */
    public long timeOfLastAppendPositionNs()
    {
        return timeOfLastAppendPositionNs;
    }

    /**
     * The address:port endpoint for this cluster member that other members connect to for achieving consensus.
     *
     * @return the address:port endpoint for this cluster member that other members will connect to for consensus.
     */
    public String consensusEndpoint()
    {
        return consensusEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member that clients send ingress to.
     *
     * @return the address:port endpoint for this cluster member that listens for ingress.
     */
    public String ingressEndpoint()
    {
        return ingressEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member that the log is replicated to.
     *
     * @return the address:port endpoint for this cluster member that the log is replicated to.
     */
    public String logEndpoint()
    {
        return logEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member to which a stream is replayed for catchup to the leader.
     * <p>
     * It is recommended a port of 0 is used, so it is system allocated to avoid potential clashes.
     *
     * @return the address:port endpoint for this cluster member to which a stream is replayed for catchup to the
     * leader.
     */
    public String catchupEndpoint()
    {
        return catchupEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member that the archive can be reached.
     *
     * @return the address:port endpoint for this cluster member that the archive can be reached.
     */
    public String archiveEndpoint()
    {
        return archiveEndpoint;
    }

    /**
     * The address:port endpoint for this cluster member to use on the archive to set up a response channel for
     * control, replay and replication.
     *
     * @return the address:port endpoint for the archive response channel to be used by archive clients within the
     * cluster.
     */
    public String archiveResponseEndpoint()
    {
        return archiveResponseEndpoint;
    }

    /**
     * The address:port endpoint for the cluster member that will serve as the control address for the egress response
     * channel that client can connect to.
     *
     * @return the endpoint used for the egress response channel.
     */
    public String egressResponseEndpoint()
    {
        return egressResponseEndpoint;
    }

    /**
     * The string of endpoints for this member in a comma separated list in the same order they are parsed.
     *
     * @return list of endpoints for this member in a comma separated list.
     * @see #parse(String)
     */
    public String endpoints()
    {
        return endpoints;
    }

    /**
     * The {@link Publication} used for send status updates to the member.
     *
     * @return {@link Publication} used for send status updates to the member.
     */
    public ExclusivePublication publication()
    {
        return publication;
    }

    /**
     * {@link Publication} used for send status updates to the member.
     *
     * @param publication used for send status updates to the member.
     */
    public void publication(final ExclusivePublication publication)
    {
        this.publication = publication;
    }

    /**
     * Close consensus publication and null out reference.
     *
     * @param errorHandler to capture errors during close.
     */
    public void closePublication(final ErrorHandler errorHandler)
    {
        CloseHelper.close(errorHandler, publication);
        publication = null;
    }

    /**
     * Parse the details for a cluster members from a string.
     * <p>
     * <code>
     * member-id,ingress:port,consensus:port,log:port,catchup:port,archive:port|1,...
     * </code>
     *
     * @param value of the string to be parsed.
     * @return An array of cluster members.
     */
    public static ClusterMember[] parse(final String value)
    {
        if (null == value || value.isEmpty())
        {
            return ClusterMember.EMPTY_MEMBERS;
        }

        final String[] memberValues = value.split("\\|");
        final int length = memberValues.length;
        final ClusterMember[] members = new ClusterMember[length];

        for (int i = 0; i < length; i++)
        {
            final String idAndEndpoints = memberValues[i];
            final String[] memberAttributes = idAndEndpoints.split(",");

            if (memberAttributes.length < 6 || 8 < memberAttributes.length)
            {
                throw new ClusterException("invalid member value: " + idAndEndpoints + " within: " + value);
            }

            final int clusterMemberId;
            try
            {
                clusterMemberId = Integer.parseInt(memberAttributes[0]);
            }
            catch (final NumberFormatException ex)
            {
                throw new ClusterException("invalid cluster member id, must be an integer value", ex);
            }

            final String archiveResponseEndpoint = 6 < memberAttributes.length ? memberAttributes[6] : null;
            final String egressResponseEndpoint = 7 < memberAttributes.length ? memberAttributes[7] : null;

            String endpoints = String.join(
                ",",
                memberAttributes[1],
                memberAttributes[2],
                memberAttributes[3],
                memberAttributes[4],
                memberAttributes[5]);

            if (null != archiveResponseEndpoint)
            {
                endpoints += "," + archiveResponseEndpoint;
            }

            if (null != egressResponseEndpoint)
            {
                endpoints += "," + egressResponseEndpoint;
            }

            members[i] = new ClusterMember(
                clusterMemberId,
                memberAttributes[1],
                memberAttributes[2],
                memberAttributes[3],
                memberAttributes[4],
                memberAttributes[5],
                archiveResponseEndpoint,
                egressResponseEndpoint,
                endpoints);
        }

        return members;
    }

    /**
     * Parse a string containing the endpoints for a cluster node and passing to
     * {@link #ClusterMember(int, String, String, String, String, String, String)}.
     *
     * @param id        of the member node.
     * @param endpoints comma separated.
     * @return the {@link ClusterMember} with the endpoints set.
     */
    public static ClusterMember parseEndpoints(final int id, final String endpoints)
    {
        final String[] memberAttributes = endpoints.split(",");
        if (memberAttributes.length != 5)
        {
            throw new ClusterException("invalid member value: " + endpoints);
        }

        return new ClusterMember(
            id,
            memberAttributes[0],
            memberAttributes[1],
            memberAttributes[2],
            memberAttributes[3],
            memberAttributes[4],
            endpoints);
    }

    /**
     * Encode member endpoints from a cluster members array to a String.
     *
     * @param clusterMembers to fill the details from.
     * @return String representation suitable for use with {@link #parse(String)}.
     */
    public static String encodeAsString(final ClusterMember[] clusterMembers)
    {
        if (0 == clusterMembers.length)
        {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        for (int i = 0, length = clusterMembers.length; i < length; i++)
        {
            final ClusterMember member = clusterMembers[i];

            builder
                .append(member.id)
                .append(',')
                .append(member.endpoints);

            if ((length - 1) != i)
            {
                builder.append('|');
            }
        }

        return builder.toString();
    }

    /**
     * Encode member endpoints from a cluster members {@link List} to a String.
     *
     * @param clusterMembers to fill the details from.
     * @return String representation suitable for use with {@link #parse(String)}.
     */
    public static String encodeAsString(final List<ClusterMember> clusterMembers)
    {
        if (clusterMembers.isEmpty())
        {
            return "";
        }

        final StringBuilder builder = new StringBuilder();

        for (int i = 0, length = clusterMembers.size(); i < length; i++)
        {
            final ClusterMember member = clusterMembers.get(i);

            builder
                .append(member.id)
                .append(',')
                .append(member.endpoints);

            if ((length - 1) != i)
            {
                builder.append('|');
            }
        }

        return builder.toString();
    }

    /**
     * Copy votes from one array of members to another where the {@link #id()}s match.
     *
     * @param srcMembers to copy the votes from.
     * @param dstMembers to copy the votes to.
     */
    public static void copyVotes(final ClusterMember[] srcMembers, final ClusterMember[] dstMembers)
    {
        for (final ClusterMember srcMember : srcMembers)
        {
            final ClusterMember dstMember = findMember(dstMembers, srcMember.id);
            if (null != dstMember)
            {
                dstMember.vote = srcMember.vote;
            }
        }
    }

    /**
     * Add the publications for sending consensus messages to the other members of the cluster.
     *
     * @param members               of the cluster.
     * @param thisMember            this member when adding publications.
     * @param channelTemplate       for the publications.
     * @param streamId              for the publications.
     * @param bindConsensusControl  if the control endpoint should be bound for the publication.
     * @param aeron                 to add the publications to.
     * @param errorHandler          to log registration exceptions to.
     */
    public static void addConsensusPublications(
        final ClusterMember[] members,
        final ClusterMember thisMember,
        final String channelTemplate,
        final int streamId,
        final boolean bindConsensusControl,
        final Aeron aeron,
        final ErrorHandler errorHandler)
    {
        final ChannelUri channelUri = ChannelUri.parse(channelTemplate);

        for (final ClusterMember member : members)
        {
            if (member.id != thisMember.id)
            {
                channelUri.put(ENDPOINT_PARAM_NAME, member.consensusEndpoint);
                setControlEndpoint(channelUri, bindConsensusControl, thisMember.consensusEndpoint);
                member.consensusChannel = channelUri.toString();
                tryAddPublication(member, streamId, aeron, errorHandler);
            }
        }
    }

    /**
     * Add an exclusive {@link Publication} for communicating to a member on the consensus channel.
     *
     * @param thisMember            from which the publication is addressed.
     * @param otherMember           to which the publication is addressed.
     * @param channelTemplate       for the target member.
     * @param streamId              for the target member.
     * @param bindConsensusControl  if the control endpoint should be bound for the publication.
     * @param aeron                 from which the publication will be created.
     * @param errorHandler          to log registration exceptions to.
     */
    public static void addConsensusPublication(
        final ClusterMember thisMember,
        final ClusterMember otherMember,
        final String channelTemplate,
        final int streamId,
        final boolean bindConsensusControl,
        final Aeron aeron,
        final ErrorHandler errorHandler)
    {
        if (null == otherMember.consensusChannel)
        {
            final ChannelUri channelUri = ChannelUri.parse(channelTemplate);
            channelUri.put(ENDPOINT_PARAM_NAME, otherMember.consensusEndpoint);
            setControlEndpoint(channelUri, bindConsensusControl, thisMember.consensusEndpoint);
            otherMember.consensusChannel = channelUri.toString();
        }

        tryAddPublication(otherMember, streamId, aeron, errorHandler);
    }

    /**
     * Try and add an exclusive {@link Publication} for communicating to a member on the consensus channel.
     *
     * @param member       to which the publication is added.
     * @param streamId     for the target member.
     * @param aeron        from which the publication will be created.
     * @param errorHandler to log registration exceptions to.
     */
    public static void tryAddPublication(
        final ClusterMember member, final int streamId, final Aeron aeron, final ErrorHandler errorHandler)
    {
        try
        {
            member.publication = aeron.addExclusivePublication(member.consensusChannel, streamId);
        }
        catch (final RegistrationException ex)
        {
            errorHandler.onError(new ClusterEvent(
                "failed to add consensus publication for member: " + member.id + " - " + ex.getMessage()));
        }
    }

    /**
     * Close the publications associated with members of the cluster used for the consensus protocol.
     *
     * @param errorHandler   to capture errors during close.
     * @param clusterMembers to close the publications for.
     */
    public static void closeConsensusPublications(final ErrorHandler errorHandler, final ClusterMember[] clusterMembers)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.closePublication(errorHandler);
        }
    }

    /**
     * Populate map of {@link ClusterMember}s which can be looked up by id.
     *
     * @param clusterMembers       to populate the map.
     * @param clusterMemberByIdMap to be populated.
     */
    public static void addClusterMemberIds(
        final ClusterMember[] clusterMembers, final Int2ObjectHashMap<ClusterMember> clusterMemberByIdMap)
    {
        for (final ClusterMember member : clusterMembers)
        {
            clusterMemberByIdMap.put(member.id, member);
        }
    }

    /**
     * Check if the cluster leader has an active quorum of cluster followers.
     *
     * @param clusterMembers for the current cluster.
     * @param nowNs          for the current time.
     * @param timeoutNs      after which a follower is not considered active.
     * @return true if quorum of cluster members are considered active.
     */
    public static boolean hasActiveQuorum(
        final ClusterMember[] clusterMembers, final long nowNs, final long timeoutNs)
    {
        int threshold = quorumThreshold(clusterMembers.length);

        for (final ClusterMember member : clusterMembers)
        {
            if (member.isLeader || nowNs <= (member.timeOfLastAppendPositionNs + timeoutNs))
            {
                if (--threshold <= 0)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * The threshold of clusters members required to achieve quorum given a count of cluster members.
     *
     * @param memberCount for the cluster
     * @return the threshold for achieving quorum.
     */
    public static int quorumThreshold(final int memberCount)
    {
        return (memberCount >> 1) + 1;
    }

    /**
     * Calculate the position reached by a quorum of cluster members.
     *
     * @param members         of the cluster.
     * @param rankedPositions temp array to be used for sorting the positions to avoid allocation.
     * @return the position reached by a quorum of cluster members.
     */
    public static long quorumPosition(final ClusterMember[] members, final long[] rankedPositions)
    {
        final int length = rankedPositions.length;
        for (int i = 0; i < length; i++)
        {
            rankedPositions[i] = 0;
        }

        for (final ClusterMember member : members)
        {
            long newPosition = member.logPosition;

            for (int i = 0; i < length; i++)
            {
                final long rankedPosition = rankedPositions[i];

                if (newPosition > rankedPosition)
                {
                    rankedPositions[i] = newPosition;
                    newPosition = rankedPosition;
                }
            }
        }

        return rankedPositions[length - 1];
    }

    /**
     * Reset the log position of all the members to the provided value.
     *
     * @param clusterMembers to be reset.
     * @param logPosition    to set for them all.
     */
    public static void resetLogPositions(final ClusterMember[] clusterMembers, final long logPosition)
    {
        for (final ClusterMember member : clusterMembers)
        {
            member.logPosition = logPosition;
        }
    }

    /**
     * Has the voting members of a cluster arrived at provided position in their log.
     *
     * @param clusterMembers   to check.
     * @param position         to compare the {@link #logPosition()} against.
     * @param leadershipTermId expected of the members.
     * @return true if all members have reached this position otherwise false.
     */
    public static boolean hasVotersAtPosition(
        final ClusterMember[] clusterMembers, final long position, final long leadershipTermId)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (member.vote != null && (member.logPosition < position || member.leadershipTermId != leadershipTermId))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Has a quorum of members of appended a position to their local log.
     *
     * @param clusterMembers   to check.
     * @param position         to compare the {@link #logPosition()} against.
     * @param leadershipTermId expected of the members.
     * @return true if a quorum of members reached this position otherwise false.
     */
    public static boolean hasQuorumAtPosition(
        final ClusterMember[] clusterMembers, final long position, final long leadershipTermId)
    {
        int votes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (member.leadershipTermId == leadershipTermId && member.logPosition >= position)
            {
                ++votes;
            }
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Reset the state of all cluster members.
     *
     * @param members to reset.
     */
    public static void reset(final ClusterMember[] members)
    {
        for (final ClusterMember member : members)
        {
            member.reset();
        }
    }

    /**
     * Become a candidate by voting for yourself and resetting the other votes to {@link Aeron#NULL_VALUE}.
     *
     * @param members           to reset the votes for.
     * @param candidateTermId   for the candidacy.
     * @param candidateMemberId for the election.
     */
    public static void becomeCandidate(
        final ClusterMember[] members, final long candidateTermId, final int candidateMemberId)
    {
        for (final ClusterMember member : members)
        {
            if (member.id == candidateMemberId)
            {
                member.vote(Boolean.TRUE)
                    .candidateTermId(candidateTermId)
                    .isBallotSent(true);
            }
            else
            {
                member.vote(null)
                    .candidateTermId(Aeron.NULL_VALUE)
                    .isBallotSent(false);
            }
        }
    }

    /**
     * Is a member considered unanimously to be leader after voting.
     * <p>
     * If a leader has been gracefully closed then it is not included in the membership for considering a unanimous
     * position but will be considered in the membership for quorum.
     *
     * @param clusterMembers         to check for votes.
     * @param candidateTermId        for the vote.
     * @param gracefulClosedLeaderId id of a leader if gracefully closed otherwise {@link Aeron#NULL_VALUE}.
     * @return {@code true} if all members voted positively.
     */
    public static boolean isUnanimousLeader(
        final ClusterMember[] clusterMembers, final long candidateTermId, final int gracefulClosedLeaderId)
    {
        int votes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (member.id == gracefulClosedLeaderId)
            {
                continue;
            }

            if (candidateTermId != member.candidateTermId || !Boolean.TRUE.equals(member.vote))
            {
                return false;
            }

            votes++;
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Is this member considered leader by a quorum of members by having positive votes being counted for a majority
     * and no negative votes.
     *
     * @param clusterMembers  to check for votes.
     * @param candidateTermId for the vote.
     * @return {@code true} if sufficient positive votes being counted for a majority and no negative votes.
     */
    public static boolean isQuorumLeader(final ClusterMember[] clusterMembers, final long candidateTermId)
    {
        int votes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (candidateTermId == member.candidateTermId)
            {
                if (Boolean.FALSE.equals(member.vote))
                {
                    return false;
                }

                if (Boolean.TRUE.equals(member.vote))
                {
                    ++votes;
                }
            }
        }

        return votes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Determine which member of a cluster this is and check endpoints.
     *
     * @param clusterMembers  for the current cluster which can be null.
     * @param memberId        for this member.
     * @param memberEndpoints for this member.
     * @return the {@link ClusterMember} determined.
     */
    public static ClusterMember determineMember(
        final ClusterMember[] clusterMembers, final int memberId, final String memberEndpoints)
    {
        ClusterMember member = NULL_VALUE != memberId ? ClusterMember.findMember(clusterMembers, memberId) : null;

        if ((null == clusterMembers || 0 == clusterMembers.length) && null == member)
        {
            member = ClusterMember.parseEndpoints(NULL_VALUE, memberEndpoints);
        }
        else
        {
            if (null == member)
            {
                throw new ClusterException("memberId=" + memberId + " not found in clusterMembers");
            }

            if (!memberEndpoints.isEmpty())
            {
                ClusterMember.validateMemberEndpoints(member, memberEndpoints);
            }
        }

        return member;
    }

    /**
     * Check the member with the memberEndpoints.
     *
     * @param member          to check memberEndpoints against.
     * @param memberEndpoints to check member against.
     * @see ConsensusModule.Context#memberEndpoints()
     * @see ConsensusModule.Context#clusterMembers()
     */
    public static void validateMemberEndpoints(final ClusterMember member, final String memberEndpoints)
    {
        final ClusterMember endpoints = ClusterMember.parseEndpoints(Aeron.NULL_VALUE, memberEndpoints);

        if (!areSameEndpoints(member, endpoints))
        {
            throw new ClusterException(
                "clusterMembers and endpoints differ: " + member.endpoints + " != " + memberEndpoints);
        }
    }

    /**
     * Are two cluster members using the same endpoints?
     *
     * @param lhs to compare for equality.
     * @param rhs to compare for equality.
     * @return true if both are using the same endpoints or false if not.
     */
    public static boolean areSameEndpoints(final ClusterMember lhs, final ClusterMember rhs)
    {
        return lhs.ingressEndpoint.equals(rhs.ingressEndpoint) &&
            lhs.consensusEndpoint.equals(rhs.consensusEndpoint) &&
            lhs.logEndpoint.equals(rhs.logEndpoint) &&
            lhs.catchupEndpoint.equals(rhs.catchupEndpoint) &&
            lhs.archiveEndpoint.equals(rhs.archiveEndpoint);
    }

    /**
     * Is the member considered a candidate by a unanimous view to be a suitable candidate in an election.
     * <p>
     * If a leader has been gracefully closed then it is not included in the membership for considering a unanimous
     * position but will be considered in the membership for quorum.
     *
     * @param clusterMembers         to compare the candidate against.
     * @param candidate              for leadership.
     * @param gracefulClosedLeaderId id of a leader if gracefully closed otherwise {@link Aeron#NULL_VALUE}.
     * @return true if the candidate is suitable otherwise false.
     */
    public static boolean isUnanimousCandidate(
        final ClusterMember[] clusterMembers, final ClusterMember candidate, final int gracefulClosedLeaderId)
    {
        int possibleVotes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (member.id == gracefulClosedLeaderId)
            {
                continue;
            }

            if (NULL_POSITION == member.logPosition || compareLog(candidate, member) < 0)
            {
                return false;
            }

            possibleVotes++;
        }

        return possibleVotes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * Has the member achieved a quorum view to be a suitable candidate in an election.
     *
     * @param clusterMembers to compare the candidate against.
     * @param candidate      for leadership.
     * @return true if the candidate is suitable otherwise false.
     */
    public static boolean isQuorumCandidate(final ClusterMember[] clusterMembers, final ClusterMember candidate)
    {
        int possibleVotes = 0;

        for (final ClusterMember member : clusterMembers)
        {
            if (NULL_POSITION == member.logPosition || compareLog(candidate, member) < 0)
            {
                continue;
            }

            ++possibleVotes;
        }

        return possibleVotes >= ClusterMember.quorumThreshold(clusterMembers.length);
    }

    /**
     * The result is positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     *
     * @param lhsLogLeadershipTermId term for which the position is most recent.
     * @param lhsLogPosition         reached in the provided term.
     * @param rhsLogLeadershipTermId term for which the position is most recent.
     * @param rhsLogPosition         reached in the provided term.
     * @return positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     */
    public static int compareLog(
        final long lhsLogLeadershipTermId,
        final long lhsLogPosition,
        final long rhsLogLeadershipTermId,
        final long rhsLogPosition)
    {
        if (lhsLogLeadershipTermId > rhsLogLeadershipTermId)
        {
            return 1;
        }
        else if (lhsLogLeadershipTermId < rhsLogLeadershipTermId)
        {
            return -1;
        }
        else if (lhsLogPosition > rhsLogPosition)
        {
            return 1;
        }
        else if (lhsLogPosition < rhsLogPosition)
        {
            return -1;
        }

        return 0;
    }

    /**
     * The result is positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     *
     * @param lhs member to compare.
     * @param rhs member to compare.
     * @return positive if lhs has the more recent log, zero if logs are equal, and negative if rhs has the more
     * recent log.
     */
    public static int compareLog(final ClusterMember lhs, final ClusterMember rhs)
    {
        return compareLog(lhs.leadershipTermId, lhs.logPosition, rhs.leadershipTermId, rhs.logPosition);
    }

    /**
     * Is the string of member endpoints not duplicated in the members.
     *
     * @param members   to check if the provided endpoints have a duplicate.
     * @param endpoints to check for duplicates.
     * @return true if no duplicate is found otherwise false.
     */
    public static boolean notDuplicateEndpoint(final ClusterMember[] members, final String endpoints)
    {
        for (final ClusterMember member : members)
        {
            if (member.endpoints.equals(endpoints))
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Find the index at which a member id is present.
     *
     * @param clusterMembers to be searched.
     * @param memberId       to search for.
     * @return the index at which the member id is found otherwise {@link ArrayUtil#UNKNOWN_INDEX}.
     */
    public static int findMemberIndex(final ClusterMember[] clusterMembers, final int memberId)
    {
        final int length = clusterMembers.length;
        int index = ArrayUtil.UNKNOWN_INDEX;

        for (int i = 0; i < length; i++)
        {
            if (memberId == clusterMembers[i].id)
            {
                index = i;
            }
        }

        return index;
    }

    /**
     * Find a {@link ClusterMember} with a given id.
     *
     * @param clusterMembers to search.
     * @param memberId       to search for.
     * @return the {@link ClusterMember} if found otherwise null.
     */
    public static ClusterMember findMember(final ClusterMember[] clusterMembers, final int memberId)
    {
        for (final ClusterMember member : clusterMembers)
        {
            if (memberId == member.id)
            {
                return member;
            }
        }

        return null;
    }

    /**
     * Find the highest member id in an array of members.
     *
     * @param clusterMembers to search for the highest id.
     * @return the highest id otherwise {@link Aeron#NULL_VALUE} if empty.
     */
    public static int highMemberId(final ClusterMember[] clusterMembers)
    {
        int highId = Aeron.NULL_VALUE;

        for (final ClusterMember member : clusterMembers)
        {
            highId = Math.max(highId, member.id);
        }

        return highId;
    }

    /**
     * Create a string of ingress endpoints by member id in format {@code id=endpoint,id=endpoint, ...}.
     *
     * @param members for which the ingress endpoints string will be generated.
     * @return a string of ingress endpoints by id.
     */
    public static String ingressEndpoints(final ClusterMember[] members)
    {
        final StringBuilder builder = new StringBuilder(100);

        for (int i = 0, length = members.length; i < length; i++)
        {
            if (0 != i)
            {
                builder.append(',');
            }

            final ClusterMember member = members[i];
            builder.append(member.id).append('=').append(member.ingressEndpoint);
        }

        return builder.toString();
    }

    /**
     * Run through the list of cluster members and set the isLeader field based on the supplied leaderMemberId.
     *
     * @param clusterMembers list of cluster members.
     * @param leaderMemberId memberId of the current leader.
     */
    public static void setIsLeader(final ClusterMember[] clusterMembers, final int leaderMemberId)
    {
        for (final ClusterMember clusterMember : clusterMembers)
        {
            clusterMember.isLeader(clusterMember.id() == leaderMemberId);
        }
    }

    static void setControlEndpoint(final ChannelUri channelUri, final boolean shouldBind, final String endpoint)
    {
        if (!shouldBind)
        {
            return;
        }

        final String controlEndpoint = replacePortWithWildcard(endpoint);
        if (null == controlEndpoint)
        {
            return;
        }

        channelUri.put(MDC_CONTROL_PARAM_NAME, controlEndpoint);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ClusterMember{" +
            "id=" + id +
            ", isBallotSent=" + isBallotSent +
            ", isLeader=" + isLeader +
            ", leadershipTermId=" + leadershipTermId +
            ", logPosition=" + logPosition +
            ", candidateTermId=" + candidateTermId +
            ", catchupReplaySessionId=" + catchupReplaySessionId +
            ", correlationId=" + changeCorrelationId +
            ", timeOfLastAppendPositionNs=" + timeOfLastAppendPositionNs +
            ", ingressEndpoint='" + ingressEndpoint + '\'' +
            ", consensusEndpoint='" + consensusEndpoint + '\'' +
            ", logEndpoint='" + logEndpoint + '\'' +
            ", catchupEndpoint='" + catchupEndpoint + '\'' +
            ", archiveEndpoint='" + archiveEndpoint + '\'' +
            ", endpoints='" + endpoints + '\'' +
            ", publication=" + publication +
            ", vote=" + vote +
            '}';
    }
}
