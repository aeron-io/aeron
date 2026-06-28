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
import io.aeron.AeronCounters;
import io.aeron.Counter;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.ClusterEvent;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.cluster.service.ClusterCounters;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.Strings;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.errors.DistinctErrorLog;

import static io.aeron.Aeron.NULL_VALUE;

final class ClusterSession implements ClusterClientSession
{
    static final byte[] NULL_PRINCIPAL = ArrayUtil.EMPTY_BYTE_ARRAY;
    static final int MAX_ENCODED_PRINCIPAL_LENGTH = 4 * 1024;
    static final int MAX_ENCODED_MEMBERSHIP_QUERY_LENGTH = 4 * 1024;

    @SuppressWarnings("JavadocVariable")
    enum State
    {
        INIT, CONNECTING, CONNECTED, CHALLENGED, AUTHENTICATED, REJECTED, OPEN, CLOSING, INVALID, CLOSED
    }

    @SuppressWarnings("JavadocVariable")
    enum Action
    {
        CLIENT, BACKUP, HEARTBEAT, STANDBY_SNAPSHOT
    }

    private final long id;
    private final int clusterMemberId;
    private final int responseStreamId;
    private final String responseChannel;
    private final String sessionInfo;
    private boolean hasNewLeaderEventPending = false;
    private boolean hasOpenEventPending = true;
    private long correlationId;
    private long openedLogPosition = AeronArchive.NULL_POSITION;
    private long closedLogPosition = AeronArchive.NULL_POSITION;
    private transient long timeOfLastActivityNs;
    private transient long ingressImageCorrelationId = NULL_VALUE;
    private long responsePublicationId = NULL_VALUE;
    private long counterRegistrationId = NULL_VALUE;
    private Publication responsePublication;
    private Counter counter;
    private State state;
    private String responseDetail = null;
    private EventCode eventCode = null;
    private CloseReason closeReason = CloseReason.NULL_VAL;
    private byte[] encodedPrincipal = NULL_PRINCIPAL;
    private Action action = Action.CLIENT;
    private Object requestInput = null;

    ClusterSession(
        final int clusterMemberId,
        final long sessionId,
        final int responseStreamId,
        final String responseChannel,
        final String sessionInfo)
    {
        this.id = sessionId;
        this.clusterMemberId = clusterMemberId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.sessionInfo = sessionInfo;
        state(State.INIT, "");
    }

    public void close(final Aeron aeron, final ErrorHandler errorHandler, final String reason)
    {
        disconnect(aeron, errorHandler);
        state(State.CLOSED, reason);
    }

    public long id()
    {
        return id;
    }

    public byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    public boolean isOpen()
    {
        return State.OPEN == state;
    }

    public Publication responsePublication()
    {
        return responsePublication;
    }

    public long timeOfLastActivityNs()
    {
        return timeOfLastActivityNs;
    }

    public void timeOfLastActivityNs(final long timeNs)
    {
        timeOfLastActivityNs = timeNs;
    }

    void loadSnapshotState(
        final long correlationId,
        final long openedLogPosition,
        final long timeOfLastActivityNs,
        final CloseReason closeReason)
    {
        this.openedLogPosition = openedLogPosition;
        this.timeOfLastActivityNs = timeOfLastActivityNs;
        this.correlationId = correlationId;
        this.closeReason = closeReason;

        if (CloseReason.NULL_VAL != closeReason)
        {
            state(State.CLOSING, closeReason.name());
        }
        else
        {
            state(State.OPEN, "openedLogPosition=" + openedLogPosition);
        }
    }

    int responseStreamId()
    {
        return responseStreamId;
    }

    String responseChannel()
    {
        return responseChannel;
    }

    void closing(final CloseReason closeReason)
    {
        this.closeReason = closeReason;
        this.hasOpenEventPending = false;
        this.hasNewLeaderEventPending = false;
        this.timeOfLastActivityNs = 0;
        state(State.CLOSING, closeReason.name());
    }

    CloseReason closeReason()
    {
        return closeReason;
    }

    void resetCloseReason()
    {
        closedLogPosition = AeronArchive.NULL_POSITION;
        closeReason = CloseReason.NULL_VAL;
    }

    void asyncConnect(final Aeron aeron, final MutableDirectBuffer tempBuffer, final int clusterId)
    {
        counterRegistrationId = addSessionCounter(aeron, tempBuffer, clusterId);
        responsePublicationId = aeron.asyncAddPublication(responseChannel, responseStreamId);
    }

    void connect(
        final ErrorHandler errorHandler,
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final int clusterId)
    {
        if (null != responsePublication)
        {
            throw new ClusterException("response publication already added");
        }

        counterRegistrationId = addSessionCounter(aeron, tempBuffer, clusterId);

        try
        {
            responsePublication = aeron.addPublication(responseChannel, responseStreamId);
        }
        catch (final RegistrationException ex)
        {
            errorHandler.onError(new ClusterException(
                "failed to connect session response publication: " + ex.getMessage(), AeronException.Category.WARN));
        }
    }

    void disconnect(final Aeron aeron, final ErrorHandler errorHandler)
    {
        if (NULL_VALUE != responsePublicationId)
        {
            aeron.asyncRemovePublication(responsePublicationId);
            responsePublicationId = NULL_VALUE;
        }
        else
        {
            CloseHelper.close(errorHandler, responsePublication);
            responsePublication = null;
        }
        if (NULL_VALUE != counterRegistrationId)
        {
            aeron.asyncRemoveCounter(counterRegistrationId);
            counterRegistrationId = NULL_VALUE;
        }
        else
        {
            CloseHelper.close(errorHandler, counter);
            counter = null;
        }
    }

    boolean isResponsePublicationConnected(final Aeron aeron, final long nowNs)
    {
        if (null == responsePublication)
        {
            if (!aeron.isCommandActive(responsePublicationId))
            {
                responsePublication = aeron.getPublication(responsePublicationId);
                responsePublicationId = NULL_VALUE;

                counter = aeron.getCounter(counterRegistrationId);
                counterRegistrationId = NULL_VALUE;

                if (null != responsePublication)
                {
                    if (null != counter)
                    {
                        AeronCounters.setReferenceId(
                            aeron.context().countersMetaDataBuffer(),
                            aeron.context().countersValuesBuffer(),
                            counter.id(),
                            responsePublication.registrationId());
                        counter.setRelease(id);
                    }

                    timeOfLastActivityNs = nowNs;
                    state(State.CONNECTING, "connecting");
                }
                else
                {
                    state(State.INVALID, "responsePublication is null");
                }
            }
        }

        return null != responsePublication && responsePublication.isConnected();
    }

    long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        if (null == responsePublication)
        {
            return Publication.NOT_CONNECTED;
        }
        else
        {
            return responsePublication.tryClaim(length, bufferClaim);
        }
    }

    long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        if (null == responsePublication)
        {
            return Publication.NOT_CONNECTED;
        }
        else
        {
            return responsePublication.offer(buffer, offset, length);
        }
    }

    State state()
    {
        return state;
    }

    void state(final State newState, final String reason)
    {
        logStateChange(clusterMemberId, id, action, state, newState, reason);
        state = newState;
    }

    void authenticate(final byte[] encodedPrincipal)
    {
        if (encodedPrincipal != null)
        {
            this.encodedPrincipal = encodedPrincipal;
        }

        state(State.AUTHENTICATED, "authenticated");
    }

    void open(final long openedLogPosition)
    {
        this.openedLogPosition = openedLogPosition;
        state(State.OPEN, "openedLogPosition=" + openedLogPosition);
    }

    boolean appendSessionToLogAndSendOpen(
        final LogPublisher logPublisher,
        final EgressPublisher egressPublisher,
        final long leadershipTermId,
        final int memberId,
        final long nowNs,
        final long clusterTimestamp)
    {
        if (responsePublication.availableWindow() > 0)
        {
            final long resultingPosition = logPublisher.appendSessionOpen(this, leadershipTermId, clusterTimestamp);
            if (resultingPosition > 0)
            {
                open(resultingPosition);
                timeOfLastActivityNs(nowNs);
                sendSessionOpenEvent(egressPublisher, leadershipTermId, memberId);
                return true;
            }
        }

        return false;
    }

    int sendSessionOpenEvent(
        final EgressPublisher egressPublisher,
        final long leadershipTermId,
        final int memberId)
    {
        if (egressPublisher.sendEvent(this, leadershipTermId, memberId, EventCode.OK, ""))
        {
            clearOpenEventPending();
            return 1;
        }

        return 0;
    }

    void lastActivityNs(final long timeNs, final long correlationId)
    {
        timeOfLastActivityNs = timeNs;
        this.correlationId = correlationId;
    }

    void reject(final EventCode code, final String responseDetail, final DistinctErrorLog errorLog)
    {
        this.eventCode = code;
        this.responseDetail = responseDetail;
        state(State.REJECTED, Strings.isEmpty(responseDetail) ? code.name() : code.name() + ": " + responseDetail);
        if (null != errorLog)
        {
            errorLog.record(new ClusterEvent(
                code + " " + responseDetail + ", clusterMemberId=" + clusterMemberId + ", id=" + id));
        }
    }

    void redirect(final String ingressEndpoints)
    {
        this.eventCode = EventCode.REDIRECT;
        this.responseDetail = ingressEndpoints;
    }

    EventCode eventCode()
    {
        return eventCode;
    }

    String responseDetail()
    {
        return responseDetail;
    }

    long correlationId()
    {
        return correlationId;
    }

    long openedLogPosition()
    {
        return openedLogPosition;
    }

    void closedLogPosition(final long closedLogPosition)
    {
        this.closedLogPosition = closedLogPosition;
    }

    long closedLogPosition()
    {
        return closedLogPosition;
    }

    void hasNewLeaderEventPending(final boolean flag)
    {
        hasNewLeaderEventPending = flag;
    }

    boolean hasNewLeaderEventPending()
    {
        return hasNewLeaderEventPending;
    }

    boolean hasOpenEventPending()
    {
        return hasOpenEventPending;
    }

    void clearOpenEventPending()
    {
        hasOpenEventPending = false;
    }

    Action action()
    {
        return action;
    }

    void action(final Action action)
    {
        this.action = action;
    }

    void requestInput(final Object requestInput)
    {
        this.requestInput = requestInput;
    }

    Object requestInput()
    {
        return requestInput;
    }

    void linkIngressImage(final Header header)
    {
        if (NULL_VALUE == ingressImageCorrelationId)
        {
            ingressImageCorrelationId = ((Image)header.context()).correlationId();
        }
    }

    void unlinkIngressImage()
    {
        ingressImageCorrelationId = NULL_VALUE;
    }

    long ingressImageCorrelationId()
    {
        return ingressImageCorrelationId;
    }

    boolean hasTimedOut(final long nowNs, final long sessionTimeoutNs)
    {
        return State.INIT != state() && (timeOfLastActivityNs() + sessionTimeoutNs) < nowNs;
    }

    private long addSessionCounter(final Aeron aeron, final MutableDirectBuffer tempBuffer, final int clusterId)
    {
        tempBuffer.putInt(0, clusterId);
        tempBuffer.putLong(BitUtil.SIZE_OF_INT, id);

        final int keyLength = BitUtil.SIZE_OF_INT + BitUtil.SIZE_OF_LONG;

        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, "cluster-session: ");
        labelLength += tempBuffer.putStringWithoutLengthAscii(keyLength + labelLength, sessionInfo);
        labelLength += tempBuffer.putStringWithoutLengthAscii(
            keyLength + labelLength, ClusterCounters.CLUSTER_ID_LABEL_SUFFIX);
        labelLength += tempBuffer.putIntAscii(keyLength + labelLength, clusterId);

        return aeron.asyncAddCounter(
            AeronCounters.CLUSTER_SESSION_TYPE_ID,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            keyLength,
            labelLength);
    }

    private static void logStateChange(
        final int memberId,
        final long sessionId,
        final Action action,
        final State oldState,
        final State newState,
        final String reason)
    {
//        System.out.println("ClusterSession: memberId=" + memberId + " id=" + sessionId + " action=" + action + " " +
//            oldState + " -> " + newState + " " + reason);
    }

    static void checkEncodedPrincipalLength(final byte[] encodedPrincipal)
    {
        if (null != encodedPrincipal && encodedPrincipal.length > MAX_ENCODED_PRINCIPAL_LENGTH)
        {
            throw new ClusterException(
                "encoded principal max length " + MAX_ENCODED_PRINCIPAL_LENGTH +
                " exceeded: length=" + encodedPrincipal.length);
        }
    }

    public String toString()
    {
        return "ClusterSession{" +
            "id=" + id +
            ", clusterMemberId=" + clusterMemberId +
            ", responseStreamId=" + responseStreamId +
            ", responseChannel='" + responseChannel + '\'' +
            ", sessionInfo='" + sessionInfo + '\'' +
            ", hasNewLeaderEventPending=" + hasNewLeaderEventPending +
            ", hasOpenEventPending=" + hasOpenEventPending +
            ", correlationId=" + correlationId +
            ", openedLogPosition=" + openedLogPosition +
            ", closedLogPosition=" + closedLogPosition +
            ", timeOfLastActivityNs=" + timeOfLastActivityNs +
            ", ingressImageCorrelationId=" + ingressImageCorrelationId +
            ", responsePublicationId=" + responsePublicationId +
            ", counterRegistrationId=" + counterRegistrationId +
            ", responsePublication=" + responsePublication +
            ", counter=" + counter +
            ", state=" + state +
            ", responseDetail='" + responseDetail + '\'' +
            ", eventCode=" + eventCode +
            ", closeReason=" + closeReason +
            ", action=" + action +
            ", requestInput=" + requestInput +
            '}';
    }
}
