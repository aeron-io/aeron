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
package io.aeron;

import io.aeron.counter.AeronCounter;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.status.ChannelEndpointStatus;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.util.Objects;

import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * This class serves as a registry for all counter type IDs used by Aeron.
 * <p>
 * Type IDs less than 1000 are reserved for Aeron use. Any custom counters should use a typeId of 1000 or higher.
 * <p>
 * Aeron uses the following specific ranges:
 * <ul>
 *     <li>{@code 0 - 99}: for client/driver counters.</li>
 *     <li>{@code 100 - 199}: for archive counters.</li>
 *     <li>{@code 200 - 299}: for cluster counters.</li>
 * </ul>
 */
public final class AeronCounters
{
    // Client/driver counters
    /**
     * System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
     */
    @AeronCounter
    public static final int DRIVER_SYSTEM_COUNTER_TYPE_ID = 0;

    /**
     * The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will
     * experience back pressure when this position is passed as a means of flow control.
     */
    @AeronCounter
    public static final int DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1;

    /**
     * The position the Sender has reached for sending data to the media on a session-channel-stream tuple.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_POSITION_TYPE_ID = 2;

    /**
     * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
     * It is possible the stream is not complete to this point if the stream has experienced loss.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVER_HWM_TYPE_ID = 3;
    /**
     * The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have
     * multiple
     */
    @AeronCounter(expectedCName = "SUBSCRIPTION_POSITION")
    public static final int DRIVER_SUBSCRIBER_POSITION_TYPE_ID = 4;

    /**
     * The highest position the Receiver has rebuilt up to on a session-channel-stream tuple while rebuilding the
     * stream.The stream is complete up to this point.
     */
    @AeronCounter(expectedCName = "RECEIVER_POSITION")
    public static final int DRIVER_RECEIVER_POS_TYPE_ID = 5;

    /**
     * The status of a send-channel-endpoint represented as a counter value.
     */
    @AeronCounter
    public static final int DRIVER_SEND_CHANNEL_STATUS_TYPE_ID = 6;

    /**
     * The status of a receive-channel-endpoint represented as a counter value.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

    /**
     * The position the Sender can immediately send up-to on a session-channel-stream tuple.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_LIMIT_TYPE_ID = 9;

    /**
     * A counter per Image indicating presence of the congestion control.
     */
    @AeronCounter
    public static final int DRIVER_PER_IMAGE_TYPE_ID = 10;

    /**
     * A counter for tracking the last heartbeat of an entity with a given registration id.
     */
    @AeronCounter(expectedCName = "CLIENT_HEARTBEAT_TIMESTAMP")
    public static final int DRIVER_HEARTBEAT_TYPE_ID = 11;

    /**
     * The position in bytes a publication has reached appending to the log.
     * <p>
     * <b>Note:</b> This is a not a real-time value like the other and is updated one per second for monitoring
     * purposes.
     */
    @AeronCounter(expectedCName = "PUBLISHER_POSITION")
    public static final int DRIVER_PUBLISHER_POS_TYPE_ID = 12;

    /**
     * Count of back-pressure events (BPE)s a sender has experienced on a stream.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_BPE_TYPE_ID = 13;

    /**
     * Count of media driver neighbors for name resolution.
     */
    @AeronCounter(existsInC = false)
    public static final int NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 15;

    /**
     * Count of entries in the name resolver cache.
     */
    @AeronCounter(existsInC = false)
    public static final int NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 16;

    /**
     * Counter used to store the status of a bind address and port for the local end of a channel.
     * <p>
     * When the value is {@link ChannelEndpointStatus#ACTIVE} then the key value and label will be updated with the
     * socket address and port which is bound.
     */
    @AeronCounter(expectedCName = "LOCAL_SOCKADDR")
    public static final int DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

    /**
     * Count of number of active receivers for flow control strategy.
     */
    @AeronCounter(expectedCName = "FC_NUM_RECEIVERS")
    public static final int FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID = 17;

    /**
     * Count of number of destinations for multi-destination cast channels.
     */
    @AeronCounter(expectedCName = "CHANNEL_NUM_DESTINATIONS")
    public static final int MDC_DESTINATIONS_COUNTER_TYPE_ID = 18;

    /**
     * The number of NAK messages received by the Sender.
     */
    @AeronCounter
    public static final int DRIVER_SENDER_NAKS_RECEIVED_TYPE_ID = 19;

    /**
     * The number of NAK messages sent by the Receiver.
     */
    @AeronCounter
    public static final int DRIVER_RECEIVER_NAKS_SENT_TYPE_ID = 20;

    // Archive counters
    /**
     * The position a recording has reached when being archived.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDING_POSITION_TYPE_ID = 100;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    @AeronCounter
    public static final int ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of concurrent control sessions.
     */
    @AeronCounter
    public static final int ARCHIVE_CONTROL_SESSIONS_TYPE_ID = 102;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of an archive agent.
     */
    @AeronCounter
    public static final int ARCHIVE_MAX_CYCLE_TIME_TYPE_ID = 103;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * an archive agent.
     */
    @AeronCounter
    public static final int ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 104;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took recorder to write a block of
     * data to the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID = 105;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes written by the recorder
     * to the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID = 106;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the recorder spent writing data to
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID = 107;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took replayer to read a block from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID = 108;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes read by the replayer from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID = 109;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the replayer spent reading data from
     * the storage.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID = 110;

    /**
     * The type id of the {@link Counter} used for tracking the count of active recording sessions.
     */
    @AeronCounter(existsInC = false)
    public static final int ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID = 111;

    /**
     * The type id of the {@link Counter} used for tracking the count of active replay sessions.
     */
    @AeronCounter
    public static final int ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID = 112;

    // Cluster counters

    /**
     * Counter type id for the consensus module state.
     */
    @AeronCounter
    public static final int CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;

    /**
     * Counter type id for the cluster node role.
     */
    @AeronCounter
    public static final int CLUSTER_NODE_ROLE_TYPE_ID = 201;

    /**
     * Counter type id for the control toggle.
     */
    @AeronCounter
    public static final int CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;

    /**
     * Counter type id of the commit position.
     */
    @AeronCounter
    public static final int CLUSTER_COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Counter representing the Recovery State for the cluster.
     */
    @AeronCounter
    public static final int CLUSTER_RECOVERY_STATE_TYPE_ID = 204;

    /**
     * Counter type id for count of snapshots taken.
     */
    @AeronCounter
    public static final int CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;

    /**
     * Counter type for count of standby snapshots received.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID = 232;

    /**
     * Type id for election state counter.
     */
    @AeronCounter
    public static final int CLUSTER_ELECTION_STATE_TYPE_ID = 207;

    /**
     * The type id of the {@link Counter} used for the backup state.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_STATE_TYPE_ID = 208;

    /**
     * The type id of the {@link Counter} used for the live log position counter.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;

    /**
     * The type id of the {@link Counter} used for the next query deadline counter.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;

    /**
     * The type id of the {@link Counter} used for tracking the number of snapshots downloaded.
     */
    @AeronCounter
    public static final int CLUSTER_BACKUP_SNAPSHOT_RETRIEVE_COUNT_TYPE_ID = 240;

    /**
     * Counter type id for the consensus module error count.
     */
    @AeronCounter
    public static final int CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

    /**
     * Counter type id for the number of cluster clients which have been timed out.
     */
    @AeronCounter
    public static final int CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

    /**
     * Counter type id for the number of invalid requests which the cluster has received.
     */
    @AeronCounter
    public static final int CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

    /**
     * Counter type id for the clustered service error count.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the consensus module.
     */
    @AeronCounter
    public static final int CLUSTER_MAX_CYCLE_TIME_TYPE_ID = 216;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the consensus module.
     */
    @AeronCounter
    public static final int CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 217;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the service container.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID = 218;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the service container.
     */
    @AeronCounter
    public static final int CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 219;

    /**
     * The type id of the {@link Counter} used for the cluster standby state.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_STATE_TYPE_ID = 220;

    /**
     * Counter type id for the clustered service error count.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID = 221;

    /**
     * Counter type for responses to heartbeat request from the cluster.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID = 222;

    /**
     * Standby control toggle type id.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID = 223;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the cluster standby.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID = 227;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the cluster standby.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 228;

    /**
     * The type id of the {@link Counter} to make visible the memberId that the cluster standby is currently using to
     * as a source for the cluster log.
     */
    @AeronCounter
    public static final int CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID = 231;

    /**
     * Counter type id for the transition module error count.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_ERROR_COUNT")
    public static final int TRANSITION_MODULE_ERROR_COUNT_TYPE_ID = 226;

    /**
     * The type if of the {@link Counter} used for transition module state.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_STATE")
    public static final int TRANSITION_MODULE_STATE_TYPE_ID = 224;

    /**
     * Transition module control toggle type id.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_CONTROL_TOGGLE")
    public static final int TRANSITION_MODULE_CONTROL_TOGGLE_TYPE_ID = 225;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the transition module.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_MAX_CYCLE_TIME")
    public static final int TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID = 229;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the transition module.
     */
    @AeronCounter(expectedCName = "CLUSTER_TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED")
    public static final int TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 230;

    /**
     * The type of the {@link Counter} used for handling node specific operations.
     */
    @AeronCounter(existsInC = false)
    public static final int NODE_CONTROL_TOGGLE_TYPE_ID = 233;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum total snapshot duration.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID = 234;

    /**
     * The type id of the {@link Counter} used for keeping track of the count total snapshot duration
     * has exceeded the threshold.
     */
    @AeronCounter
    public static final int CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 235;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum snapshot duration
     * for a given clustered service.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID = 236;

    /**
     * The type id of the {@link Counter} used for keeping track of the count snapshot duration
     * has exceeded the threshold for a given clustered service.
     */
    @AeronCounter
    public static final int CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 237;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of elections that have occurred.
     */
    @AeronCounter
    public static final int CLUSTER_ELECTION_COUNT_TYPE_ID = 238;

    /**
     * The type id of the {@link Counter} used for keeping track of the Cluster leadership term id.
     */
    @AeronCounter(existsInC = false)
    public static final int CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID = 239;

    private AeronCounters()
    {
    }

    /**
     * Checks that the counter specified by {@code counterId} has the counterTypeId that matches the specified value.
     * If not it will throw a {@link io.aeron.exceptions.ConfigurationException}.
     *
     * @param countersReader        to look up the counter type id.
     * @param counterId             counter to reference.
     * @param expectedCounterTypeId the expected type id for the counter.
     * @throws io.aeron.exceptions.ConfigurationException if the type id does not match.
     * @throws IllegalArgumentException                   if the counterId is not valid.
     */
    public static void validateCounterTypeId(
        final CountersReader countersReader,
        final int counterId,
        final int expectedCounterTypeId)
    {
        final int counterTypeId = countersReader.getCounterTypeId(counterId);
        if (expectedCounterTypeId != counterTypeId)
        {
            throw new ConfigurationException(
                "The type for counterId=" + counterId +
                ", typeId=" + counterTypeId +
                " does not match the expected=" + expectedCounterTypeId);
        }
    }

    /**
     * Convenience overload for {@link AeronCounters#validateCounterTypeId(CountersReader, int, int)}.
     *
     * @param aeron                 to resolve a counters' reader.
     * @param counter               to be checked for the appropriate counterTypeId.
     * @param expectedCounterTypeId the expected type id for the counter.
     * @throws io.aeron.exceptions.ConfigurationException if the type id does not match.
     * @throws IllegalArgumentException                   if the counterId is not valid.
     * @see AeronCounters#validateCounterTypeId(CountersReader, int, int)
     */
    public static void validateCounterTypeId(
        final Aeron aeron,
        final Counter counter,
        final int expectedCounterTypeId)
    {
        validateCounterTypeId(aeron.countersReader(), counter.id(), expectedCounterTypeId);
    }

    /**
     * Append version information at the end of the counter's label.
     *
     * @param tempBuffer     to append label to.
     * @param offset         at which current label data ends.
     * @param fullVersion    of the component.
     * @param commitHashCode Git commit SHA.
     * @return length of the suffix appended.
     */
    public static int appendVersionInfo(
        final MutableDirectBuffer tempBuffer, final int offset, final String fullVersion, final String commitHashCode)
    {
        int length = tempBuffer.putStringWithoutLengthAscii(offset, " ");
        length += tempBuffer.putStringWithoutLengthAscii(
            offset + length, formatVersionInfo(fullVersion, commitHashCode));
        return length;
    }

    /**
     * Append specified {@code value} at the end of the counter's label as ASCII encoded value up to the
     * {@link CountersReader#MAX_LABEL_LENGTH}.
     *
     * @param metaDataBuffer containing the counter metadata.
     * @param counterId      to append version info to.
     * @param value          to be appended to the label.
     * @return number of bytes that got appended.
     * @throws IllegalArgumentException if {@code counterId} is invalid or points to non-allocated counter.
     */
    public static int appendToLabel(
        final AtomicBuffer metaDataBuffer, final int counterId, final String value)
    {
        Objects.requireNonNull(metaDataBuffer);
        if (counterId < 0)
        {
            throw new IllegalArgumentException("counter id " + counterId + " is negative");
        }

        final int maxCounterId = (metaDataBuffer.capacity() / CountersReader.METADATA_LENGTH) - 1;
        if (counterId > maxCounterId)
        {
            throw new IllegalArgumentException(
                "counter id " + counterId + " out of range: 0 - maxCounterId=" + maxCounterId);
        }

        final int counterMetaDataOffset = CountersReader.metaDataOffset(counterId);
        final int state = metaDataBuffer.getIntVolatile(counterMetaDataOffset);
        if (CountersReader.RECORD_ALLOCATED != state)
        {
            throw new IllegalArgumentException("counter id " + counterId + " is not allocated, state: " + state);
        }

        final int existingLabelLength = metaDataBuffer.getInt(counterMetaDataOffset + CountersReader.LABEL_OFFSET);
        final int remainingLabelLength = CountersReader.MAX_LABEL_LENGTH - existingLabelLength;

        final int writtenLength = metaDataBuffer.putStringWithoutLengthAscii(
            counterMetaDataOffset + CountersReader.LABEL_OFFSET + SIZE_OF_INT + existingLabelLength,
            value,
            0,
            remainingLabelLength);
        if (writtenLength > 0)
        {
            metaDataBuffer.putIntRelease(
                counterMetaDataOffset + CountersReader.LABEL_OFFSET, existingLabelLength + writtenLength);
        }

        return writtenLength;
    }

    /**
     * Format version information for display purposes.
     *
     * @param fullVersion of the component.
     * @param commitHash  Git commit SHA.
     * @return formatted String.
     */
    public static String formatVersionInfo(final String fullVersion, final String commitHash)
    {
        return "version=" + fullVersion + " commit=" + commitHash;
    }
}
