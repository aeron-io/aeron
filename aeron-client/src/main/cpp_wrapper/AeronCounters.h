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

#ifndef AERON_AERONCOUNTERS_H
#define AERON_AERONCOUNTERS_H

#include <cstdint>

namespace aeron
{

namespace AeronCounters
{
    /**
    * System-wide counters for monitoring. These are separate from counters used for position tracking on streams.
    */
    const std::int32_t DRIVER_SYSTEM_COUNTER_TYPE_ID = 0;

    /**
     * The limit as a position in bytes applied to publishers on a session-channel-stream tuple. Publishers will
     * experience back pressure when this position is passed as a means of flow control.
     */
    const std::int32_t DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1;

    /**
     * The position the Sender has reached for sending data to the media on a session-channel-stream tuple.
     */
    const std::int32_t DRIVER_SENDER_POSITION_TYPE_ID = 2;

    /**
     * The highest position the Receiver has observed on a session-channel-stream tuple while rebuilding the stream.
     * It is possible the stream is not complete to this point if the stream has experienced loss.
     */
    const std::int32_t DRIVER_RECEIVER_HWM_TYPE_ID = 3;
    /**
     * The position an individual Subscriber has reached on a session-channel-stream tuple. It is possible to have
     * multiple
     */
    const std::int32_t DRIVER_SUBSCRIBER_POSITION_TYPE_ID = 4;

    /**
     * The highest position the Receiver has rebuilt up to on a session-channel-stream tuple while rebuilding the
     * stream.
     * The stream is complete up to this point.
     */
    const std::int32_t DRIVER_RECEIVER_POS_TYPE_ID = 5;

    /**
     * The status of a send-channel-endpoint represented as a counter value.
     */
    const std::int32_t DRIVER_SEND_CHANNEL_STATUS_TYPE_ID = 6;

    /**
     * The status of a receive-channel-endpoint represented as a counter value.
     */
    const std::int32_t DRIVER_RECEIVE_CHANNEL_STATUS_TYPE_ID = 7;

    /**
     * The position the Sender can immediately send up-to on a session-channel-stream tuple.
     */
    const std::int32_t DRIVER_SENDER_LIMIT_TYPE_ID = 9;

    /**
     * A counter per Image indicating presence of the congestion control.
     */
    const std::int32_t DRIVER_PER_IMAGE_TYPE_ID = 10;

    /**
     * A counter for tracking the last heartbeat of an entity with a given registration id.
     */
    const std::int32_t DRIVER_HEARTBEAT_TYPE_ID = 11;

    /**
     * The position in bytes a publication has reached appending to the log.
     * <p>
     * <b>Note:</b> This is a not a real-time value like the other and is updated one per second for monitoring
     * purposes.
     */
    const std::int32_t DRIVER_PUBLISHER_POS_TYPE_ID = 12;

    /**
     * Count of back-pressure events (BPE)s a sender has experienced on a stream.
     */
    const std::int32_t DRIVER_SENDER_BPE_TYPE_ID = 13;

    /**
     * Count of media driver neighbors for name resolution.
     */
    const std::int32_t NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID = 15;

    /**
     * Count of entries in the name resolver cache.
     */
    const std::int32_t NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID = 16;

    /**
     * Counter used to store the status of a bind address and port for the local end of a channel.
     * <p>
     * When the value is {@link ChannelEndpointStatus#ACTIVE} then the key value and label will be updated with the
     * socket address and port which is bound.
     */
    const std::int32_t DRIVER_LOCAL_SOCKET_ADDRESS_STATUS_TYPE_ID = 14;

    /**
     * Count of number of active receivers for flow control strategy.
     */
    const std::int32_t FLOW_CONTROL_RECEIVERS_COUNTER_TYPE_ID = 17;

    /**
     * Count of number of destinations for multi-destination cast channels.
     */
    const std::int32_t MDC_DESTINATIONS_COUNTER_TYPE_ID = 18;

    // Archive counters
    /**
     * The position a recording has reached when being archived.
     */
    const std::int32_t ARCHIVE_RECORDING_POSITION_TYPE_ID = 100;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    const std::int32_t ARCHIVE_ERROR_COUNT_TYPE_ID = 101;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of concurrent control sessions.
     */
    const std::int32_t ARCHIVE_CONTROL_SESSIONS_TYPE_ID = 102;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of an archive agent.
     */
    const std::int32_t ARCHIVE_MAX_CYCLE_TIME_TYPE_ID = 103;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * an archive agent.
     */
    const std::int32_t ARCHIVE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 104;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took recoder to write a block of
     * data to the storage.
     */
    const std::int32_t ARCHIVE_RECORDER_MAX_WRITE_TIME_TYPE_ID = 105;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes written by the recorder
     * to the storage.
     */
    const std::int32_t ARCHIVE_RECORDER_TOTAL_WRITE_BYTES_TYPE_ID = 106;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the recorder spent writing data to
     * the storage.
     */
    const std::int32_t ARCHIVE_RECORDER_TOTAL_WRITE_TIME_TYPE_ID = 107;

    /**
     * The type id of the {@link Counter} used for keeping track of the max time it took replayer to read a block from
     * the storage.
     */
    const std::int32_t ARCHIVE_REPLAYER_MAX_READ_TIME_TYPE_ID = 108;

    /**
     * The type id of the {@link Counter} used for keeping track of the total number of bytes read by the replayer from
     * the storage.
     */
    const std::int32_t ARCHIVE_REPLAYER_TOTAL_READ_BYTES_TYPE_ID = 109;

    /**
     * The type id of the {@link Counter} used for keeping track of the total time the replayer spent reading data from
     * the storage.
     */
    const std::int32_t ARCHIVE_REPLAYER_TOTAL_READ_TIME_TYPE_ID = 110;

    /**
     * The type id of the {@link Counter} used for tracking the count of active recording sessions.
     */
    const std::int32_t ARCHIVE_RECORDING_SESSION_COUNT_TYPE_ID = 111;

    /**
     * The type id of the {@link Counter} used for tracking the count of active replay sessions.
     */
    const std::int32_t ARCHIVE_REPLAY_SESSION_COUNT_TYPE_ID = 112;

    // Cluster counters

    /**
     * Counter type id for the consensus module state.
     */
    const std::int32_t CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID = 200;

    /**
     * Counter type id for the cluster node role.
     */
    const std::int32_t CLUSTER_NODE_ROLE_TYPE_ID = 201;

    /**
     * Counter type id for the control toggle.
     */
    const std::int32_t CLUSTER_CONTROL_TOGGLE_TYPE_ID = 202;

    /**
     * Counter type id of the commit position.
     */
    const std::int32_t CLUSTER_COMMIT_POSITION_TYPE_ID = 203;

    /**
     * Counter representing the Recovery State for the cluster.
     */
    const std::int32_t CLUSTER_RECOVERY_STATE_TYPE_ID = 204;

    /**
     * Counter type id for count of snapshots taken.
     */
    const std::int32_t CLUSTER_SNAPSHOT_COUNTER_TYPE_ID = 205;

    /**
     * Counter type for count of standby snapshots received.
     */
    const std::int32_t CLUSTER_STANDBY_SNAPSHOT_COUNTER_TYPE_ID = 232;

    /**
     * Type id for election state counter.
     */
    const std::int32_t CLUSTER_ELECTION_STATE_TYPE_ID = 207;

    /**
     * The type id of the {@link Counter} used for the backup state.
     */
    const std::int32_t CLUSTER_BACKUP_STATE_TYPE_ID = 208;

    /**
     * The type id of the {@link Counter} used for the live log position counter.
     */
    const std::int32_t CLUSTER_BACKUP_LIVE_LOG_POSITION_TYPE_ID = 209;

    /**
     * The type id of the {@link Counter} used for the next query deadline counter.
     */
    const std::int32_t CLUSTER_BACKUP_QUERY_DEADLINE_TYPE_ID = 210;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of errors that have occurred.
     */
    const std::int32_t CLUSTER_BACKUP_ERROR_COUNT_TYPE_ID = 211;

    /**
     * Counter type id for the consensus module error count.
     */
    const std::int32_t CLUSTER_CONSENSUS_MODULE_ERROR_COUNT_TYPE_ID = 212;

    /**
     * Counter type id for the number of cluster clients which have been timed out.
     */
    const std::int32_t CLUSTER_CLIENT_TIMEOUT_COUNT_TYPE_ID = 213;

    /**
     * Counter type id for the number of invalid requests which the cluster has received.
     */
    const std::int32_t CLUSTER_INVALID_REQUEST_COUNT_TYPE_ID = 214;

    /**
     * Counter type id for the clustered service error count.
     */
    const std::int32_t CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID = 215;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the consensus module.
     */
    const std::int32_t CLUSTER_MAX_CYCLE_TIME_TYPE_ID = 216;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the consensus module.
     */
    const std::int32_t CLUSTER_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 217;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the service container.
     */
    const std::int32_t CLUSTER_CLUSTERED_SERVICE_MAX_CYCLE_TIME_TYPE_ID = 218;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the service container.
     */
    const std::int32_t CLUSTER_CLUSTERED_SERVICE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 219;

    /**
     * The type id of the {@link Counter} used for the cluster standby state.
     */
    const std::int32_t CLUSTER_STANDBY_STATE_TYPE_ID = 220;

    /**
     * Counter type id for the clustered service error count.
     */
    const std::int32_t CLUSTER_STANDBY_ERROR_COUNT_TYPE_ID = 221;

    /**
     * Counter type for responses to heartbeat request from the cluster.
     */
    const std::int32_t CLUSTER_STANDBY_HEARTBEAT_RESPONSE_COUNT_TYPE_ID = 222;

    /**
     * Standby control toggle type id
     */
    const std::int32_t CLUSTER_STANDBY_CONTROL_TOGGLE_TYPE_ID = 223;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the cluster standby.
     */
    const std::int32_t CLUSTER_STANDBY_MAX_CYCLE_TIME_TYPE_ID = 227;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the cluster standby.
     */
    const std::int32_t CLUSTER_STANDBY_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 228;

    /**
     * The type id of the {@link Counter} to make visible the memberId that the cluster standby is currently using to
     * as a source for the cluster log.
     */
    const std::int32_t CLUSTER_STANDBY_SOURCE_MEMBER_ID_TYPE_ID = 231;

    /**
     * Counter type id for the transition module error count.
     */
    const std::int32_t TRANSITION_MODULE_ERROR_COUNT_TYPE_ID = 226;

    /**
     * The type if of the {@link Counter} used for transition module state
     */
    const std::int32_t TRANSITION_MODULE_STATE_TYPE_ID = 224;

    /**
     * The type id of the {@link Counter} used for keeping track of the max duty cycle time of the transition module.
     */
    const std::int32_t TRANSITION_MODULE_MAX_CYCLE_TIME_TYPE_ID = 229;

    /**
     * The type id of the {@link Counter} used for keeping track of the count of cycle time threshold exceeded of
     * the transition module.
     */
    const std::int32_t TRANSITION_MODULE_CYCLE_TIME_THRESHOLD_EXCEEDED_TYPE_ID = 230;

    /**
     * The type of the {@link Counter} used for handling node specific operations.
     */
    const std::int32_t NODE_CONTROL_TOGGLE_TYPE_ID = 233;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum total snapshot duration.
     */
    const std::int32_t CLUSTER_TOTAL_MAX_SNAPSHOT_DURATION_TYPE_ID = 234;

    /**
     * The type id of the {@link Counter} used for keeping track of the count total snapshot duration
     * has exceeded the threshold.
     */
    const std::int32_t CLUSTER_TOTAL_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 235;

    /**
     * The type id of the {@link Counter} used for keeping track of the maximum snapshot duration
     * for a given clustered service.
     */
    const std::int32_t CLUSTERED_SERVICE_MAX_SNAPSHOT_DURATION_TYPE_ID = 236;

    /**
     * The type id of the {@link Counter} used for keeping track of the count snapshot duration
     * has exceeded the threshold for a given clustered service.
     */
    const std::int32_t CLUSTERED_SERVICE_SNAPSHOT_DURATION_THRESHOLD_EXCEEDED_TYPE_ID = 237;

    /**
     * The type id of the {@link Counter} used for keeping track of the number of elections that have occurred.
     */
    const std::int32_t CLUSTER_ELECTION_COUNT_TYPE_ID = 238;

    /**
     * The type id of the {@link Counter} used for keeping track of the Cluster leadership term id.
     */
    const std::int32_t CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID = 239;
}

}



#endif //AERON_AERONCOUNTERS_H
