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

#ifndef AERON_CONSENSUS_MODULE_CONFIGURATION_H
#define AERON_CONSENSUS_MODULE_CONFIGURATION_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * Protocol version
 * ----------------------------------------------------------------------- */
#define AERON_CM_PROTOCOL_MAJOR_VERSION  1
#define AERON_CM_PROTOCOL_MINOR_VERSION  0
#define AERON_CM_PROTOCOL_PATCH_VERSION  0

/* -----------------------------------------------------------------------
 * Snapshot type IDs
 * ----------------------------------------------------------------------- */
#define AERON_CM_SNAPSHOT_TYPE_ID   INT64_C(1)  /* ConsensusModule snapshot */
#define AERON_SVC_SNAPSHOT_TYPE_ID  INT64_C(2)  /* ClusteredService snapshot */

/* -----------------------------------------------------------------------
 * Stream IDs (defaults)
 * ----------------------------------------------------------------------- */
#define AERON_CM_LOG_STREAM_ID_DEFAULT               100
#define AERON_CM_INGRESS_STREAM_ID_DEFAULT           101
#define AERON_CM_CONSENSUS_MODULE_STREAM_ID_DEFAULT  104  /* CM ← service */
#define AERON_CM_SERVICE_STREAM_ID_DEFAULT           105  /* CM → service */
#define AERON_CM_SNAPSHOT_STREAM_ID_DEFAULT          107
#define AERON_CM_CONSENSUS_STREAM_ID_DEFAULT         108  /* inter-node */

/* -----------------------------------------------------------------------
 * Timeouts (nanoseconds)
 * ----------------------------------------------------------------------- */
#define AERON_CM_SESSION_TIMEOUT_NS_DEFAULT            (INT64_C(10)  * 1000000000LL)
#define AERON_CM_LEADER_HEARTBEAT_TIMEOUT_NS_DEFAULT   (INT64_C(10)  * 1000000000LL)
#define AERON_CM_LEADER_HEARTBEAT_INTERVAL_NS_DEFAULT  (INT64_C(200) * 1000000LL)
#define AERON_CM_STARTUP_CANVASS_TIMEOUT_NS_DEFAULT    (INT64_C(60)  * 1000000000LL)
#define AERON_CM_ELECTION_TIMEOUT_NS_DEFAULT           (INT64_C(1)   * 1000000000LL)
#define AERON_CM_ELECTION_STATUS_INTERVAL_NS_DEFAULT   (INT64_C(20)  * 1000000LL)
#define AERON_CM_TERMINATION_TIMEOUT_NS_DEFAULT        (INT64_C(10)  * 1000000000LL)

/* -----------------------------------------------------------------------
 * Member defaults
 * ----------------------------------------------------------------------- */
#define AERON_CM_MEMBER_ID_DEFAULT          0
#define AERON_CM_APPOINTED_LEADER_DEFAULT   (-1)  /* -1 = no appointed leader */
#define AERON_CM_SERVICE_COUNT_DEFAULT      1

/* -----------------------------------------------------------------------
 * Channel defaults
 * ----------------------------------------------------------------------- */
#define AERON_CM_LOG_CHANNEL_DEFAULT        "aeron:udp?term-length=64m"
#define AERON_CM_INGRESS_CHANNEL_DEFAULT    "aeron:udp?endpoint=localhost:9010"
#define AERON_CM_CONSENSUS_CHANNEL_DEFAULT  "aeron:udp?term-length=64m"
#define AERON_CM_CONTROL_CHANNEL_DEFAULT    "aeron:ipc"
#define AERON_CM_SNAPSHOT_CHANNEL_DEFAULT   "aeron:ipc"

/* -----------------------------------------------------------------------
 * Ingress
 * ----------------------------------------------------------------------- */
#define AERON_CM_INGRESS_FRAGMENT_LIMIT_DEFAULT  50

/* -----------------------------------------------------------------------
 * Cluster role — matches Java Cluster.Role
 * ----------------------------------------------------------------------- */
#ifndef AERON_CLUSTER_ROLE_DEFINED
#define AERON_CLUSTER_ROLE_DEFINED
typedef enum aeron_cluster_role_en
{
    AERON_CLUSTER_ROLE_FOLLOWER  = 0,
    AERON_CLUSTER_ROLE_CANDIDATE = 1,
    AERON_CLUSTER_ROLE_LEADER    = 2,
}
aeron_cluster_role_t;
#endif

/* -----------------------------------------------------------------------
 * Election state codes — must match Java ElectionState enum ordinals
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_election_state_en
{
    AERON_ELECTION_INIT                    = 0,
    AERON_ELECTION_CANVASS                 = 1,
    AERON_ELECTION_NOMINATE                = 2,
    AERON_ELECTION_CANDIDATE_BALLOT        = 3,
    AERON_ELECTION_FOLLOWER_BALLOT         = 4,
    AERON_ELECTION_LEADER_LOG_REPLICATION  = 5,
    AERON_ELECTION_LEADER_REPLAY           = 6,
    AERON_ELECTION_LEADER_INIT             = 7,
    AERON_ELECTION_LEADER_READY            = 8,
    AERON_ELECTION_FOLLOWER_LOG_REPLICATION = 9,
    AERON_ELECTION_FOLLOWER_REPLAY         = 10,
    AERON_ELECTION_FOLLOWER_CATCHUP_INIT   = 11,
    AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT  = 12,
    AERON_ELECTION_FOLLOWER_CATCHUP        = 13,
    AERON_ELECTION_FOLLOWER_LOG_INIT       = 14,
    AERON_ELECTION_FOLLOWER_LOG_AWAIT      = 15,
    AERON_ELECTION_FOLLOWER_READY          = 16,
    AERON_ELECTION_CLOSED                  = 17,
}
aeron_cluster_election_state_t;

/* -----------------------------------------------------------------------
 * ConsensusModule state codes — must match Java ConsensusModule.State enum
 * ----------------------------------------------------------------------- */
typedef enum aeron_cm_state_en
{
    AERON_CM_STATE_INIT             = 0,
    AERON_CM_STATE_ACTIVE           = 1,
    AERON_CM_STATE_SUSPENDED        = 2,
    AERON_CM_STATE_SNAPSHOT         = 3,
    AERON_CM_STATE_QUORUM_SNAPSHOT  = 4,
    AERON_CM_STATE_LEAVING          = 5,
    AERON_CM_STATE_CLOSED           = 6,
}
aeron_cm_state_t;

/* -----------------------------------------------------------------------
 * ClusterAction enum — matches Java ClusterAction enum
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_ACTION_SNAPSHOT       0
#define AERON_CLUSTER_ACTION_STANDBY_SNAPSHOT 1

/* -----------------------------------------------------------------------
 * ClusterControl toggle state codes — matches Java ClusterControl.ToggleState
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_TOGGLE_INACTIVE          0
#define AERON_CLUSTER_TOGGLE_NEUTRAL           1
#define AERON_CLUSTER_TOGGLE_SUSPEND           2
#define AERON_CLUSTER_TOGGLE_RESUME            3
#define AERON_CLUSTER_TOGGLE_SNAPSHOT          4
#define AERON_CLUSTER_TOGGLE_SHUTDOWN          5
#define AERON_CLUSTER_TOGGLE_ABORT             6
#define AERON_CLUSTER_TOGGLE_STANDBY_SNAPSHOT  7

/* -----------------------------------------------------------------------
 * AppendPosition flags — matches Java ConsensusModuleAgent.APPEND_POSITION_FLAG_*
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_APPEND_POSITION_FLAG_NONE    ((int8_t)0)
#define AERON_CLUSTER_APPEND_POSITION_FLAG_CATCHUP ((int8_t)1)

/* -----------------------------------------------------------------------
 * Cluster action flags
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_ACTION_FLAGS_DEFAULT          0
#define AERON_CLUSTER_ACTION_FLAGS_STANDBY_SNAPSHOT 1

/* -----------------------------------------------------------------------
 * RecordingLog entry types
 * ----------------------------------------------------------------------- */
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_TERM             0
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_SNAPSHOT         1
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_STANDBY_SNAPSHOT 2
#define AERON_CLUSTER_RECORDING_LOG_ENTRY_TYPE_INVALID_FLAG     (1 << 31)
#define AERON_CLUSTER_RECORDING_LOG_MAX_ENTRY_LENGTH            4096
#define AERON_CLUSTER_RECORDING_LOG_FILE_NAME                   "recording.log"

/* -----------------------------------------------------------------------
 * Environment variable names
 * ----------------------------------------------------------------------- */
#define AERON_CM_MEMBER_ID_ENV_VAR                "AERON_CLUSTER_MEMBER_ID"
#define AERON_CM_APPOINTED_LEADER_ENV_VAR         "AERON_CLUSTER_APPOINTED_LEADER_ID"
#define AERON_CM_LOG_CHANNEL_ENV_VAR              "AERON_CLUSTER_LOG_CHANNEL"
#define AERON_CM_INGRESS_CHANNEL_ENV_VAR          "AERON_CLUSTER_INGRESS_CHANNEL"
#define AERON_CM_CONSENSUS_CHANNEL_ENV_VAR        "AERON_CLUSTER_CONSENSUS_CHANNEL"
#define AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR      "AERON_CLUSTER_CONSENSUS_STREAM_ID"
#define AERON_CM_CLUSTER_MEMBERS_ENV_VAR          "AERON_CLUSTER_MEMBERS"
#define AERON_CM_CLUSTER_DIR_ENV_VAR              "AERON_CLUSTER_DIR"
#define AERON_CM_SERVICE_COUNT_ENV_VAR            "AERON_CLUSTER_SERVICE_COUNT"
#define AERON_CM_SESSION_TIMEOUT_ENV_VAR          "AERON_CLUSTER_SESSION_TIMEOUT"
#define AERON_CM_LEADER_HEARTBEAT_TIMEOUT_ENV_VAR "AERON_CLUSTER_LEADER_HEARTBEAT_TIMEOUT"
#define AERON_CM_LEADER_HEARTBEAT_INTERVAL_ENV_VAR "AERON_CLUSTER_LEADER_HEARTBEAT_INTERVAL"
#define AERON_CM_ELECTION_TIMEOUT_ENV_VAR         "AERON_CLUSTER_ELECTION_TIMEOUT"
#define AERON_CM_APP_VERSION_ENV_VAR              "AERON_CLUSTER_APP_VERSION"

#ifdef __cplusplus
}
#endif

#endif /* AERON_CONSENSUS_MODULE_CONFIGURATION_H */
