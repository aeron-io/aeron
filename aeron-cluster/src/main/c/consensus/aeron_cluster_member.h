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

#ifndef AERON_CLUSTER_MEMBER_H
#define AERON_CLUSTER_MEMBER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Endpoint string format within the cluster members string:
 * "memberId,ingressEp:consensusEp:logEp:catchupEp:archiveEp"
 * Multiple members separated by '|'.
 *
 * Example:
 * "0,localhost:20110:localhost:20111:localhost:20113:localhost:20114:localhost:8010|
 *  1,localhost:20120:localhost:20121:localhost:20123:localhost:20124:localhost:8020"
 */

typedef struct aeron_cluster_member_stct
{
    int32_t  id;

    char    *ingress_endpoint;    /* host:port for client connections */
    char    *consensus_endpoint;  /* host:port for inter-node consensus */
    char    *log_endpoint;        /* host:port for log publication */
    char    *catchup_endpoint;    /* host:port for log catchup */
    char    *archive_endpoint;    /* host:port for archive */

    /* State tracked during election and replication */
    int64_t  log_position;
    int64_t  leadership_term_id;
    int64_t  candidate_term_id;
    int64_t  change_correlation_id;
    int64_t  time_of_last_append_position_ns;
    bool     is_leader;

    /* Publication for sending consensus messages to this peer */
    aeron_exclusive_publication_t *publication;
}
aeron_cluster_member_t;

/* -----------------------------------------------------------------------
 * Parse "0,ep1:ep2:ep3:ep4:ep5|1,..." into an array of members.
 * Returns the number of members parsed, or -1 on error.
 * Caller owns the returned array and must call aeron_cluster_members_free().
 * ----------------------------------------------------------------------- */
int aeron_cluster_members_parse(
    const char *members_string,
    aeron_cluster_member_t **members_out,
    int *count_out);

void aeron_cluster_members_free(aeron_cluster_member_t *members, int count);

aeron_cluster_member_t *aeron_cluster_member_find_by_id(
    aeron_cluster_member_t *members, int count, int32_t id);

/* -----------------------------------------------------------------------
 * Quorum helpers
 * ----------------------------------------------------------------------- */

/* Returns the minimum number of members needed for a quorum (floor(n/2)+1). */
static inline int aeron_cluster_member_quorum_threshold(int member_count)
{
    return member_count / 2 + 1;
}

/*
 * Compute the quorum position: sort members' log_position values in descending
 * order and return the value at index (member_count - quorum_threshold).
 * Members that haven't sent a heartbeat within heartbeat_timeout_ns are treated
 * as position -1 (timed out).
 */
int64_t aeron_cluster_member_quorum_position(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns);

/* Returns true if this member has a valid log position for quorum purposes. */
bool aeron_cluster_member_is_quorum_candidate(
    const aeron_cluster_member_t *member,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns);

/* Count members that have voted for a given candidateTermId. */
int aeron_cluster_member_count_votes(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_MEMBER_H */
