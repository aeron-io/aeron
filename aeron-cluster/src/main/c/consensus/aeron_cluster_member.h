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

    char    *ingress_endpoint;          /* host:port for client connections */
    char    *consensus_endpoint;        /* host:port for inter-node consensus */
    char    *log_endpoint;              /* host:port for log publication */
    char    *catchup_endpoint;          /* host:port for log catchup */
    char    *archive_endpoint;          /* host:port for archive control channel */
    char    *archive_response_endpoint; /* host:port for archive control responses (optional) */
    char    *egress_response_endpoint;  /* host:port for egress responses (optional) */

    /* State tracked during election and replication */
    int64_t  log_position;
    int64_t  leadership_term_id;
    int64_t  candidate_term_id;
    int64_t  change_correlation_id;
    int64_t  time_of_last_append_position_ns;
    /* Catchup replay state — set by leader when starting catchup for this follower */
    int64_t  catchup_replay_session_id;     /* -1 if no replay in progress */
    int64_t  catchup_replay_correlation_id; /* correlation id for the archive replay request */
    bool     is_leader;
    bool     is_ballot_sent;
    bool     has_terminate_notified;  /* true once TerminationAck received from this member */

    /* Vote tracking (-1 = null/unknown, 0 = false/NO, 1 = true/YES) */
    int8_t   vote;

    /* Consensus channel used for this member's publication (built at publication-add time) */
    char     consensus_channel[512];

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

/* Count members that have voted (vote==1) for a given candidateTermId. */
int aeron_cluster_member_count_votes(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id);

/**
 * Would this member vote for `candidate`?
 * true if member.log_position != -1 AND candidate has equal or better log.
 */
bool aeron_cluster_member_will_vote_for(
    const aeron_cluster_member_t *member,
    const aeron_cluster_member_t *candidate);

/**
 * Is `candidate` a viable candidate for all active members (excluding graceful_closed_leader)?
 * Equivalent to Java ClusterMember.isUnanimousCandidate().
 */
bool aeron_cluster_member_is_unanimous_candidate(
    aeron_cluster_member_t *members,
    int member_count,
    const aeron_cluster_member_t *candidate,
    int32_t graceful_closed_leader_id);

/**
 * Does `candidate` have enough votes from active members (quorum)?
 * Equivalent to Java ClusterMember.isQuorumCandidate().
 */
bool aeron_cluster_member_is_quorum_candidate_for(
    aeron_cluster_member_t *members,
    int member_count,
    const aeron_cluster_member_t *candidate);

/**
 * Has the candidate achieved a quorum of positive votes with no negative votes?
 * Equivalent to Java ClusterMember.isQuorumLeader().
 */
bool aeron_cluster_member_is_quorum_leader(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id);

/**
 * Has the candidate achieved unanimous positive votes (excluding graceful_closed_leader)?
 * Equivalent to Java ClusterMember.isUnanimousLeader().
 */
bool aeron_cluster_member_is_unanimous_leader(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id,
    int32_t graceful_closed_leader_id);

/**
 * Has a quorum of active members reached the given log position?
 * Equivalent to Java ClusterMember.hasQuorumAtPosition().
 */
bool aeron_cluster_member_has_quorum_at_position(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t leadership_term_id,
    int64_t position,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns);

/* -----------------------------------------------------------------------
 * Additional static utilities (mirrors Java ClusterMember statics)
 * ----------------------------------------------------------------------- */

/**
 * Public log comparison: positive if lhs has better (more recent) log.
 * Mirrors Java ClusterMember.compareLog(lhs, rhs).
 */
int aeron_cluster_member_compare_log(
    const aeron_cluster_member_t *lhs,
    const aeron_cluster_member_t *rhs);

/**
 * Public log comparison by raw fields.
 * Mirrors Java ClusterMember.compareLog(lhsTermId, lhsPos, rhsTermId, rhsPos).
 */
int aeron_cluster_member_compare_log_terms(
    int64_t lhs_term_id, int64_t lhs_log_position,
    int64_t rhs_term_id, int64_t rhs_log_position);

/**
 * Reset vote/candidateTermId/ballotSent state on all members.
 * Mirrors Java ClusterMember.reset(members[]).
 */
void aeron_cluster_members_reset(aeron_cluster_member_t *members, int count);

/**
 * Prepare members for a candidacy: self gets vote=YES + ballot sent; others are cleared.
 * Mirrors Java ClusterMember.becomeCandidate(members, candidateTermId, candidateMemberId).
 */
void aeron_cluster_members_become_candidate(
    aeron_cluster_member_t *members, int count,
    int64_t candidate_term_id, int32_t candidate_member_id);

/**
 * Does at least a quorum of members have an active append-position heartbeat?
 * Mirrors Java ClusterMember.hasActiveQuorum(members, nowNs, timeoutNs).
 */
bool aeron_cluster_members_has_active_quorum(
    const aeron_cluster_member_t *members,
    int count,
    int64_t now_ns,
    int64_t timeout_ns);

/**
 * Set is_leader on each member: true only for the member whose id matches leader_id.
 * Mirrors Java ClusterMember.setIsLeader(members, leaderMemberId).
 */
void aeron_cluster_members_set_is_leader(
    aeron_cluster_member_t *members, int count, int32_t leader_id);

/**
 * Are two members using identical endpoints?
 * Mirrors Java ClusterMember.areSameEndpoints(lhs, rhs).
 */
bool aeron_cluster_member_are_same_endpoints(
    const aeron_cluster_member_t *lhs,
    const aeron_cluster_member_t *rhs);

/**
 * Encode all members into "id,ep:ep:ep:ep:ep|id,..." notation.
 * Returns the number of bytes written (excluding NUL), or -1 on truncation.
 * Mirrors Java ClusterMember.encodeAsString(members[]).
 */
int aeron_cluster_members_encode_as_string(
    const aeron_cluster_member_t *members, int count,
    char *buf, size_t buf_len);

/**
 * Build "id=ingressEp,id=ingressEp,..." string.
 * Returns the number of bytes written (excluding NUL), or -1 on truncation.
 * Mirrors Java ClusterMember.ingressEndpoints(members[]).
 */
int aeron_cluster_members_ingress_endpoints(
    const aeron_cluster_member_t *members, int count,
    char *buf, size_t buf_len);

/**
 * Collect all member IDs into out_ids[].
 * out_ids must have room for at least `count` int32_t values.
 * Mirrors Java ClusterMember.addClusterMemberIds().
 */
void aeron_cluster_members_collect_ids(
    const aeron_cluster_member_t *members, int count,
    int32_t *out_ids);

/**
 * Add exclusive publications to each member except the one with id == self_id.
 * channel_template is an Aeron URI whose `endpoint` parameter will be replaced with
 * member->consensus_endpoint.  Blocks (spin-polls) until each publication is ready.
 * Returns 0 on success or -1 on error (AERON_ERR set).
 * Mirrors Java ClusterMember.addConsensusPublications().
 */
int aeron_cluster_members_add_consensus_publications(
    aeron_cluster_member_t *members, int count,
    int32_t self_id,
    aeron_t *aeron,
    const char *channel_template,
    int32_t stream_id);

/**
 * Close the publication on every member that has one.
 * Mirrors Java ClusterMember.closeConsensusPublications().
 */
void aeron_cluster_members_close_consensus_publications(
    aeron_cluster_member_t *members, int count);

/**
 * Locate `self_id` in `members[]`, optionally validating `endpoints_str`.
 * If members is NULL/empty a temporary member is constructed from `endpoints_str`.
 * Returns pointer into the members array, or NULL on error (AERON_ERR set).
 * Mirrors Java ClusterMember.determineMember().
 */
aeron_cluster_member_t *aeron_cluster_members_determine_member(
    aeron_cluster_member_t *members, int count,
    int32_t self_id,
    const char *endpoints_str);

/**
 * Parse a single member from an endpoint string of the form "ep0,ep1,ep2,ep3,ep4"
 * (ingress,consensus,log,catchup,archive — 5 comma-separated values, no ID prefix).
 * Mirrors Java ClusterMember.parseEndpoints(id, endpoints).
 * The `member` struct is zeroed and populated; caller must call aeron_cluster_members_free()
 * on a one-element array, or free individual string fields manually.
 * Returns 0 on success, -1 on error.
 */
int aeron_cluster_member_parse_endpoints(
    aeron_cluster_member_t *member,
    int32_t id,
    const char *endpoints_str);

/**
 * Validate that `member`'s endpoints match those encoded in `endpoints_str`.
 * Mirrors Java ClusterMember.validateMemberEndpoints(member, memberEndpoints).
 * Returns 0 if endpoints match, -1 if they differ (AERON_ERR set).
 */
int aeron_cluster_members_validate_endpoints(
    const aeron_cluster_member_t *member,
    const char *endpoints_str);

/**
 * Add a consensus publication for `other_member` using `this_member`'s control endpoint.
 * The publication channel is built from `channel_template` with `other_member->consensus_endpoint`
 * substituted as the endpoint parameter.
 * Mirrors Java ClusterMember.addConsensusPublication().
 * Returns 0 on success, -1 on error (AERON_ERR set).
 */
int aeron_cluster_member_add_consensus_publication(
    aeron_cluster_member_t *other_member,
    const aeron_cluster_member_t *this_member,
    aeron_t *aeron,
    const char *channel_template,
    int32_t stream_id);

/**
 * Try to add an exclusive publication for `member` using the pre-built
 * `member->consensus_channel`. On RegistrationException logs a warning and
 * returns 0 (non-fatal). Returns -1 only on allocation errors.
 * Mirrors Java ClusterMember.tryAddPublication().
 */
int aeron_cluster_member_try_add_publication(
    aeron_cluster_member_t *member,
    aeron_t *aeron,
    int32_t stream_id);

/**
 * Close the consensus publication for a single member and set publication to NULL.
 * Mirrors Java ClusterMember.closePublication().
 */
void aeron_cluster_member_close_publication(aeron_cluster_member_t *member);

/* -----------------------------------------------------------------------
 * Per-member activity helpers
 * ----------------------------------------------------------------------- */

/**
 * Is this member's heartbeat within the given timeout?
 * Mirrors Java ClusterMember.isActive(nowNs, timeoutNs).
 */
static inline bool aeron_cluster_member_is_active(
    const aeron_cluster_member_t *member, int64_t now_ns, int64_t timeout_ns)
{
    return member->time_of_last_append_position_ns + timeout_ns > now_ns;
}

/**
 * Has this member reached the given term+position and is still heartbeating?
 * Mirrors Java ClusterMember.hasReachedPosition(leadershipTermId, position, nowNs, timeoutNs).
 */
static inline bool aeron_cluster_member_has_reached_position(
    const aeron_cluster_member_t *member,
    int64_t leadership_term_id,
    int64_t position,
    int64_t now_ns,
    int64_t timeout_ns)
{
    return aeron_cluster_member_is_active(member, now_ns, timeout_ns) &&
           member->leadership_term_id == leadership_term_id &&
           member->log_position >= position;
}

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_MEMBER_H */
