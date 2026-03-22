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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include "aeron_cluster_member.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* -----------------------------------------------------------------------
 * Internal: deep-copy a string field
 * ----------------------------------------------------------------------- */
static int member_set_string(char **target, const char *src, size_t len)
{
    if (aeron_alloc((void **)target, len + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate member endpoint string");
        return -1;
    }
    memcpy(*target, src, len);
    (*target)[len] = '\0';
    return 0;
}

/* -----------------------------------------------------------------------
 * Parse: "memberId,ep1:ep2:ep3:ep4:ep5"
 * ep indices: 0=ingress 1=consensus 2=log 3=catchup 4=archive
 * ----------------------------------------------------------------------- */
static int parse_single_member(aeron_cluster_member_t *m, const char *str, size_t len)
{
    memset(m, 0, sizeof(aeron_cluster_member_t));
    m->log_position           = -1;
    m->leadership_term_id     = -1;
    m->candidate_term_id      = -1;
    m->time_of_last_append_position_ns = 0;

    /* Copy to local buffer for parsing */
    char buf[4096];
    if (len >= sizeof(buf)) { len = sizeof(buf) - 1; }
    memcpy(buf, str, len);
    buf[len] = '\0';

    /* First token is memberId */
    char *p = buf;
    char *comma = strchr(p, ',');
    if (NULL == comma)
    {
        AERON_SET_ERR(EINVAL, "malformed member entry (no comma): %s", buf);
        return -1;
    }
    *comma = '\0';
    m->id = (int32_t)atoi(p);
    p = comma + 1;

    /* Remaining tokens are endpoints separated by ':' */
    /* Note: endpoints themselves contain ':' for host:port, so split on the
     * known number of ':' separators between endpoint groups. In the standard
     * format, each endpoint is "host:port", so the format is:
     * "host:port:host:port:host:port:host:port:host:port"
     * which has 9 colons total for 5 host:port pairs.
     * We split on every 2nd colon to get each endpoint pair.
     */
    char *endpoints[5] = {NULL, NULL, NULL, NULL, NULL};
    int ep_idx = 0;
    endpoints[ep_idx] = p;

    /* Count colons to find boundaries between host:port groups */
    int colon_count = 0;
    for (char *c = p; *c != '\0' && ep_idx < 4; c++)
    {
        if (':' == *c)
        {
            colon_count++;
            if (0 == (colon_count % 2))  /* every 2nd colon is an endpoint boundary */
            {
                *c = '\0';
                ep_idx++;
                endpoints[ep_idx] = c + 1;
            }
        }
    }

    char **targets[] = { &m->ingress_endpoint, &m->consensus_endpoint,
                         &m->log_endpoint, &m->catchup_endpoint, &m->archive_endpoint };
    for (int i = 0; i <= ep_idx && i < 5; i++)
    {
        if (NULL != endpoints[i] && member_set_string(targets[i], endpoints[i],
            strlen(endpoints[i])) < 0)
        {
            return -1;
        }
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Parse the full members string "m0|m1|m2|..."
 * ----------------------------------------------------------------------- */
int aeron_cluster_members_parse(
    const char *members_string,
    aeron_cluster_member_t **members_out,
    int *count_out)
{
    if (NULL == members_string || '\0' == *members_string)
    {
        *members_out = NULL;
        *count_out   = 0;
        return 0;
    }

    /* Count '|' separators to determine member count */
    int count = 1;
    for (const char *p = members_string; *p; p++) { if ('|' == *p) count++; }

    aeron_cluster_member_t *members = NULL;
    if (aeron_alloc((void **)&members,
        (size_t)count * sizeof(aeron_cluster_member_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate members array");
        return -1;
    }

    const char *start = members_string;
    int idx = 0;
    while (idx < count)
    {
        const char *pipe = strchr(start, '|');
        size_t seg_len = (NULL != pipe) ? (size_t)(pipe - start) : strlen(start);

        if (parse_single_member(&members[idx], start, seg_len) < 0)
        {
            aeron_cluster_members_free(members, idx);
            return -1;
        }

        idx++;
        start = (NULL != pipe) ? pipe + 1 : start + seg_len;
    }

    *members_out = members;
    *count_out   = count;
    return 0;
}

void aeron_cluster_members_free(aeron_cluster_member_t *members, int count)
{
    if (NULL == members) { return; }
    for (int i = 0; i < count; i++)
    {
        aeron_free(members[i].ingress_endpoint);
        aeron_free(members[i].consensus_endpoint);
        aeron_free(members[i].log_endpoint);
        aeron_free(members[i].catchup_endpoint);
        aeron_free(members[i].archive_endpoint);
    }
    aeron_free(members);
}

aeron_cluster_member_t *aeron_cluster_member_find_by_id(
    aeron_cluster_member_t *members, int count, int32_t id)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id == id) { return &members[i]; }
    }
    return NULL;
}

/* -----------------------------------------------------------------------
 * Quorum position
 * ----------------------------------------------------------------------- */
static int compare_int64_desc(const void *a, const void *b)
{
    int64_t va = *(const int64_t *)a;
    int64_t vb = *(const int64_t *)b;
    return (va < vb) ? 1 : (va > vb) ? -1 : 0;
}

int64_t aeron_cluster_member_quorum_position(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns)
{
    /* Build an array of positions (timed-out = -1), sort descending,
     * return the value at index (member_count - quorum_threshold). */
    int64_t positions[64];  /* max 64 members */
    if (member_count > 64) { member_count = 64; }

    for (int i = 0; i < member_count; i++)
    {
        if (aeron_cluster_member_is_quorum_candidate(&members[i], now_ns, heartbeat_timeout_ns))
        {
            positions[i] = members[i].log_position;
        }
        else
        {
            positions[i] = -1;
        }
    }

    qsort(positions, (size_t)member_count, sizeof(int64_t), compare_int64_desc);

    /* The quorum threshold index (0-based): member_count - floor(n/2) - 1 */
    int threshold_idx = member_count - aeron_cluster_member_quorum_threshold(member_count);
    return positions[threshold_idx];
}

bool aeron_cluster_member_is_quorum_candidate(
    const aeron_cluster_member_t *member,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns)
{
    return member->log_position >= 0 &&
           (now_ns - member->time_of_last_append_position_ns) < heartbeat_timeout_ns;
}

int aeron_cluster_member_count_votes(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id)
{
    int votes = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (members[i].candidate_term_id == candidate_term_id) { votes++; }
    }
    return votes;
}

/* compareLog: positive if lhs has better log */
static int compare_log(
    int64_t lhs_term, int64_t lhs_pos,
    int64_t rhs_term, int64_t rhs_pos)
{
    if (lhs_term != rhs_term) { return (lhs_term > rhs_term) ? 1 : -1; }
    if (lhs_pos  != rhs_pos)  { return (lhs_pos  > rhs_pos)  ? 1 : -1; }
    return 0;
}

bool aeron_cluster_member_will_vote_for(
    const aeron_cluster_member_t *member,
    const aeron_cluster_member_t *candidate)
{
    if (member->log_position < 0) { return false; }  /* NULL_POSITION */
    return compare_log(member->leadership_term_id, member->log_position,
                       candidate->leadership_term_id, candidate->log_position) <= 0;
}

bool aeron_cluster_member_is_unanimous_candidate(
    aeron_cluster_member_t *members,
    int member_count,
    const aeron_cluster_member_t *candidate,
    int32_t graceful_closed_leader_id)
{
    int possible_votes = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (members[i].id == graceful_closed_leader_id) { continue; }
        if (!aeron_cluster_member_will_vote_for(&members[i], candidate)) { return false; }
        possible_votes++;
    }
    return possible_votes >= aeron_cluster_member_quorum_threshold(member_count);
}

bool aeron_cluster_member_is_quorum_candidate_for(
    aeron_cluster_member_t *members,
    int member_count,
    const aeron_cluster_member_t *candidate)
{
    int possible_votes = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (aeron_cluster_member_will_vote_for(&members[i], candidate)) { possible_votes++; }
    }
    return possible_votes >= aeron_cluster_member_quorum_threshold(member_count);
}

bool aeron_cluster_member_is_quorum_leader(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id)
{
    int votes = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (members[i].candidate_term_id == candidate_term_id)
        {
            if (members[i].vote == 0) { return false; }  /* explicit NO */
            if (members[i].vote == 1) { votes++; }
        }
    }
    return votes >= aeron_cluster_member_quorum_threshold(member_count);
}

bool aeron_cluster_member_is_unanimous_leader(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t candidate_term_id,
    int32_t graceful_closed_leader_id)
{
    int votes = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (members[i].id == graceful_closed_leader_id) { continue; }
        if (members[i].candidate_term_id != candidate_term_id || members[i].vote != 1)
        {
            return false;
        }
        votes++;
    }
    return votes >= aeron_cluster_member_quorum_threshold(member_count);
}

bool aeron_cluster_member_has_quorum_at_position(
    aeron_cluster_member_t *members,
    int member_count,
    int64_t leadership_term_id,
    int64_t position,
    int64_t now_ns,
    int64_t heartbeat_timeout_ns)
{
    int count = 0;
    for (int i = 0; i < member_count; i++)
    {
        if (aeron_cluster_member_is_quorum_candidate(&members[i], now_ns, heartbeat_timeout_ns) &&
            members[i].leadership_term_id == leadership_term_id &&
            members[i].log_position >= position)
        {
            count++;
        }
    }
    return count >= aeron_cluster_member_quorum_threshold(member_count);
}
