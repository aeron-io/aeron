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
#include "uri/aeron_uri_string_builder.h"
#include "uri/aeron_uri.h"

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
 * Parse: "memberId,ep0,ep1,ep2,ep3,ep4[,ep5[,ep6]]"   (Java / canonical format)
 *    or: "memberId,ep0:ep1:ep2:ep3:ep4"               (old colon format)
 * ep indices: 0=ingress 1=consensus 2=log 3=catchup 4=archive
 *             5=archiveResponse (optional) 6=egressResponse (optional)
 * ----------------------------------------------------------------------- */
static int parse_single_member(aeron_cluster_member_t *m, const char *str, size_t len)
{
    memset(m, 0, sizeof(aeron_cluster_member_t));
    m->log_position           = -1;
    m->leadership_term_id     = -1;
    m->candidate_term_id      = -1;
    m->time_of_last_append_position_ns = 0;
    m->catchup_replay_session_id     = -1;
    m->catchup_replay_correlation_id = -1;

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

    /* Detect format: Java (comma-separated) vs old C (colon-separated).
     * Java format has another comma in the endpoints section. */
    char *endpoints[7] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL};
    int ep_count = 0;

    if (NULL != strchr(p, ','))
    {
        /* Java comma-separated format: "ep0,ep1,ep2,ep3,ep4[,ep5[,ep6]]" */
        char *tok = p;
        for (ep_count = 0; ep_count < 7 && tok != NULL; ep_count++)
        {
            char *next = strchr(tok, ',');
            if (NULL != next) { *next = '\0'; }
            endpoints[ep_count] = tok;
            tok = (NULL != next) ? next + 1 : NULL;
        }
    }
    else
    {
        /* Old colon-separated format:
         *   4 colons: "ep0:ep1:ep2:ep3:ep4"          (simple names)
         *   9 colons: "h:p:h:p:h:p:h:p:h:p"          (host:port pairs)
         */
        int total_colons = 0;
        for (const char *c = p; *c != '\0'; c++) { if (':' == *c) total_colons++; }
        int step = (total_colons <= 4) ? 1 : 2;

        endpoints[0] = p;
        int colon_count = 0;
        ep_count = 1;
        for (char *c = p; *c != '\0' && ep_count < 5; c++)
        {
            if (':' == *c)
            {
                colon_count++;
                if (0 == (colon_count % step))
                {
                    *c = '\0';
                    endpoints[ep_count++] = c + 1;
                }
            }
        }
    }

    char **targets[] = {
        &m->ingress_endpoint, &m->consensus_endpoint,
        &m->log_endpoint, &m->catchup_endpoint, &m->archive_endpoint,
        &m->archive_response_endpoint, &m->egress_response_endpoint
    };
    for (int i = 0; i < ep_count && i < 7; i++)
    {
        if (NULL != endpoints[i] && '\0' != endpoints[i][0] &&
            member_set_string(targets[i], endpoints[i], strlen(endpoints[i])) < 0)
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
        aeron_free(members[i].archive_response_endpoint);
        aeron_free(members[i].egress_response_endpoint);
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

/* -----------------------------------------------------------------------
 * Additional static utilities
 * ----------------------------------------------------------------------- */

int aeron_cluster_member_compare_log(
    const aeron_cluster_member_t *lhs,
    const aeron_cluster_member_t *rhs)
{
    return compare_log(lhs->leadership_term_id, lhs->log_position,
                       rhs->leadership_term_id, rhs->log_position);
}

int aeron_cluster_member_compare_log_terms(
    int64_t lhs_term_id, int64_t lhs_log_position,
    int64_t rhs_term_id, int64_t rhs_log_position)
{
    return compare_log(lhs_term_id, lhs_log_position, rhs_term_id, rhs_log_position);
}

void aeron_cluster_members_reset(aeron_cluster_member_t *members, int count)
{
    for (int i = 0; i < count; i++)
    {
        members[i].vote                          = -1;
        members[i].candidate_term_id             = -1;
        members[i].is_ballot_sent                = false;
        members[i].catchup_replay_session_id     = -1;
        members[i].catchup_replay_correlation_id = -1;
    }
}

void aeron_cluster_members_become_candidate(
    aeron_cluster_member_t *members, int count,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id == candidate_member_id)
        {
            members[i].vote              = 1;        /* YES */
            members[i].candidate_term_id = candidate_term_id;
            members[i].is_ballot_sent    = true;
        }
        else
        {
            members[i].vote              = -1;       /* null / unknown */
            members[i].candidate_term_id = -1;
            members[i].is_ballot_sent    = false;
        }
    }
}

bool aeron_cluster_members_has_active_quorum(
    const aeron_cluster_member_t *members,
    int count,
    int64_t now_ns,
    int64_t timeout_ns)
{
    int threshold = aeron_cluster_member_quorum_threshold(count);
    for (int i = 0; i < count; i++)
    {
        /* A member is "active" if it has sent an append-position within timeout */
        if ((now_ns - members[i].time_of_last_append_position_ns) < timeout_ns)
        {
            if (--threshold <= 0) { return true; }
        }
    }
    return false;
}

void aeron_cluster_members_set_is_leader(
    aeron_cluster_member_t *members, int count, int32_t leader_id)
{
    for (int i = 0; i < count; i++)
    {
        members[i].is_leader = (members[i].id == leader_id);
    }
}

bool aeron_cluster_member_are_same_endpoints(
    const aeron_cluster_member_t *lhs,
    const aeron_cluster_member_t *rhs)
{
    return (0 == strcmp(lhs->ingress_endpoint   ? lhs->ingress_endpoint   : "",
                        rhs->ingress_endpoint   ? rhs->ingress_endpoint   : "")) &&
           (0 == strcmp(lhs->consensus_endpoint ? lhs->consensus_endpoint : "",
                        rhs->consensus_endpoint ? rhs->consensus_endpoint : "")) &&
           (0 == strcmp(lhs->log_endpoint       ? lhs->log_endpoint       : "",
                        rhs->log_endpoint       ? rhs->log_endpoint       : "")) &&
           (0 == strcmp(lhs->catchup_endpoint   ? lhs->catchup_endpoint   : "",
                        rhs->catchup_endpoint   ? rhs->catchup_endpoint   : "")) &&
           (0 == strcmp(lhs->archive_endpoint   ? lhs->archive_endpoint   : "",
                        rhs->archive_endpoint   ? rhs->archive_endpoint   : "")) &&
           (0 == strcmp(lhs->archive_response_endpoint ? lhs->archive_response_endpoint : "",
                        rhs->archive_response_endpoint ? rhs->archive_response_endpoint : "")) &&
           (0 == strcmp(lhs->egress_response_endpoint  ? lhs->egress_response_endpoint  : "",
                        rhs->egress_response_endpoint  ? rhs->egress_response_endpoint  : ""));
}

int aeron_cluster_members_encode_as_string(
    const aeron_cluster_member_t *members, int count,
    char *buf, size_t buf_len)
{
    size_t pos = 0;
    for (int i = 0; i < count; i++)
    {
        const aeron_cluster_member_t *m = &members[i];
        int n = snprintf(buf + pos, buf_len - pos, "%s%d,%s,%s,%s,%s,%s",
            (i > 0) ? "|" : "",
            m->id,
            m->ingress_endpoint   ? m->ingress_endpoint   : "",
            m->consensus_endpoint ? m->consensus_endpoint : "",
            m->log_endpoint       ? m->log_endpoint       : "",
            m->catchup_endpoint   ? m->catchup_endpoint   : "",
            m->archive_endpoint   ? m->archive_endpoint   : "");
        if (n < 0 || (size_t)n >= buf_len - pos) { return -1; }
        pos += (size_t)n;

        /* Append optional fields only if present */
        if (NULL != m->archive_response_endpoint)
        {
            n = snprintf(buf + pos, buf_len - pos, ",%s", m->archive_response_endpoint);
            if (n < 0 || (size_t)n >= buf_len - pos) { return -1; }
            pos += (size_t)n;
        }
        if (NULL != m->egress_response_endpoint)
        {
            n = snprintf(buf + pos, buf_len - pos, ",%s", m->egress_response_endpoint);
            if (n < 0 || (size_t)n >= buf_len - pos) { return -1; }
            pos += (size_t)n;
        }
    }
    return (int)pos;
}

int aeron_cluster_members_ingress_endpoints(
    const aeron_cluster_member_t *members, int count,
    char *buf, size_t buf_len)
{
    size_t pos = 0;
    for (int i = 0; i < count; i++)
    {
        int n = snprintf(buf + pos, buf_len - pos, "%s%d=%s",
            (i > 0) ? "," : "",
            members[i].id,
            members[i].ingress_endpoint ? members[i].ingress_endpoint : "");
        if (n < 0 || (size_t)n >= buf_len - pos) { return -1; }
        pos += (size_t)n;
    }
    return (int)pos;
}

void aeron_cluster_members_collect_ids(
    const aeron_cluster_member_t *members, int count,
    int32_t *out_ids)
{
    for (int i = 0; i < count; i++)
    {
        out_ids[i] = members[i].id;
    }
}

int aeron_cluster_members_add_consensus_publications(
    aeron_cluster_member_t *members, int count,
    int32_t self_id,
    aeron_t *aeron,
    const char *channel_template,
    int32_t stream_id)
{
    for (int i = 0; i < count; i++)
    {
        aeron_cluster_member_t *m = &members[i];
        if (m->id == self_id || NULL == m->consensus_endpoint) { continue; }

        /* Build channel by substituting the peer's consensus endpoint */
        aeron_uri_string_builder_t builder;
        if (aeron_uri_string_builder_init_on_string(&builder, channel_template) < 0)
        {
            AERON_APPEND_ERR("failed to parse channel template for member %d", m->id);
            return -1;
        }
        if (aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY,
            m->consensus_endpoint) < 0)
        {
            aeron_uri_string_builder_close(&builder);
            AERON_APPEND_ERR("failed to set endpoint for member %d", m->id);
            return -1;
        }
        if (aeron_uri_string_builder_sprint(&builder, m->consensus_channel,
            sizeof(m->consensus_channel)) < 0)
        {
            aeron_uri_string_builder_close(&builder);
            AERON_APPEND_ERR("channel too long for member %d", m->id);
            return -1;
        }
        aeron_uri_string_builder_close(&builder);

        /* Spin-poll until the publication is connected */
        aeron_async_add_exclusive_publication_t *async = NULL;
        if (aeron_async_add_exclusive_publication(&async, aeron,
            m->consensus_channel, stream_id) < 0)
        {
            AERON_APPEND_ERR("failed to start adding publication for member %d", m->id);
            return -1;
        }
        int rc = 0;
        do { rc = aeron_async_add_exclusive_publication_poll(&m->publication, async); } while (0 == rc);
        if (rc < 0)
        {
            AERON_APPEND_ERR("failed to add publication for member %d", m->id);
            return -1;
        }
    }
    return 0;
}

void aeron_cluster_members_close_consensus_publications(
    aeron_cluster_member_t *members, int count)
{
    for (int i = 0; i < count; i++)
    {
        if (NULL != members[i].publication)
        {
            aeron_exclusive_publication_close(members[i].publication, NULL, NULL);
            members[i].publication = NULL;
        }
    }
}

aeron_cluster_member_t *aeron_cluster_members_determine_member(
    aeron_cluster_member_t *members, int count,
    int32_t self_id,
    const char *endpoints_str)
{
    aeron_cluster_member_t *found = (self_id >= 0)
        ? aeron_cluster_member_find_by_id(members, count, self_id)
        : NULL;

    if (NULL == members || 0 == count)
    {
        /* No cluster members configured — use endpoints_str to build a solo member */
        if (NULL == endpoints_str || '\0' == *endpoints_str)
        {
            AERON_SET_ERR(EINVAL, "%s", "no cluster members and no endpoints_str provided");
            return NULL;
        }
        /* Caller must manage this static — return first entry after re-parse */
        AERON_SET_ERR(EINVAL, "%s",
            "determineMember: no cluster members array; parse endpoints separately");
        return NULL;
    }

    if (NULL == found)
    {
        AERON_SET_ERR(EINVAL, "memberId=%d not found in clusterMembers", self_id);
        return NULL;
    }

    /* Optionally validate endpoints */
    if (NULL != endpoints_str && '\0' != *endpoints_str)
    {
        aeron_cluster_member_t *tmp = NULL;
        int tmp_count = 0;
        char entry[2048];
        /* Wrap as "id,endpoints" and parse */
        snprintf(entry, sizeof(entry), "%d,%s", self_id, endpoints_str);
        if (aeron_cluster_members_parse(entry, &tmp, &tmp_count) < 0 || 0 == tmp_count)
        {
            aeron_cluster_members_free(tmp, tmp_count);
            AERON_APPEND_ERR("failed to parse endpoints_str for member %d", self_id);
            return NULL;
        }
        bool same = aeron_cluster_member_are_same_endpoints(found, &tmp[0]);
        aeron_cluster_members_free(tmp, tmp_count);
        if (!same)
        {
            AERON_SET_ERR(EINVAL,
                "clusterMembers and endpoints differ for member %d", self_id);
            return NULL;
        }
    }

    return found;
}

/* -----------------------------------------------------------------------
 * parseEndpoints / validateMemberEndpoints / addConsensusPublication
 * ----------------------------------------------------------------------- */

int aeron_cluster_member_parse_endpoints(
    aeron_cluster_member_t *member,
    int32_t id,
    const char *endpoints_str)
{
    /* Build "id,endpoints_str" and reuse parse_single_member. */
    char buf[4096];
    int n = snprintf(buf, sizeof(buf), "%d,%s", id, endpoints_str);
    if (n < 0 || (size_t)n >= sizeof(buf))
    {
        AERON_SET_ERR(EINVAL, "endpoints string too long for member %d", id);
        return -1;
    }
    return parse_single_member(member, buf, (size_t)n);
}

int aeron_cluster_members_validate_endpoints(
    const aeron_cluster_member_t *member,
    const char *endpoints_str)
{
    /* Parse a temporary member from endpoints_str and compare. */
    aeron_cluster_member_t tmp;
    if (aeron_cluster_member_parse_endpoints(&tmp, member->id, endpoints_str) < 0)
    {
        return -1;
    }
    bool same = aeron_cluster_member_are_same_endpoints(member, &tmp);
    /* Free the temporary member's endpoint strings */
    aeron_free(tmp.ingress_endpoint);
    aeron_free(tmp.consensus_endpoint);
    aeron_free(tmp.log_endpoint);
    aeron_free(tmp.catchup_endpoint);
    aeron_free(tmp.archive_endpoint);
    aeron_free(tmp.archive_response_endpoint);
    aeron_free(tmp.egress_response_endpoint);
    if (!same)
    {
        AERON_SET_ERR(EINVAL,
            "clusterMembers and memberEndpoints differ for member %d", member->id);
        return -1;
    }
    return 0;
}

int aeron_cluster_member_try_add_publication(
    aeron_cluster_member_t *member,
    aeron_t *aeron,
    int32_t stream_id)
{
    /* Non-blocking async add: spin until ready. Non-fatal on registration failure. */
    aeron_async_add_exclusive_publication_t *async = NULL;
    if (aeron_async_add_exclusive_publication(&async, aeron,
        member->consensus_channel, stream_id) < 0)
    {
        /* Log warning but don't propagate — mirrors Java's tryAddPublication. */
        AERON_APPEND_ERR("failed to start adding consensus publication for member %d", member->id);
        aeron_err_clear();  /* non-fatal */
        return 0;
    }
    int rc = 0;
    do { rc = aeron_async_add_exclusive_publication_poll(&member->publication, async); } while (0 == rc);
    if (rc < 0)
    {
        AERON_APPEND_ERR("failed to add consensus publication for member %d", member->id);
        aeron_err_clear();  /* non-fatal */
    }
    return 0;
}

int aeron_cluster_member_add_consensus_publication(
    aeron_cluster_member_t *other_member,
    const aeron_cluster_member_t *this_member,
    aeron_t *aeron,
    const char *channel_template,
    int32_t stream_id)
{
    /* Build consensus channel if not already set. */
    if ('\0' == other_member->consensus_channel[0])
    {
        aeron_uri_string_builder_t builder;
        if (aeron_uri_string_builder_init_on_string(&builder, channel_template) < 0)
        {
            AERON_APPEND_ERR("failed to parse channel template for member %d", other_member->id);
            return -1;
        }
        /* Set remote endpoint to other_member's consensus endpoint */
        if (aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_ENDPOINT_KEY,
            other_member->consensus_endpoint) < 0)
        {
            aeron_uri_string_builder_close(&builder);
            AERON_APPEND_ERR("failed to set endpoint for member %d", other_member->id);
            return -1;
        }
        /* Set control endpoint to this_member's consensus endpoint (bind side) */
        if (NULL != this_member && NULL != this_member->consensus_endpoint)
        {
            aeron_uri_string_builder_put(&builder, AERON_UDP_CHANNEL_CONTROL_KEY,
                this_member->consensus_endpoint);
        }
        if (aeron_uri_string_builder_sprint(&builder, other_member->consensus_channel,
            sizeof(other_member->consensus_channel)) < 0)
        {
            aeron_uri_string_builder_close(&builder);
            AERON_APPEND_ERR("channel too long for member %d", other_member->id);
            return -1;
        }
        aeron_uri_string_builder_close(&builder);
    }
    return aeron_cluster_member_try_add_publication(other_member, aeron, stream_id);
}

/* -----------------------------------------------------------------------
 * Dynamic membership helpers
 * ----------------------------------------------------------------------- */

aeron_cluster_member_t *aeron_cluster_member_determine_dated_member(
    aeron_cluster_member_t *members,
    int count,
    int32_t exclude_id)
{
    aeron_cluster_member_t *dated = NULL;

    for (int i = 0; i < count; i++)
    {
        if (members[i].id == exclude_id)
        {
            continue;
        }

        if (NULL == dated ||
            members[i].time_of_last_append_position_ns < dated->time_of_last_append_position_ns)
        {
            dated = &members[i];
        }
    }

    return dated;
}

int32_t aeron_cluster_members_high_member_id(
    const aeron_cluster_member_t *members, int count)
{
    if (0 == count || NULL == members)
    {
        return -1;
    }

    int32_t high = members[0].id;
    for (int i = 1; i < count; i++)
    {
        if (members[i].id > high)
        {
            high = members[i].id;
        }
    }

    return high;
}

bool aeron_cluster_members_has_terminated_all(
    const aeron_cluster_member_t *members,
    int count,
    int32_t self_id)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id == self_id)
        {
            continue;
        }

        if (!members[i].has_terminate_notified)
        {
            return false;
        }
    }

    return true;
}

/* -----------------------------------------------------------------------
 * Single-member publication close
 * ----------------------------------------------------------------------- */
void aeron_cluster_member_close_publication(aeron_cluster_member_t *member)
{
    if (NULL != member->publication)
    {
        aeron_exclusive_publication_close(member->publication, NULL, NULL);
        member->publication = NULL;
    }
}
