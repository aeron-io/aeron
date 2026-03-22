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

#ifndef AERON_CLUSTER_MOCK_ELECTION_AGENT_H
#define AERON_CLUSTER_MOCK_ELECTION_AGENT_H

/**
 * Mock infrastructure for Election and ConsensusModuleAgent tests.
 *
 * Mirrors Java's Mockito-based test approach:
 *   mock(ConsensusPublisher.class)   → MockPublisherOps (records publisher calls)
 *   mock(ConsensusModuleAgent.class) → MockAgentOps     (records + stubs agent calls)
 *   verify(publisher).requestVote() → check pub.request_vote_count()
 *   when(agent.appendPosition()).thenReturn(X) → agent_ops.append_position = X
 */

#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <gtest/gtest.h>

extern "C"
{
#include "aeron_cluster_election.h"
#include "aeron_cluster_member.h"
#include "aeron_consensus_module_configuration.h"
}

/* -----------------------------------------------------------------------
 * MockPublisherOps — records every publisher call
 * ----------------------------------------------------------------------- */
struct PublisherCall
{
    std::string  type;
    int32_t      to_member_id         = -1;
    int64_t      log_leadership_term_id = -1;
    int64_t      log_position         = -1;
    int64_t      candidate_term_id    = -1;
    int64_t      leadership_term_id   = -1;
    int32_t      candidate_member_id  = -1;
    int32_t      follower_member_id   = -1;
    int32_t      leader_member_id     = -1;
    bool         vote_value           = false;
    bool         is_startup           = false;
};

struct MockPublisherOps
{
    std::vector<PublisherCall> calls;

    int count(const std::string &type) const
    {
        int n = 0;
        for (auto &c : calls) { if (c.type == type) n++; }
        return n;
    }

    int canvass_count()        const { return count("canvass"); }
    int request_vote_count()   const { return count("request_vote"); }
    int vote_count()           const { return count("vote"); }
    int new_leadership_count() const { return count("new_leadership_term"); }
    int append_pos_count()     const { return count("append_position"); }
    int commit_pos_count()     const { return count("commit_position"); }

    const PublisherCall *last(const std::string &type) const
    {
        for (int i = (int)calls.size()-1; i >= 0; i--)
            if (calls[i].type == type) return &calls[i];
        return nullptr;
    }

    bool sent_to(const std::string &type, int32_t member_id) const
    {
        for (auto &c : calls)
            if (c.type == type && c.to_member_id == member_id) return true;
        return false;
    }

    void reset() { calls.clear(); }
};

static bool mock_canvass(void *cd, aeron_cluster_member_t *m,
    int64_t log_term_id, int64_t log_pos,
    int64_t leadership_term_id, int32_t follower_id, int32_t)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="canvass"; c.to_member_id=m->id;
    c.log_leadership_term_id=log_term_id; c.log_position=log_pos;
    c.leadership_term_id=leadership_term_id; c.follower_member_id=follower_id;
    p->calls.push_back(c); return true;
}
static bool mock_request_vote(void *cd, aeron_cluster_member_t *m,
    int64_t log_term_id, int64_t log_pos, int64_t cand_term_id, int32_t cand_id)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="request_vote"; c.to_member_id=m->id;
    c.log_leadership_term_id=log_term_id; c.log_position=log_pos;
    c.candidate_term_id=cand_term_id; c.candidate_member_id=cand_id;
    p->calls.push_back(c); return true;
}
static bool mock_vote_fn(void *cd, aeron_cluster_member_t *m,
    int64_t cand_term, int64_t log_term, int64_t log_pos,
    int32_t cand_id, int32_t follower_id, bool vote)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="vote"; c.to_member_id=m->id;
    c.candidate_term_id=cand_term; c.log_leadership_term_id=log_term;
    c.log_position=log_pos; c.candidate_member_id=cand_id;
    c.follower_member_id=follower_id; c.vote_value=vote;
    p->calls.push_back(c); return true;
}
static bool mock_new_leadership_term(void *cd, aeron_cluster_member_t *m,
    int64_t, int64_t next_term, int64_t, int64_t,
    int64_t leadership_term_id, int64_t, int64_t log_pos,
    int64_t, int64_t, int32_t leader_id, int32_t, int32_t, bool startup)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="new_leadership_term"; c.to_member_id=m->id;
    c.candidate_term_id=next_term; c.leadership_term_id=leadership_term_id;
    c.log_position=log_pos; c.leader_member_id=leader_id; c.is_startup=startup;
    p->calls.push_back(c); return true;
}
static bool mock_append_pos(void *cd, aeron_cluster_member_t *m,
    int64_t term_id, int64_t log_pos, int32_t follower_id, int8_t)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="append_position"; c.to_member_id=m->id;
    c.leadership_term_id=term_id; c.log_position=log_pos; c.follower_member_id=follower_id;
    p->calls.push_back(c); return true;
}
static bool mock_commit_pos(void *cd, aeron_cluster_member_t *m,
    int64_t term_id, int64_t log_pos, int32_t leader_id)
{
    auto *p = static_cast<MockPublisherOps*>(cd);
    PublisherCall c; c.type="commit_position"; c.to_member_id=m->id;
    c.leadership_term_id=term_id; c.log_position=log_pos; c.leader_member_id=leader_id;
    p->calls.push_back(c); return true;
}

static void init_mock_pub_ops(aeron_cluster_election_publisher_ops_t *ops,
                               MockPublisherOps *recorder)
{
    ops->clientd          = recorder;
    ops->canvass_position = mock_canvass;
    ops->request_vote     = mock_request_vote;
    ops->vote             = mock_vote_fn;
    ops->new_leadership_term = mock_new_leadership_term;
    ops->append_position  = mock_append_pos;
    ops->commit_position  = mock_commit_pos;
}

/* -----------------------------------------------------------------------
 * MockAgentOps — stubs + records agent callbacks
 * ----------------------------------------------------------------------- */
struct AgentCall
{
    std::string type;
    aeron_cluster_election_state_t state = AERON_ELECTION_INIT;
    aeron_cluster_member_t *leader = nullptr;
    int64_t commit_position = -1;
    int64_t new_term_id = -1;
};

struct MockAgentOps
{
    /* Stub return values (configure before test) */
    int32_t protocol_version  = 0;
    int32_t app_version       = 0;
    int64_t append_position   = 0;
    int64_t log_recording_id  = -1;

    /* Recorded calls */
    std::vector<AgentCall> calls;
    std::vector<aeron_cluster_election_state_t> state_changes;
    int election_complete_count   = 0;
    int begin_new_term_count      = 0;
    int follower_new_term_count   = 0;
    int replay_new_term_count     = 0;
    std::vector<int64_t> notified_commit_positions;
    aeron_cluster_member_t *last_elected_leader = nullptr;
    int64_t last_new_term_id = -1;

    void reset()
    {
        calls.clear();
        state_changes.clear();
        election_complete_count = follower_new_term_count = begin_new_term_count = 0;
        replay_new_term_count = 0;
        notified_commit_positions.clear();
        last_elected_leader = nullptr;
        last_new_term_id = -1;
    }
};

static int32_t mock_get_protocol_version(void *cd)
{ return static_cast<MockAgentOps*>(cd)->protocol_version; }
static int32_t mock_get_app_version(void *cd)
{ return static_cast<MockAgentOps*>(cd)->app_version; }
static int64_t mock_get_append_position(void *cd)
{ return static_cast<MockAgentOps*>(cd)->append_position; }
static int64_t mock_get_log_recording_id(void *cd)
{ return static_cast<MockAgentOps*>(cd)->log_recording_id; }

static void mock_on_state_change(void *cd, aeron_cluster_election_state_t s, int64_t)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->state_changes.push_back(s);
    AgentCall c; c.type="state_change"; c.state=s; a->calls.push_back(c);
}
static void mock_on_election_complete(void *cd, aeron_cluster_member_t *leader, int64_t)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->election_complete_count++;
    a->last_elected_leader = leader;
    AgentCall c; c.type="election_complete"; c.leader=leader; a->calls.push_back(c);
}
static void mock_begin_new_leadership_term(void *cd,
    int64_t, int64_t new_term, int64_t, int64_t, bool)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->begin_new_term_count++;
    a->last_new_term_id = new_term;
    AgentCall c; c.type="begin_new_term"; c.new_term_id=new_term; a->calls.push_back(c);
}
static void mock_on_follower_new_leadership_term(void *cd,
    int64_t, int64_t next_term, int64_t, int64_t,
    int64_t, int64_t, int64_t, int64_t, int64_t,
    int32_t, int32_t, int32_t, bool)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->follower_new_term_count++;
    a->last_new_term_id = next_term;
    AgentCall c; c.type="follower_new_term"; a->calls.push_back(c);
}
static void mock_on_replay_new_leadership_term(void *cd,
    int64_t, int64_t, int64_t, int64_t, int32_t, int32_t, int32_t)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->replay_new_term_count++;
    AgentCall c; c.type="replay_new_term"; a->calls.push_back(c);
}
static void mock_notify_commit_position(void *cd, int64_t pos)
{
    auto *a = static_cast<MockAgentOps*>(cd);
    a->notified_commit_positions.push_back(pos);
    AgentCall c; c.type="commit"; c.commit_position=pos; a->calls.push_back(c);
}

static void init_mock_agent_ops(aeron_cluster_election_agent_ops_t *ops,
                                 MockAgentOps *recorder)
{
    ops->clientd                         = recorder;
    ops->get_protocol_version            = mock_get_protocol_version;
    ops->get_app_version                 = mock_get_app_version;
    ops->get_append_position             = mock_get_append_position;
    ops->get_log_recording_id            = mock_get_log_recording_id;
    ops->on_state_change                 = mock_on_state_change;
    ops->on_election_complete            = mock_on_election_complete;
    ops->begin_new_leadership_term       = mock_begin_new_leadership_term;
    ops->on_follower_new_leadership_term = mock_on_follower_new_leadership_term;
    ops->on_replay_new_leadership_term   = mock_on_replay_new_leadership_term;
    ops->notify_commit_position          = mock_notify_commit_position;
}

/* -----------------------------------------------------------------------
 * ElectionTestFixture — combines mock pub + mock agent + real election
 * ----------------------------------------------------------------------- */
class ElectionTestFixture
{
public:
    static constexpr int64_t NULL_VALUE    = -1LL;
    static constexpr int64_t NULL_POSITION = -1LL;
    static constexpr int64_t RECORDING_ID  = 600LL;
    static constexpr int32_t LOG_SESSION_ID = 777;

    MockPublisherOps pub;
    MockAgentOps     agent;

    aeron_cluster_member_t *members     = nullptr;
    int                     member_count = 0;
    aeron_cluster_election_t *election  = nullptr;

    ~ElectionTestFixture() { teardown(); }

    void teardown()
    {
        if (election) { aeron_cluster_election_close(election); election = nullptr; }
        if (members)  { aeron_cluster_members_free(members, member_count); members = nullptr; }
    }

    /** Parse topology and create a fresh election. */
    void build(const char *topology, int32_t self_id,
               int64_t log_leadership_term_id = NULL_VALUE,
               int64_t log_position           = 0,
               int64_t leadership_term_id     = NULL_VALUE,
               int64_t leader_recording_id    = -1,
               int64_t startup_canvass_ns  = 5000000000LL,
               int64_t election_timeout_ns = 1000000000LL,
               int64_t status_interval_ns  = 20000000LL,
               int64_t heartbeat_timeout_ns = 10000000000LL,
               bool is_node_startup = true)
    {
        teardown();
        pub.reset(); agent.reset();

        ASSERT_EQ(0, aeron_cluster_members_parse(topology, &members, &member_count));
        aeron_cluster_member_t *self = aeron_cluster_member_find_by_id(
            members, member_count, self_id);
        ASSERT_NE(nullptr, self);

        /* Use NULL for real agent — we override ops below */
        ASSERT_EQ(0, aeron_cluster_election_create(
            &election, nullptr /* agent — overridden via ops */,
            self, members, member_count,
            log_leadership_term_id, log_position, leadership_term_id,
            leader_recording_id,
            startup_canvass_ns, election_timeout_ns, status_interval_ns,
            heartbeat_timeout_ns, is_node_startup));

        init_mock_pub_ops(&election->pub_ops, &pub);
        init_mock_agent_ops(&election->agent_ops, &agent);
    }

    /** Helpers for peer messages */
    void on_canvass(int32_t from, int64_t log_term, int64_t log_pos,
                    int64_t leadership, int32_t proto = 0)
    {
        aeron_cluster_election_on_canvass_position(election,
            log_term, log_pos, leadership, from, proto);
    }

    void on_vote(int32_t from, int64_t cand_term, int64_t log_term,
                 int64_t log_pos, int32_t cand_id, bool vote)
    {
        aeron_cluster_election_on_vote(election,
            cand_term, log_term, log_pos, cand_id, from, vote);
    }

    void on_append_pos(int32_t from, int64_t term, int64_t pos)
    {
        /* Update member state so quorum check passes */
        auto *m = aeron_cluster_member_find_by_id(members, member_count, from);
        if (m) { m->leadership_term_id = term; m->log_position = pos; }
        aeron_cluster_election_on_append_position(election, term, pos, from, 0);
    }

    void on_new_leadership_term(int64_t log_term, int64_t next_term,
        int64_t next_base, int64_t next_log_pos,
        int64_t term, int64_t base, int64_t log_pos,
        int64_t rec_id, int64_t ts, int32_t leader_id,
        int32_t session_id, int32_t app_ver, bool startup)
    {
        aeron_cluster_election_on_new_leadership_term(election,
            log_term, next_term, next_base, next_log_pos,
            term, base, log_pos, rec_id, ts, leader_id,
            session_id, app_ver, startup);
    }

    void on_commit_position(int64_t term, int64_t pos, int32_t leader_id)
    {
        aeron_cluster_election_on_commit_position(election, term, pos, leader_id);
    }

    int do_work(int64_t now_ns)
    {
        return aeron_cluster_election_do_work(election, now_ns);
    }

    aeron_cluster_election_state_t state() const { return election->state; }

    bool state_reached(aeron_cluster_election_state_t s) const
    {
        for (auto &c : agent.state_changes)
            if (c == s) return true;
        return false;
    }
};

#endif /* AERON_CLUSTER_MOCK_ELECTION_AGENT_H */
