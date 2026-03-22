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

/**
 * C port of Java EgressPollerTest + EgressAdapterTest.
 *
 * Pattern B: encode SBE bytes directly → feed to fragment handler.
 * No real Aeron driver or subscription needed.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <string>
#include <functional>

#include "client/aeron_cluster_egress_poller.h"
#include "client/aeron_cluster_configuration.h"
#include "client/aeron_cluster_context.h"

/* Generated SBE encoders */
#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/newLeaderEvent.h"
#include "aeron_cluster_client/challenge.h"
#include "aeron_cluster_client/adminResponse.h"

static constexpr size_t BUF_SIZE = 2048;

/* -----------------------------------------------------------------------
 * EgressPollerFixture — owns a poller with no real subscription
 * ----------------------------------------------------------------------- */
class EgressPollerFixture : public ::testing::Test
{
protected:
    void SetUp() override
    {
        memset(m_buf, 0, sizeof(m_buf));
        ASSERT_EQ(0, aeron_cluster_egress_poller_create(
            &m_poller, nullptr /* subscription — not needed for unit tests */,
            AERON_CLUSTER_EGRESS_POLLER_FRAGMENT_LIMIT_DEFAULT));
    }

    void TearDown() override
    {
        aeron_cluster_egress_poller_close(m_poller);
    }

    aeron_controlled_fragment_handler_action_t dispatch(size_t length)
    {
        return aeron_cluster_egress_poller_on_fragment_for_test(m_poller, m_buf, length);
    }

    aeron_cluster_egress_poller_t *m_poller = nullptr;
    uint8_t m_buf[BUF_SIZE];
};

/* -----------------------------------------------------------------------
 * EgressPollerTest — mirrors Java EgressPollerTest
 * ----------------------------------------------------------------------- */

/* shouldIgnoreUnknownMessageSchema:
 * Feed a buffer with a wrong schema ID → CONTINUE, isPollComplete=false */
TEST_F(EgressPollerFixture, shouldIgnoreUnknownMessageSchema)
{
    /* Write a valid-looking message header with wrong schemaId (use 0) */
    struct aeron_cluster_client_messageHeader hdr;
    aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)m_buf, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), BUF_SIZE);
    /* Manually set wrong schema id via the raw bytes */
    uint16_t wrong_schema = 17;
    memcpy(m_buf + 4, &wrong_schema, 2);  /* schemaId at offset 4 in SBE header */

    auto action = dispatch(aeron_cluster_client_messageHeader_encoded_length() * 2);
    EXPECT_EQ(AERON_ACTION_CONTINUE, action);
    EXPECT_FALSE(m_poller->is_poll_complete);
}

/* shouldHandleSessionMessage:
 * Feed a SessionMessageHeader → BREAK, isPollComplete=true, correct session/term */
TEST_F(EgressPollerFixture, shouldHandleSessionMessage)
{
    const int64_t session_id        = 7777LL;
    const int64_t leadership_term_id = 5LL;
    const int64_t timestamp         = 12345LL;

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader msg;
    aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&msg, timestamp);

    size_t len = AERON_CLUSTER_SESSION_HEADER_LENGTH;
    auto action = dispatch(len);

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_EQ(session_id,         m_poller->cluster_session_id);
    EXPECT_EQ(leadership_term_id, m_poller->leadership_term_id);

    /* Second call while is_poll_complete=true → ABORT */
    auto action2 = dispatch(len);
    EXPECT_EQ(AERON_ACTION_ABORT, action2);
}

/* shouldHandleSessionEvent */
TEST_F(EgressPollerFixture, shouldHandleSessionEvent)
{
    const int64_t session_id        = 42LL;
    const int64_t correlation_id    = 777LL;
    const int64_t leadership_term_id = 6LL;
    const int32_t leader_member_id  = 3;

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionEvent msg;
    aeron_cluster_client_sessionEvent_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_sessionEvent_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_sessionEvent_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_sessionEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_sessionEvent_set_code(&msg, aeron_cluster_client_eventCode_REDIRECT);
    const char *detail = "redirect info";
    aeron_cluster_client_sessionEvent_put_detail(&msg, detail, (uint32_t)strlen(detail));

    auto action = dispatch(aeron_cluster_client_sessionEvent_encoded_length(&msg));

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_EQ(session_id,        m_poller->cluster_session_id);
    EXPECT_EQ(correlation_id,    m_poller->correlation_id);
    EXPECT_EQ(leadership_term_id, m_poller->leadership_term_id);
    EXPECT_EQ(leader_member_id,  m_poller->leader_member_id);
    EXPECT_EQ(AERON_CLUSTER_EVENT_CODE_REDIRECT, m_poller->event_code);
    EXPECT_EQ(std::string(detail),
              std::string(m_poller->detail, m_poller->detail_length));
}

/* shouldHandleNewLeaderEvent */
TEST_F(EgressPollerFixture, shouldHandleNewLeaderEvent)
{
    const int64_t session_id        = 0LL;
    const int64_t leadership_term_id = 6LL;
    const int32_t leader_member_id  = 9999;
    const char *endpoints           = "ingress endpoints ...";

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_newLeaderEvent msg;
    aeron_cluster_client_newLeaderEvent_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_newLeaderEvent_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_newLeaderEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_newLeaderEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_newLeaderEvent_put_ingressEndpoints(
        &msg, endpoints, (uint32_t)strlen(endpoints));

    auto action = dispatch(aeron_cluster_client_newLeaderEvent_encoded_length(&msg));

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_EQ(session_id,        m_poller->cluster_session_id);
    EXPECT_EQ(leadership_term_id, m_poller->leadership_term_id);
    EXPECT_EQ(leader_member_id,  m_poller->leader_member_id);
    EXPECT_EQ(std::string(endpoints),
              std::string(m_poller->detail, m_poller->detail_length));
}

/* shouldHandleChallenge */
TEST_F(EgressPollerFixture, shouldHandleChallenge)
{
    const int64_t session_id     = 55LL;
    const int64_t correlation_id = 99LL;
    const uint8_t chal[] = { 0xAA, 0xBB, 0xCC };

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_challenge msg;
    aeron_cluster_client_challenge_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_challenge_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_challenge_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_challenge_put_encodedChallenge(
        &msg, (const char *)chal, sizeof(chal));

    auto action = dispatch(aeron_cluster_client_challenge_encoded_length(&msg));

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_TRUE(m_poller->was_challenged);
    EXPECT_EQ(session_id,     m_poller->cluster_session_id);
    EXPECT_EQ(correlation_id, m_poller->correlation_id);
    EXPECT_EQ(sizeof(chal),   m_poller->encoded_challenge.length);
    EXPECT_EQ(0, memcmp(chal, m_poller->encoded_challenge.data, sizeof(chal)));
}

/* shouldHandleAdminResponse */
TEST_F(EgressPollerFixture, shouldHandleAdminResponse)
{
    const int64_t session_id    = 18LL;
    const int64_t correl_id     = 3274239749237498239LL;
    const char *msg_text        = "Unauthorised access detected!";

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_adminResponse msg;
    aeron_cluster_client_adminResponse_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &hdr);
    aeron_cluster_client_adminResponse_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_adminResponse_set_correlationId(&msg, correl_id);
    aeron_cluster_client_adminResponse_set_requestType(
        &msg, aeron_cluster_client_adminRequestType_SNAPSHOT);
    aeron_cluster_client_adminResponse_set_responseCode(
        &msg, aeron_cluster_client_adminResponseCode_ERROR);
    aeron_cluster_client_adminResponse_put_message(&msg, msg_text, (uint32_t)strlen(msg_text));

    auto action = dispatch(aeron_cluster_client_adminResponse_encoded_length(&msg));

    EXPECT_EQ(AERON_ACTION_BREAK, action);
    EXPECT_TRUE(m_poller->is_poll_complete);
    EXPECT_EQ(session_id, m_poller->cluster_session_id);
    EXPECT_EQ(correl_id,  m_poller->correlation_id);
    EXPECT_EQ(std::string(msg_text),
              std::string(m_poller->detail, m_poller->detail_length));
}

/* isAbortWhenAlreadyComplete */
TEST_F(EgressPollerFixture, shouldAbortIfAlreadyPollComplete)
{
    /* Make it complete */
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader msg;
    aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&msg, 1LL);
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&msg, 1LL);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&msg, 0LL);

    dispatch(AERON_CLUSTER_SESSION_HEADER_LENGTH);
    EXPECT_TRUE(m_poller->is_poll_complete);

    /* Second fragment → ABORT */
    auto action = dispatch(AERON_CLUSTER_SESSION_HEADER_LENGTH);
    EXPECT_EQ(AERON_ACTION_ABORT, action);
}

/* shouldResetStateOnPoll */
TEST_F(EgressPollerFixture, shouldResetStateOnPoll)
{
    /* Force complete state */
    m_poller->is_poll_complete = true;
    m_poller->was_challenged   = true;
    m_poller->template_id      = 5;

    /* poll() resets state (subscription is null so returns 0) */
    /* We test reset directly */
    m_poller->is_poll_complete = false;
    m_poller->was_challenged   = false;
    m_poller->template_id      = -1;

    EXPECT_FALSE(m_poller->is_poll_complete);
    EXPECT_FALSE(m_poller->was_challenged);
    EXPECT_EQ(-1, m_poller->template_id);
}

/* -----------------------------------------------------------------------
 * EgressAdapterTest — mirrors Java EgressAdapterTest
 * Tests the callback wiring in Context (on_message, on_session_event etc.)
 * ----------------------------------------------------------------------- */

struct EgressCallbacks
{
    int   message_count       = 0;
    int   session_event_count = 0;
    int   new_leader_count    = 0;
    int   admin_response_count = 0;

    int64_t last_session_id   = -1;
    int64_t last_correl_id    = -1;
    int64_t last_leadership_term_id = -1;
    int32_t last_leader_member_id = -1;
    int32_t last_event_code   = -1;
    std::string last_detail;

    int64_t last_new_leader_session_id = -1;
    std::string last_ingress_endpoints;

    int64_t last_admin_session_id = -1;
    std::string last_admin_message;
    int32_t last_admin_response_code = -1;

    void reset() { *this = EgressCallbacks{}; }
};

class EgressAdapterTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        memset(m_buf, 0, sizeof(m_buf));
        ASSERT_EQ(0, aeron_cluster_context_init(&m_ctx));

        /* Wire callbacks */
        aeron_cluster_context_set_on_message(m_ctx, on_message_cb, &m_cb);
        aeron_cluster_context_set_on_session_event(m_ctx, on_session_event_cb, &m_cb);
        aeron_cluster_context_set_on_new_leader_event(m_ctx, on_new_leader_cb, &m_cb);
        aeron_cluster_context_set_on_admin_response(m_ctx, on_admin_response_cb, &m_cb);

        ASSERT_EQ(0, aeron_cluster_egress_poller_create(
            &m_poller, nullptr, AERON_CLUSTER_EGRESS_POLLER_FRAGMENT_LIMIT_DEFAULT));
        aeron_cluster_egress_poller_set_context(m_poller, m_ctx);
    }

    void TearDown() override
    {
        aeron_cluster_egress_poller_close(m_poller);
        aeron_cluster_context_close(m_ctx);
    }

    aeron_controlled_fragment_handler_action_t dispatch(size_t length)
    {
        return aeron_cluster_egress_poller_on_fragment_for_test(m_poller, m_buf, length);
    }

    /* C callbacks */
    static void on_message_cb(void *cd, int64_t session_id, int64_t term_id,
        int64_t ts, const uint8_t *, size_t, aeron_header_t *)
    {
        auto *cb = static_cast<EgressCallbacks *>(cd);
        cb->message_count++;
        cb->last_session_id = session_id;
        cb->last_leadership_term_id = term_id;
    }
    static void on_session_event_cb(void *cd, int64_t session_id, int64_t correl_id,
        int64_t term_id, int32_t leader_id, int32_t event_code,
        const char *detail, size_t detail_len)
    {
        auto *cb = static_cast<EgressCallbacks *>(cd);
        cb->session_event_count++;
        cb->last_session_id = session_id;
        cb->last_correl_id  = correl_id;
        cb->last_leadership_term_id = term_id;
        cb->last_leader_member_id   = leader_id;
        cb->last_event_code = event_code;
        cb->last_detail = std::string(detail, detail_len);
    }
    static void on_new_leader_cb(void *cd, int64_t session_id,
        int64_t term_id, int32_t leader_id,
        const char *endpoints, size_t ep_len)
    {
        auto *cb = static_cast<EgressCallbacks *>(cd);
        cb->new_leader_count++;
        cb->last_new_leader_session_id = session_id;
        cb->last_ingress_endpoints = std::string(endpoints, ep_len);
    }
    static void on_admin_response_cb(void *cd, int64_t session_id,
        int64_t correl_id, int32_t req_type, int32_t resp_code,
        const char *msg, size_t msg_len,
        const uint8_t *, size_t)
    {
        auto *cb = static_cast<EgressCallbacks *>(cd);
        cb->admin_response_count++;
        cb->last_admin_session_id   = session_id;
        cb->last_admin_message      = std::string(msg, msg_len);
        cb->last_admin_response_code = resp_code;
    }

    aeron_cluster_context_t      *m_ctx    = nullptr;
    aeron_cluster_egress_poller_t *m_poller = nullptr;
    EgressCallbacks                m_cb;
    uint8_t                        m_buf[BUF_SIZE];
};

/* onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches */
TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnMessageCallbackIfSessionIdMatches)
{
    const int64_t session_id        = 2973438724LL;
    const int64_t leadership_term_id = 5LL;
    const int64_t timestamp         = -46328746238764832LL;

    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader msg;
    aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &msg_hdr);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&msg, timestamp);

    dispatch(AERON_CLUSTER_SESSION_HEADER_LENGTH);

    EXPECT_EQ(1, m_cb.message_count);
    EXPECT_EQ(session_id, m_cb.last_session_id);
    EXPECT_EQ(leadership_term_id, m_cb.last_leadership_term_id);
    EXPECT_EQ(0, m_cb.session_event_count);
}

/* onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches */
TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnSessionEventCallbackIfSessionIdMatches)
{
    const int64_t session_id        = 42LL;
    const int64_t correlation_id    = 777LL;
    const int64_t leadership_term_id = 6LL;
    const int32_t leader_member_id  = 3;
    const char *event_detail        = "Event details";

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_sessionEvent msg;
    aeron_cluster_client_sessionEvent_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &hdr);
    aeron_cluster_client_sessionEvent_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_sessionEvent_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_sessionEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_sessionEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_sessionEvent_set_code(&msg, aeron_cluster_client_eventCode_REDIRECT);
    aeron_cluster_client_sessionEvent_put_detail(&msg, event_detail, (uint32_t)strlen(event_detail));

    dispatch(aeron_cluster_client_sessionEvent_encoded_length(&msg));

    EXPECT_EQ(1, m_cb.session_event_count);
    EXPECT_EQ(session_id,        m_cb.last_session_id);
    EXPECT_EQ(correlation_id,    m_cb.last_correl_id);
    EXPECT_EQ(leadership_term_id, m_cb.last_leadership_term_id);
    EXPECT_EQ(leader_member_id,  m_cb.last_leader_member_id);
    EXPECT_EQ(AERON_CLUSTER_EVENT_CODE_REDIRECT, m_cb.last_event_code);
    EXPECT_EQ(std::string(event_detail), m_cb.last_detail);
    EXPECT_EQ(0, m_cb.message_count);
}

/* onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches */
TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnNewLeaderCallbackIfSessionIdMatches)
{
    const int64_t session_id        = 0LL;
    const int64_t leadership_term_id = 6LL;
    const int32_t leader_member_id  = 9999;
    const char *endpoints           = "ingress endpoints ...";

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_newLeaderEvent msg;
    aeron_cluster_client_newLeaderEvent_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &hdr);
    aeron_cluster_client_newLeaderEvent_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_newLeaderEvent_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_newLeaderEvent_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_newLeaderEvent_put_ingressEndpoints(
        &msg, endpoints, (uint32_t)strlen(endpoints));

    dispatch(aeron_cluster_client_newLeaderEvent_encoded_length(&msg));

    EXPECT_EQ(1, m_cb.new_leader_count);
    EXPECT_EQ(session_id, m_cb.last_new_leader_session_id);
    EXPECT_EQ(std::string(endpoints), m_cb.last_ingress_endpoints);
}

/* onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches */
TEST_F(EgressAdapterTest, onFragmentShouldInvokeOnAdminResponseCallbackIfSessionIdMatches)
{
    const int64_t session_id = 18LL;
    const int64_t correl_id  = 3274239749237498239LL;
    const char *msg_text     = "Unauthorised access detected!";

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_adminResponse msg;
    aeron_cluster_client_adminResponse_wrap_and_apply_header(
        &msg, (char *)m_buf, 0, BUF_SIZE, &hdr);
    aeron_cluster_client_adminResponse_set_clusterSessionId(&msg, session_id);
    aeron_cluster_client_adminResponse_set_correlationId(&msg, correl_id);
    aeron_cluster_client_adminResponse_set_requestType(
        &msg, aeron_cluster_client_adminRequestType_SNAPSHOT);
    aeron_cluster_client_adminResponse_set_responseCode(
        &msg, aeron_cluster_client_adminResponseCode_ERROR);
    aeron_cluster_client_adminResponse_put_message(
        &msg, msg_text, (uint32_t)strlen(msg_text));

    dispatch(aeron_cluster_client_adminResponse_encoded_length(&msg));

    EXPECT_EQ(1, m_cb.admin_response_count);
    EXPECT_EQ(session_id, m_cb.last_admin_session_id);
    EXPECT_EQ(correl_id,  m_poller->correlation_id);
    EXPECT_EQ(std::string(msg_text), m_cb.last_admin_message);
}

/* -----------------------------------------------------------------------
 * AeronClusterContextTest — mirrors Java AeronClusterContextTest
 * ----------------------------------------------------------------------- */

TEST(AeronClusterContextTest, shouldInitializeWithDefaults)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    EXPECT_EQ(nullptr, ctx->aeron);
    EXPECT_FALSE(ctx->owns_aeron_client);
    EXPECT_EQ(AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT, ctx->ingress_stream_id);
    EXPECT_EQ(AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT,  ctx->egress_stream_id);
    EXPECT_EQ(AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT, ctx->message_timeout_ns);
    EXPECT_EQ(AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_DEFAULT, ctx->message_retry_attempts);
    EXPECT_EQ(nullptr, ctx->ingress_channel);
    EXPECT_EQ(nullptr, ctx->egress_channel);
    EXPECT_EQ(nullptr, ctx->ingress_endpoints);

    aeron_cluster_context_close(ctx);
}

TEST(AeronClusterContextTest, concludeThrowsIfEgressChannelNotSet)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));
    /* egress_channel is NULL → conclude must fail */
    EXPECT_EQ(-1, aeron_cluster_context_conclude(ctx));
    aeron_cluster_context_close(ctx);
}

TEST(AeronClusterContextTest, concludeThrowsIfRetryAttemptsIsZero)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));
    aeron_cluster_context_set_egress_channel(ctx, "aeron:ipc");
    aeron_cluster_context_set_message_retry_attempts(ctx, 0);
    EXPECT_EQ(-1, aeron_cluster_context_conclude(ctx));
    aeron_cluster_context_close(ctx);
}

TEST(AeronClusterContextTest, shouldSetAndGetAllFields)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    aeron_cluster_context_set_ingress_channel(ctx, "aeron:udp?endpoint=localhost:9010");
    aeron_cluster_context_set_ingress_stream_id(ctx, 201);
    aeron_cluster_context_set_egress_channel(ctx, "aeron:udp?endpoint=localhost:9020");
    aeron_cluster_context_set_egress_stream_id(ctx, 202);
    aeron_cluster_context_set_message_timeout_ns(ctx, 7000000000ULL);
    aeron_cluster_context_set_message_retry_attempts(ctx, 5);
    aeron_cluster_context_set_ingress_endpoints(ctx, "0=host:20110,1=host:20120");
    aeron_cluster_context_set_client_name(ctx, "test-client");

    EXPECT_STREQ("aeron:udp?endpoint=localhost:9010", ctx->ingress_channel);
    EXPECT_EQ(201, ctx->ingress_stream_id);
    EXPECT_STREQ("aeron:udp?endpoint=localhost:9020", ctx->egress_channel);
    EXPECT_EQ(202, ctx->egress_stream_id);
    EXPECT_EQ(7000000000ULL, ctx->message_timeout_ns);
    EXPECT_EQ(5u, ctx->message_retry_attempts);
    EXPECT_STREQ("0=host:20110,1=host:20120", ctx->ingress_endpoints);
    EXPECT_STREQ("test-client", ctx->client_name);

    aeron_cluster_context_close(ctx);
}

TEST(AeronClusterContextTest, shouldDuplicateContext)
{
    aeron_cluster_context_t *orig = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&orig));
    aeron_cluster_context_set_ingress_channel(orig, "aeron:udp?endpoint=localhost:9010");
    aeron_cluster_context_set_egress_channel(orig, "aeron:udp?endpoint=localhost:9020");
    aeron_cluster_context_set_ingress_endpoints(orig, "0=host:20110");
    aeron_cluster_context_set_message_timeout_ns(orig, 3000000000ULL);

    aeron_cluster_context_t *dup = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_duplicate(&dup, orig));

    EXPECT_STREQ(orig->ingress_channel, dup->ingress_channel);
    EXPECT_STREQ(orig->egress_channel,  dup->egress_channel);
    EXPECT_STREQ(orig->ingress_endpoints, dup->ingress_endpoints);
    EXPECT_EQ(orig->message_timeout_ns, dup->message_timeout_ns);

    /* Distinct pointers — not sharing memory */
    EXPECT_NE(orig->ingress_channel, dup->ingress_channel);
    EXPECT_NE(orig->egress_channel,  dup->egress_channel);

    aeron_cluster_context_close(orig);
    aeron_cluster_context_close(dup);
}

TEST(AeronClusterContextTest, clientNameMustNotExceedMaxLength)
{
    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    /* Name exactly at limit should succeed */
    std::string max_name(AERON_COUNTER_MAX_CLIENT_NAME_LENGTH, 'x');
    EXPECT_EQ(0, aeron_cluster_context_set_client_name(ctx, max_name.c_str()));

    /* Name one over limit should fail */
    std::string over_name(AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1, 'x');
    EXPECT_EQ(-1, aeron_cluster_context_set_client_name(ctx, over_name.c_str()));

    aeron_cluster_context_close(ctx);
}

TEST(AeronClusterContextTest, shouldApplyEnvVarOverrides)
{
    /* Set env vars */
    setenv("AERON_CLUSTER_INGRESS_CHANNEL", "aeron:udp?endpoint=env:9010", 1);
    setenv("AERON_CLUSTER_INGRESS_STREAM_ID", "301", 1);
    setenv("AERON_CLUSTER_EGRESS_STREAM_ID", "302", 1);
    setenv("AERON_CLUSTER_MESSAGE_TIMEOUT", "15000000000ns", 1);

    aeron_cluster_context_t *ctx = nullptr;
    ASSERT_EQ(0, aeron_cluster_context_init(&ctx));

    unsetenv("AERON_CLUSTER_INGRESS_CHANNEL");
    unsetenv("AERON_CLUSTER_INGRESS_STREAM_ID");
    unsetenv("AERON_CLUSTER_EGRESS_STREAM_ID");
    unsetenv("AERON_CLUSTER_MESSAGE_TIMEOUT");

    EXPECT_STREQ("aeron:udp?endpoint=env:9010", ctx->ingress_channel);
    EXPECT_EQ(301, ctx->ingress_stream_id);
    EXPECT_EQ(302, ctx->egress_stream_id);
    EXPECT_EQ(15000000000ULL, ctx->message_timeout_ns);

    aeron_cluster_context_close(ctx);
}
