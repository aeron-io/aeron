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

#ifndef AERON_CLUSTER_CLUSTER_CONTEXT_H
#define AERON_CLUSTER_CLUSTER_CONTEXT_H

#include "AeronCluster.h"
#include "concurrent/YieldingIdleStrategy.h"
#include "Context.h"

namespace aeron { namespace cluster { namespace client
{

typedef std::function<void()> delegating_invoker_t;

typedef std::function<std::pair<const char *, std::uint32_t>()>
    credentials_encoded_credentials_supplier_t;

typedef std::function<std::pair<const char *, std::uint32_t>(
    std::pair<const char *, std::uint32_t> encodedChallenge)>
    credentials_challenge_supplier_t;

typedef std::function<void(std::pair<const char *, std::uint32_t>)>
    credentials_free_t;

struct CredentialsSupplier
{
    credentials_encoded_credentials_supplier_t m_encodedCredentials =
        []() -> std::pair<const char *, std::uint32_t> { return {nullptr, 0}; };
    credentials_challenge_supplier_t m_onChallenge =
        [](std::pair<const char *, std::uint32_t>) -> std::pair<const char *, std::uint32_t> { return {nullptr, 0}; };
    credentials_free_t m_onFree =
        [](std::pair<const char *, std::uint32_t> cred) { delete[] cred.first; };

    explicit CredentialsSupplier(
        credentials_encoded_credentials_supplier_t encodedCredentials =
            []() -> std::pair<const char *, std::uint32_t> { return {nullptr, 0}; },
        credentials_challenge_supplier_t onChallenge =
            [](std::pair<const char *, std::uint32_t>) -> std::pair<const char *, std::uint32_t> { return {nullptr, 0}; },
        credentials_free_t onFree =
            [](std::pair<const char *, std::uint32_t> cred) { delete[] cred.first; }) :
        m_encodedCredentials(std::move(encodedCredentials)),
        m_onChallenge(std::move(onChallenge)),
        m_onFree(std::move(onFree))
    {
    }
};

using egress_message_listener_t = std::function<void(
    std::int64_t clusterSessionId,
    std::int64_t leadershipTermId,
    std::int64_t timestamp,
    const uint8_t *buffer,
    std::size_t length,
    aeron_header_t *header)>;

using session_event_listener_t = std::function<void(
    std::int64_t clusterSessionId,
    std::int64_t correlationId,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    std::int32_t eventCode,
    const std::string &detail)>;

using new_leader_event_listener_t = std::function<void(
    std::int64_t clusterSessionId,
    std::int64_t leadershipTermId,
    std::int32_t leaderMemberId,
    const std::string &ingressEndpoints)>;

using admin_response_listener_t = std::function<void(
    std::int64_t clusterSessionId,
    std::int64_t correlationId,
    std::int32_t requestType,
    std::int32_t responseCode,
    const std::string &message,
    const uint8_t *payload,
    std::size_t payloadLength)>;

class Context
{
    friend class AeronCluster;

public:
    Context()
    {
        if (aeron_cluster_context_init(&m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        setupContext();
    }

    ~Context()
    {
        aeron_cluster_context_close(m_ctx);
        m_ctx = nullptr;
    }

    inline Context &aeron(std::shared_ptr<Aeron> aeron)
    {
        if (aeron_cluster_context_set_aeron(m_ctx, aeron->aeron()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_aeronW = std::move(aeron);
        return *this;
    }

    inline std::shared_ptr<Aeron> aeron() const { return m_aeronW; }

    inline Context &aeronDirectoryName(const std::string &dir)
    {
        aeron_cluster_context_set_aeron_directory_name(m_ctx, dir.c_str());
        return *this;
    }
    inline std::string aeronDirectoryName() const
    {
        return {aeron_cluster_context_get_aeron_directory_name(m_ctx)};
    }

    inline Context &clientName(const std::string &name)
    {
        if (aeron_cluster_context_set_client_name(m_ctx, name.c_str()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return *this;
    }
    inline std::string clientName() const
    {
        return {aeron_cluster_context_get_client_name(m_ctx)};
    }

    inline Context &ingressChannel(const std::string &channel)
    {
        if (aeron_cluster_context_set_ingress_channel(m_ctx, channel.c_str()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return *this;
    }
    inline std::string ingressChannel() const
    {
        const char *ch = aeron_cluster_context_get_ingress_channel(m_ctx);
        return ch != nullptr ? ch : "";
    }

    inline Context &ingressStreamId(std::int32_t streamId)
    {
        aeron_cluster_context_set_ingress_stream_id(m_ctx, streamId);
        return *this;
    }
    inline std::int32_t ingressStreamId() const
    {
        return aeron_cluster_context_get_ingress_stream_id(m_ctx);
    }

    inline Context &ingressEndpoints(const std::string &endpoints)
    {
        if (aeron_cluster_context_set_ingress_endpoints(m_ctx, endpoints.c_str()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return *this;
    }
    inline std::string ingressEndpoints() const
    {
        const char *ep = aeron_cluster_context_get_ingress_endpoints(m_ctx);
        return ep != nullptr ? ep : "";
    }

    inline Context &egressChannel(const std::string &channel)
    {
        if (aeron_cluster_context_set_egress_channel(m_ctx, channel.c_str()) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return *this;
    }
    inline std::string egressChannel() const
    {
        const char *ch = aeron_cluster_context_get_egress_channel(m_ctx);
        return ch != nullptr ? ch : "";
    }

    inline Context &egressStreamId(std::int32_t streamId)
    {
        aeron_cluster_context_set_egress_stream_id(m_ctx, streamId);
        return *this;
    }
    inline std::int32_t egressStreamId() const
    {
        return aeron_cluster_context_get_egress_stream_id(m_ctx);
    }

    inline Context &messageTimeoutNs(std::uint64_t timeoutNs)
    {
        aeron_cluster_context_set_message_timeout_ns(m_ctx, timeoutNs);
        return *this;
    }
    inline std::uint64_t messageTimeoutNs() const
    {
        return aeron_cluster_context_get_message_timeout_ns(m_ctx);
    }

    inline Context &messageRetryAttempts(std::uint32_t attempts)
    {
        aeron_cluster_context_set_message_retry_attempts(m_ctx, attempts);
        return *this;
    }
    inline std::uint32_t messageRetryAttempts() const
    {
        return aeron_cluster_context_get_message_retry_attempts(m_ctx);
    }

    template<typename IdleStrategy>
    inline Context &idleStrategy(IdleStrategy &strategy)
    {
        m_idleFunc = [&strategy](int work_count){ strategy.idle(work_count); };
        return *this;
    }

    inline Context &credentialsSupplier(const CredentialsSupplier &cs)
    {
        m_credentialsSupplier = cs;
        return *this;
    }

    inline Context &onMessage(egress_message_listener_t listener)
    {
        m_onMessage = std::move(listener);
        return *this;
    }

    inline Context &onSessionEvent(session_event_listener_t listener)
    {
        m_onSessionEvent = std::move(listener);
        return *this;
    }

    inline Context &onNewLeaderEvent(new_leader_event_listener_t listener)
    {
        m_onNewLeaderEvent = std::move(listener);
        return *this;
    }

    inline Context &onAdminResponse(admin_response_listener_t listener)
    {
        m_onAdminResponse = std::move(listener);
        return *this;
    }

    inline Context &errorHandler(const exception_handler_t &handler)
    {
        if (aeron_cluster_context_set_error_handler(m_ctx, errorHandlerFunc, this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        m_errorHandler = handler;
        return *this;
    }

    inline Context &delegatingInvoker(delegating_invoker_t invoker)
    {
        m_delegatingInvoker = std::move(invoker);
        return *this;
    }

private:
    aeron_cluster_context_t *m_ctx = nullptr;
    std::shared_ptr<Aeron> m_aeronW = nullptr;

    std::function<void(int)> m_idleFunc;
    YieldingIdleStrategy m_defaultIdleStrategy;

    CredentialsSupplier m_credentialsSupplier;
    aeron_cluster_encoded_credentials_t m_lastCredentials = {};

    egress_message_listener_t      m_onMessage       = nullptr;
    session_event_listener_t       m_onSessionEvent  = nullptr;
    new_leader_event_listener_t    m_onNewLeaderEvent = nullptr;
    admin_response_listener_t      m_onAdminResponse = nullptr;
    exception_handler_t            m_errorHandler    = nullptr;
    delegating_invoker_t           m_delegatingInvoker = nullptr;

    explicit Context(aeron_cluster_context_t *ctx) : m_ctx(ctx)
    {
        setupContext();
    }

    void setupContext()
    {
        if (aeron_cluster_context_set_idle_strategy(m_ctx, idleFunc, this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_cluster_context_set_credentials_supplier(
            m_ctx, encodedCredentialsFunc, onChallengeFunc, onFreeFunc, this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_cluster_context_set_on_message(m_ctx, onMessageFunc, this) < 0 ||
            aeron_cluster_context_set_on_session_event(m_ctx, onSessionEventFunc, this) < 0 ||
            aeron_cluster_context_set_on_new_leader_event(m_ctx, onNewLeaderEventFunc, this) < 0 ||
            aeron_cluster_context_set_on_admin_response(m_ctx, onAdminResponseFunc, this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_cluster_context_set_delegating_invoker(m_ctx, delegatingInvokerFunc, this) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        idleStrategy(m_defaultIdleStrategy);
    }

    static void idleFunc(void *clientd, int work_count)
    {
        static_cast<Context *>(clientd)->m_idleFunc(work_count);
    }

    static aeron_cluster_encoded_credentials_t *encodedCredentialsFunc(void *clientd)
    {
        auto *ctx = static_cast<Context *>(clientd);
        auto cred = ctx->m_credentialsSupplier.m_encodedCredentials();
        ctx->m_lastCredentials = {cred.first, cred.second};
        return &ctx->m_lastCredentials;
    }

    static aeron_cluster_encoded_credentials_t *onChallengeFunc(
        aeron_cluster_encoded_credentials_t *challenge, void *clientd)
    {
        auto *ctx = static_cast<Context *>(clientd);
        auto cred = ctx->m_credentialsSupplier.m_onChallenge({challenge->data, challenge->length});
        ctx->m_lastCredentials = {cred.first, cred.second};
        return &ctx->m_lastCredentials;
    }

    static void onFreeFunc(aeron_cluster_encoded_credentials_t *cred, void *clientd)
    {
        auto *ctx = static_cast<Context *>(clientd);
        ctx->m_credentialsSupplier.m_onFree({cred->data, cred->length});
    }

    static void onMessageFunc(
        void *clientd,
        std::int64_t sessionId, std::int64_t termId, std::int64_t ts,
        const uint8_t *buf, std::size_t len, aeron_header_t *hdr)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_onMessage) { ctx->m_onMessage(sessionId, termId, ts, buf, len, hdr); }
    }

    static void onSessionEventFunc(
        void *clientd,
        std::int64_t sessionId, std::int64_t correlId, std::int64_t termId,
        std::int32_t leaderId, std::int32_t code,
        const char *detail, std::size_t detailLen)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_onSessionEvent)
        {
            ctx->m_onSessionEvent(sessionId, correlId, termId, leaderId, code,
                                  std::string(detail, detailLen));
        }
    }

    static void onNewLeaderEventFunc(
        void *clientd,
        std::int64_t sessionId, std::int64_t termId, std::int32_t leaderId,
        const char *endpoints, std::size_t endpointsLen)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_onNewLeaderEvent)
        {
            ctx->m_onNewLeaderEvent(sessionId, termId, leaderId, std::string(endpoints, endpointsLen));
        }
    }

    static void onAdminResponseFunc(
        void *clientd,
        std::int64_t sessionId, std::int64_t correlId,
        std::int32_t reqType, std::int32_t respCode,
        const char *msg, std::size_t msgLen,
        const uint8_t *payload, std::size_t payloadLen)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_onAdminResponse)
        {
            ctx->m_onAdminResponse(sessionId, correlId, reqType, respCode,
                                   std::string(msg, msgLen), payload, payloadLen);
        }
    }

    static void errorHandlerFunc(void *clientd, int errcode, const char *message)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_errorHandler)
        {
            try { CLUSTER_MAP_TO_SOURCED_EXCEPTION_AND_THROW(errcode, message); }
            catch (SourcedException &e) { ctx->m_errorHandler(e); }
        }
    }

    static void delegatingInvokerFunc(void *clientd)
    {
        auto *ctx = static_cast<Context *>(clientd);
        if (ctx->m_delegatingInvoker) { ctx->m_delegatingInvoker(); }
    }
};

}}}

#endif /* AERON_CLUSTER_CLUSTER_CONTEXT_H */
