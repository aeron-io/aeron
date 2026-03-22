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

#ifndef AERON_CLUSTER_CONSENSUS_MODULE_CONTEXT_H
#define AERON_CLUSTER_CONSENSUS_MODULE_CONTEXT_H

#include <string>
#include <cstdint>

#include "consensus/aeron_cm_context.h"
#include "client/util/ClusterException.h"
#include "Aeron.h"

namespace aeron { namespace cluster { namespace consensus
{

using namespace aeron::cluster::client;

class ConsensusModuleContext
{
    friend class ConsensusModule;

public:
    ConsensusModuleContext()
    {
        if (aeron_cm_context_init(&m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    ~ConsensusModuleContext()
    {
        aeron_cm_context_close(m_ctx);
    }

    ConsensusModuleContext(const ConsensusModuleContext &) = delete;
    ConsensusModuleContext &operator=(const ConsensusModuleContext &) = delete;

    ConsensusModuleContext &memberId(std::int32_t id)
    { m_ctx->member_id = id; return *this; }
    std::int32_t memberId() const { return m_ctx->member_id; }

    ConsensusModuleContext &clusterMembers(const std::string &members)
    {
        aeron_free(m_ctx->cluster_members);
        m_ctx->cluster_members = nullptr;
        if (!members.empty())
        {
            const size_t n = members.size() + 1;
            aeron_alloc((void **)&m_ctx->cluster_members, n);
            memcpy(m_ctx->cluster_members, members.c_str(), n);
        }
        return *this;
    }

    ConsensusModuleContext &clusterDir(const std::string &dir)
    { snprintf(m_ctx->cluster_dir, sizeof(m_ctx->cluster_dir), "%s", dir.c_str()); return *this; }
    std::string clusterDir() const { return m_ctx->cluster_dir; }

    ConsensusModuleContext &ingressChannel(const std::string &ch)
    {
        aeron_free(m_ctx->ingress_channel);
        const size_t n = ch.size() + 1;
        aeron_alloc((void **)&m_ctx->ingress_channel, n);
        memcpy(m_ctx->ingress_channel, ch.c_str(), n);
        return *this;
    }

    ConsensusModuleContext &logChannel(const std::string &ch)
    {
        aeron_free(m_ctx->log_channel);
        const size_t n = ch.size() + 1;
        aeron_alloc((void **)&m_ctx->log_channel, n);
        memcpy(m_ctx->log_channel, ch.c_str(), n);
        return *this;
    }

    ConsensusModuleContext &consensusChannel(const std::string &ch)
    {
        aeron_free(m_ctx->consensus_channel);
        const size_t n = ch.size() + 1;
        aeron_alloc((void **)&m_ctx->consensus_channel, n);
        memcpy(m_ctx->consensus_channel, ch.c_str(), n);
        return *this;
    }

    ConsensusModuleContext &serviceCount(int count)
    { m_ctx->service_count = count; return *this; }
    int serviceCount() const { return m_ctx->service_count; }

    ConsensusModuleContext &sessionTimeoutNs(std::int64_t ns)
    { m_ctx->session_timeout_ns = ns; return *this; }

    ConsensusModuleContext &leaderHeartbeatTimeoutNs(std::int64_t ns)
    { m_ctx->leader_heartbeat_timeout_ns = ns; return *this; }

    ConsensusModuleContext &aeron(std::shared_ptr<Aeron> aeronClient)
    {
        m_ctx->aeron = aeronClient->aeron();
        m_aeronW = std::move(aeronClient);
        return *this;
    }

    aeron_cm_context_t *ctx() { return m_ctx; }

private:
    aeron_cm_context_t      *m_ctx = nullptr;
    std::shared_ptr<Aeron>   m_aeronW;
};

}}}

#endif /* AERON_CLUSTER_CONSENSUS_MODULE_CONTEXT_H */
