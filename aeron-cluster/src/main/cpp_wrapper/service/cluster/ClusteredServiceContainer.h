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

#ifndef AERON_CLUSTER_CLUSTERED_SERVICE_CONTAINER_H
#define AERON_CLUSTER_CLUSTERED_SERVICE_CONTAINER_H

#include <functional>
#include <stdexcept>
#include <atomic>
#include <thread>

#include "ClusteredService.h"
#include "service/aeron_cluster_service_context.h"
#include "service/aeron_clustered_service_agent.h"
#include "client/util/ClusterException.h"

namespace aeron { namespace cluster { namespace service
{

/**
 * C++ wrapper around aeron_clustered_service_agent_t.
 * Runs the agent in a background thread (or invoke manually via doWork()).
 */
class ClusteredServiceContainer
{
public:
    explicit ClusteredServiceContainer(ClusteredService &service) :
        m_service(service),
        m_ctx(nullptr),
        m_agent(nullptr),
        m_running(false)
    {
        if (aeron_cluster_service_context_init(&m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        setupCallbacks();
    }

    ~ClusteredServiceContainer()
    {
        stop();
        if (NULL != m_agent) { aeron_clustered_service_agent_close(m_agent); }
        if (NULL != m_ctx)   { aeron_cluster_service_context_close(m_ctx); }
    }

    ClusteredServiceContainer(const ClusteredServiceContainer &) = delete;
    ClusteredServiceContainer &operator=(const ClusteredServiceContainer &) = delete;

    /** Configure before launch. */
    ClusteredServiceContainer &controlChannel(const std::string &ch)
    { aeron_cluster_service_context_set_control_channel(m_ctx, ch.c_str()); return *this; }

    ClusteredServiceContainer &serviceChannel(const std::string &ch)
    { aeron_cluster_service_context_set_service_channel(m_ctx, ch.c_str()); return *this; }

    ClusteredServiceContainer &clusterDir(const std::string &dir)
    { aeron_cluster_service_context_set_cluster_dir(m_ctx, dir.c_str()); return *this; }

    ClusteredServiceContainer &serviceId(std::int32_t id)
    { aeron_cluster_service_context_set_service_id(m_ctx, id); return *this; }

    /** Start the agent in a background thread. */
    void launch()
    {
        if (aeron_cluster_service_context_conclude(m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_clustered_service_agent_create(&m_agent, m_ctx) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_clustered_service_agent_on_start(m_agent) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_running = true;
        m_thread = std::thread([this]() {
            while (m_running)
            {
                int work = aeron_clustered_service_agent_do_work(m_agent, aeron_nano_clock());
                if (0 == work)
                {
                    m_ctx->idle_strategy_func(m_ctx->idle_strategy_state, 0);
                }
            }
        });
    }

    /** Stop the background thread. */
    void stop()
    {
        m_running = false;
        if (m_thread.joinable()) { m_thread.join(); }
    }

    /** Single duty-cycle call for invoker mode (no background thread). */
    int doWork()
    {
        return aeron_clustered_service_agent_do_work(m_agent, aeron_nano_clock());
    }

private:
    ClusteredService &m_service;
    aeron_cluster_service_context_t *m_ctx;
    aeron_clustered_service_agent_t *m_agent;
    aeron_clustered_service_t        m_c_service{};
    std::atomic<bool>                m_running;
    std::thread                      m_thread;

    void setupCallbacks()
    {
        m_c_service.clientd = this;

        m_c_service.on_start = [](void *cd, aeron_cluster_t *cluster,
            aeron_cluster_snapshot_image_t *snap)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onStart(cluster, snap);
        };
        m_c_service.on_session_open = [](void *cd,
            aeron_cluster_client_session_t *s, int64_t ts)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onSessionOpen(s, ts);
        };
        m_c_service.on_session_close = [](void *cd,
            aeron_cluster_client_session_t *s, int64_t ts,
            aeron_cluster_close_reason_t reason)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onSessionClose(s, ts, reason);
        };
        m_c_service.on_session_message = [](void *cd,
            aeron_cluster_client_session_t *s, int64_t ts,
            const uint8_t *buf, size_t len)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onSessionMessage(s, ts, buf, len);
        };
        m_c_service.on_timer_event = [](void *cd, int64_t correlId, int64_t ts)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onTimerEvent(correlId, ts);
        };
        m_c_service.on_take_snapshot = [](void *cd, aeron_exclusive_publication_t *pub)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onTakeSnapshot(pub);
        };
        m_c_service.on_role_change = [](void *cd, aeron_cluster_role_t role)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onRoleChange(role);
        };
        m_c_service.on_terminate = [](void *cd, aeron_cluster_t *cluster)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service.onTerminate(cluster);
        };
        m_c_service.on_new_leadership_term_event = [](void *cd,
            int64_t ltid, int64_t lpos, int64_t ts, int64_t tblpos,
            int32_t lmid, int32_t lsid, int32_t av)
        {
            static_cast<ClusteredServiceContainer *>(cd)->m_service
                .onNewLeadershipTermEvent(ltid, lpos, ts, tblpos, lmid, lsid, av);
        };
        m_c_service.do_background_work = [](void *cd, int64_t now_ns) -> int
        {
            return static_cast<ClusteredServiceContainer *>(cd)->m_service.doBackgroundWork(now_ns);
        };

        aeron_cluster_service_context_set_service(m_ctx, &m_c_service);
    }
};

}}}

#endif /* AERON_CLUSTER_CLUSTERED_SERVICE_CONTAINER_H */
