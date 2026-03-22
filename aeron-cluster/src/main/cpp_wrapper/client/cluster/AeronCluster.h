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

#ifndef AERON_CLUSTER_AERON_CLUSTER_H
#define AERON_CLUSTER_AERON_CLUSTER_H

#include "client/aeron_cluster_client.h"
#include "client/aeron_cluster_async_connect.h"

#include "Aeron.h"
#include "client/util/ClusterException.h"

namespace aeron { namespace cluster { namespace client
{

class Context;  // forward declaration — defined in ClusterContext.h

/**
 * C++ wrapper for the Aeron Cluster client.
 * Mirrors the Java AeronCluster API.
 */
class AeronCluster
{
public:
    using Context_t = Context;

    /**
     * Allows for the async establishment of a cluster session.
     */
    class AsyncConnect
    {
        friend class AeronCluster;

    public:
        /**
         * Poll for a complete connection.
         * @return a new AeronCluster when successfully connected, nullptr if not yet done.
         */
        std::shared_ptr<AeronCluster> poll()
        {
            if (nullptr == m_async)
            {
                throw ClusterException("AsyncConnect already complete", SOURCEINFO);
            }

            aeron_cluster_t *cluster = nullptr;

            if (aeron_cluster_async_connect_poll(&cluster, m_async) < 0)
            {
                CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            if (nullptr == cluster)
            {
                return {};
            }

            m_async = nullptr;

            return std::shared_ptr<AeronCluster>(new AeronCluster(cluster, m_aeronW));
        }

    private:
        explicit AsyncConnect(Context &ctx);

        aeron_cluster_async_connect_t *m_async;
        std::shared_ptr<Aeron>         m_aeronW;
    };

    /**
     * Begin an asynchronous connect.
     */
    static std::shared_ptr<AsyncConnect> asyncConnect(Context &ctx)
    {
        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    /**
     * Connect synchronously. Blocks until connected or timeout.
     */
    static std::shared_ptr<AeronCluster> connect(Context &ctx);

    ~AeronCluster()
    {
        aeron_cluster_close(m_cluster);
    }

    AeronCluster(const AeronCluster &) = delete;
    AeronCluster &operator=(const AeronCluster &) = delete;

    inline bool isClosed() const
    {
        return aeron_cluster_is_closed(m_cluster);
    }

    inline std::int64_t clusterSessionId() const
    {
        return aeron_cluster_cluster_session_id(m_cluster);
    }

    inline std::int64_t leadershipTermId() const
    {
        return aeron_cluster_leadership_term_id(m_cluster);
    }

    inline std::int32_t leaderMemberId() const
    {
        return aeron_cluster_leader_member_id(m_cluster);
    }

    /**
     * Offer a message to the cluster (session header prepended automatically).
     * @return offer position > 0 on success; AERON_PUBLICATION_* error codes on failure.
     */
    inline std::int64_t offer(const uint8_t *buffer, std::size_t length)
    {
        return aeron_cluster_offer(m_cluster, buffer, length);
    }

    inline std::int64_t offer(AtomicBuffer &buffer, util::index_t offset, util::index_t length)
    {
        return aeron_cluster_offer(
            m_cluster,
            reinterpret_cast<const uint8_t *>(buffer.buffer()) + offset,
            static_cast<std::size_t>(length));
    }

    /**
     * Send a keep-alive to prevent session timeout.
     * @return true if sent.
     */
    inline bool sendKeepAlive()
    {
        return aeron_cluster_send_keep_alive(m_cluster);
    }

    /**
     * Send an admin request to trigger a cluster snapshot.
     */
    inline std::int64_t sendAdminRequestToTakeASnapshot(std::int64_t correlationId)
    {
        std::int64_t result = aeron_cluster_send_admin_request_snapshot(m_cluster, correlationId);
        if (result < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return result;
    }

    /**
     * Poll the egress subscription for messages and events.
     * @return number of fragments consumed.
     */
    inline int pollEgress()
    {
        int fragments = aeron_cluster_poll_egress(m_cluster);
        if (fragments < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return fragments;
    }

    /**
     * Track the result of an offer() to detect leader changes.
     */
    inline void trackIngressPublicationResult(std::int64_t result)
    {
        aeron_cluster_track_ingress_result(m_cluster, result);
    }

    aeron_cluster_t *cluster() { return m_cluster; }

private:
    explicit AeronCluster(aeron_cluster_t *cluster, std::shared_ptr<Aeron> aeronW) :
        m_cluster(cluster),
        m_aeronW(std::move(aeronW))
    {
    }

    aeron_cluster_t        *m_cluster;
    std::shared_ptr<Aeron>  m_aeronW;
};

}}}

// ClusterContext.h is included AFTER AeronCluster so the Context forward declaration is resolved.
#include "ClusterContext.h"

namespace aeron { namespace cluster { namespace client
{

// ----- AeronCluster::AsyncConnect constructor (needs full Context definition) -----
inline AeronCluster::AsyncConnect::AsyncConnect(Context &ctx) :
    m_async(nullptr),
    m_aeronW(ctx.aeron())
{
    if (aeron_cluster_async_connect(&m_async, ctx.m_ctx) < 0)
    {
        CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }
}

// ----- AeronCluster::connect (needs full Context definition) -----
inline std::shared_ptr<AeronCluster> AeronCluster::connect(Context &ctx)
{
    aeron_cluster_t *cluster = nullptr;

    if (aeron_cluster_connect(&cluster, ctx.m_ctx) < 0)
    {
        CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
    }

    return std::shared_ptr<AeronCluster>(new AeronCluster(cluster, ctx.aeron()));
}

}}}

#endif /* AERON_CLUSTER_AERON_CLUSTER_H */
