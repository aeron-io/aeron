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

#ifndef AERON_CLUSTER_CLUSTERED_SERVICE_H
#define AERON_CLUSTER_CLUSTERED_SERVICE_H

#include "service/aeron_cluster_service.h"
#include "service/aeron_clustered_service_agent.h"
#include "client/cluster/AeronCluster.h"
#include "AtomicBuffer.h"
#include "ExclusivePublication.h"

namespace aeron { namespace cluster { namespace service
{

using namespace aeron::cluster::client;

/**
 * Pure-virtual base class for C++ clustered services.
 * Implement this and pass an instance to ClusteredServiceContainer.
 */
class ClusteredService
{
public:
    virtual ~ClusteredService() = default;

    /**
     * Called once on startup (or recovery from snapshot).
     * @param snapshotImage non-null when recovering; read state from it.
     */
    virtual void onStart(
        aeron_cluster_t *cluster,
        aeron_cluster_snapshot_image_t *snapshotImage) = 0;

    virtual void onSessionOpen(
        aeron_cluster_client_session_t *session,
        std::int64_t timestamp) = 0;

    virtual void onSessionClose(
        aeron_cluster_client_session_t *session,
        std::int64_t timestamp,
        aeron_cluster_close_reason_t closeReason) = 0;

    virtual void onSessionMessage(
        aeron_cluster_client_session_t *session,
        std::int64_t timestamp,
        const std::uint8_t *buffer,
        std::size_t length) = 0;

    virtual void onTimerEvent(
        std::int64_t correlationId,
        std::int64_t timestamp) = 0;

    /**
     * Write service state to snapshotPublication.
     * Frame with aeron_cluster_service_snapshot_taker_* helpers.
     */
    virtual void onTakeSnapshot(aeron_exclusive_publication_t *snapshotPublication) = 0;

    virtual void onRoleChange(aeron_cluster_role_t newRole) = 0;

    virtual void onTerminate(aeron_cluster_t *cluster) = 0;

    /* Optional overrides */
    virtual void onNewLeadershipTermEvent(
        std::int64_t leadershipTermId, std::int64_t logPosition,
        std::int64_t timestamp, std::int64_t termBaseLogPosition,
        std::int32_t leaderMemberId, std::int32_t logSessionId,
        std::int32_t appVersion) {}

    virtual int doBackgroundWork(std::int64_t nowNs) { return 0; }
};

}}}

#endif /* AERON_CLUSTER_CLUSTERED_SERVICE_H */
