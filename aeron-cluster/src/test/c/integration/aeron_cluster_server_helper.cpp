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
 * Implementation of ClusterServerHelper — includes service-side headers.
 * Kept in a separate TU so client-only TUs avoid the aeron_cluster_t typedef clash.
 */

#include "aeron_cluster_server_helper.h"

#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <string>

extern "C"
{
#include "aeronc.h"
#include "aeron_archive.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_service_context.h"
#include "aeron_clustered_service_agent.h"
#include "service/aeron_cluster_client_session.h"
}

/* -----------------------------------------------------------------------
 * Echo service callbacks
 * ----------------------------------------------------------------------- */

static void echo_on_start(void *clientd, aeron_cluster_t *cluster,
                          aeron_cluster_snapshot_image_t *img)
{
    (void)clientd; (void)cluster; (void)img;
}

static void echo_on_terminate(void *clientd, aeron_cluster_t *cluster)
{
    (void)clientd; (void)cluster;
}

static void echo_on_session_message(
    void *clientd, aeron_cluster_client_session_t *session,
    int64_t timestamp, const uint8_t *buffer, size_t length)
{
    (void)timestamp;
    aeron_clustered_service_agent_t *agent =
        static_cast<aeron_clustered_service_agent_t *>(clientd);
    if (agent && session)
    {
        /* Echo directly to client via session's response publication — mirrors Java session.offer() */
        aeron_cluster_client_session_offer(session, agent->leadership_term_id, buffer, length);
    }
}

/* -----------------------------------------------------------------------
 * Server handle
 * ----------------------------------------------------------------------- */

struct cluster_server_handle_stct
{
    aeron_t                         *aeron        = nullptr;
    aeron_context_t                 *aeron_ctx    = nullptr;
    aeron_consensus_module_agent_t  *cm_agent     = nullptr;
    aeron_clustered_service_agent_t *svc_agent    = nullptr;
    aeron_cm_context_t              *cm_ctx       = nullptr;
    aeron_cluster_service_context_t *svc_ctx      = nullptr;
    aeron_clustered_service_t        service{};
};

cluster_server_handle_t *cluster_server_start(
    aeron_t *aeron,
    const char *cluster_dir,
    int archive_port,
    const char *cluster_members,
    int ingress_port,
    int consensus_port)
{
    cluster_server_handle_t *srv = new cluster_server_handle_t{};

    /* Use the caller's Aeron client for the server — same process, same driver,
     * avoids cross-client IPC pub→sub issues with exclusive publications */
    srv->aeron     = aeron;
    srv->aeron_ctx = nullptr; /* not owned */

    /* CM context */
    if (aeron_cm_context_init(&srv->cm_ctx) < 0) { delete srv; return nullptr; }
    srv->cm_ctx->aeron               = srv->aeron;
    srv->cm_ctx->member_id           = 0;
    srv->cm_ctx->appointed_leader_id = 0;
    srv->cm_ctx->service_count       = 1;
    srv->cm_ctx->app_version         = 1;
    srv->cm_ctx->cluster_members     = strdup(cluster_members);
    strncpy(srv->cm_ctx->cluster_dir, cluster_dir, sizeof(srv->cm_ctx->cluster_dir) - 1);

    srv->cm_ctx->consensus_channel        = strdup("aeron:udp");
    srv->cm_ctx->consensus_stream_id      = 108;
    srv->cm_ctx->log_channel              = strdup("aeron:ipc");
    srv->cm_ctx->log_stream_id            = 100;
    srv->cm_ctx->snapshot_channel         = strdup("aeron:ipc");
    srv->cm_ctx->snapshot_stream_id       = 107;
    srv->cm_ctx->control_channel          = strdup("aeron:ipc");
    srv->cm_ctx->consensus_module_stream_id = 105;  /* CM ← service (service publishes on 105) */
    srv->cm_ctx->service_stream_id          = 104;  /* CM → service (service subscribes on 104) */
    srv->cm_ctx->ingress_channel          = strdup("aeron:ipc");
    srv->cm_ctx->ingress_stream_id        = 101;

    /* member_id is the first field of cluster_members */
    srv->cm_ctx->member_id = atoi(cluster_members);

    srv->cm_ctx->startup_canvass_timeout_ns   = INT64_C(200000000);
    srv->cm_ctx->election_timeout_ns          = INT64_C(500000000);
    srv->cm_ctx->election_status_interval_ns  = INT64_C(50000000);
    srv->cm_ctx->leader_heartbeat_timeout_ns  = INT64_C(5000000000);
    srv->cm_ctx->leader_heartbeat_interval_ns = INT64_C(200000000);
    srv->cm_ctx->session_timeout_ns           = INT64_C(10000000000);
    srv->cm_ctx->termination_timeout_ns       = INT64_C(5000000000);

    aeron_archive_context_t *arch_ctx = nullptr;
    aeron_archive_context_init(&arch_ctx);
    aeron_archive_context_set_aeron(arch_ctx, srv->aeron);
    /* Use local IPC to connect to the archive on the same host (avoids port conflict) */
    std::string ctrl = "aeron:udp?endpoint=localhost:" + std::to_string(archive_port);
    aeron_archive_context_set_control_request_channel(arch_ctx, ctrl.c_str());
    aeron_archive_context_set_control_response_channel(arch_ctx, "aeron:udp?endpoint=localhost:0");
    aeron_archive_context_set_control_request_stream_id(arch_ctx, 10);
    aeron_archive_context_set_control_response_stream_id(arch_ctx, 20);
    srv->cm_ctx->archive_ctx      = arch_ctx;
    srv->cm_ctx->owns_archive_ctx = true;

    /* Service context */
    aeron_cluster_service_context_init(&srv->svc_ctx);
    aeron_cluster_service_context_set_aeron(srv->svc_ctx, srv->aeron);
    aeron_cluster_service_context_set_service_id(srv->svc_ctx, 0);
    aeron_cluster_service_context_set_control_channel(srv->svc_ctx, "aeron:ipc");
    aeron_cluster_service_context_set_consensus_module_stream_id(srv->svc_ctx, 104);
    srv->svc_ctx->service_channel    = strdup("aeron:ipc");
    srv->svc_ctx->service_stream_id  = 105;
    srv->svc_ctx->snapshot_channel   = strdup("aeron:ipc");
    srv->svc_ctx->snapshot_stream_id = 107;
    srv->svc_ctx->cluster_id         = 0;
    strncpy(srv->svc_ctx->cluster_dir, cluster_dir, sizeof(srv->svc_ctx->cluster_dir) - 1);

    /* Create agents */
    if (aeron_consensus_module_agent_create(&srv->cm_agent, srv->cm_ctx) < 0)
    { delete srv; return nullptr; }
    if (aeron_clustered_service_agent_create(&srv->svc_agent, srv->svc_ctx) < 0)
    { delete srv; return nullptr; }

    /* Wire echo service callbacks (clientd set after svc_agent created) */
    srv->service.on_start           = echo_on_start;
    srv->service.on_terminate       = echo_on_terminate;
    srv->service.on_session_message = echo_on_session_message;
    srv->service.clientd            = srv->svc_agent;
    srv->svc_ctx->service           = &srv->service;

    /* Start service FIRST so its control subscription is ready for CM's JoinLog */
    if (aeron_clustered_service_agent_on_start(srv->svc_agent) < 0) { delete srv; return nullptr; }
    if (aeron_consensus_module_agent_on_start(srv->cm_agent) < 0) { delete srv; return nullptr; }

    return srv;
}

bool cluster_server_is_leader(const cluster_server_handle_t *srv)
{
    return srv && srv->cm_agent &&
           AERON_CLUSTER_ROLE_LEADER == srv->cm_agent->role;
}

int cluster_server_cm_state(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent) ? static_cast<int>(srv->cm_agent->state) : -1;
}

int cluster_server_session_count(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent && srv->cm_agent->session_manager)
        ? srv->cm_agent->session_manager->session_count : -1;
}

int cluster_server_pending_session_count(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent && srv->cm_agent->session_manager)
        ? srv->cm_agent->session_manager->pending_user_count : -1;
}

int cluster_server_rejected_session_count(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent && srv->cm_agent->session_manager)
        ? (srv->cm_agent->session_manager->rejected_user_count +
           srv->cm_agent->session_manager->redirect_user_count) : -1;
}

int cluster_server_session_open_pending(const cluster_server_handle_t *srv)
{
    if (!srv || !srv->cm_agent || !srv->cm_agent->session_manager) { return -1; }
    auto *mgr = srv->cm_agent->session_manager;
    if (mgr->session_count <= 0) { return 0; }
    auto *s = mgr->sessions[0];
    int pub_connected = (s->response_publication &&
        aeron_exclusive_publication_is_connected(s->response_publication)) ? 1 : 0;
    static bool printed = false;
    if (!printed && s->response_publication) {
        printed = true;
        aeron_publication_constants_t consts;
        if (aeron_exclusive_publication_constants(s->response_publication, &consts) == 0) {
            fprintf(stderr, "[Server] response_pub channel=%s stream=%d connected=%d\n",
                    consts.channel, consts.stream_id, pub_connected);
        }
        fprintf(stderr, "[Server] session id=%lld corr=%lld response_channel='%s' response_stream=%d open_pending=%d\n",
                static_cast<long long>(s->id), static_cast<long long>(s->correlation_id),
                s->response_channel, s->response_stream_id,
                static_cast<int>(s->has_open_event_pending));
    }
    return s->has_open_event_pending ? (10 + pub_connected) : pub_connected;
}

int cluster_server_has_log_pub(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent && srv->cm_agent->log_publication != nullptr) ? 1 : 0;
}

int cluster_server_has_session_log_pub(const cluster_server_handle_t *srv)
{
    return (srv && srv->cm_agent && srv->cm_agent->session_manager &&
            srv->cm_agent->session_manager->log_publisher != nullptr) ? 1 : 0;
}

int cluster_server_svc_has_log_adapter(const cluster_server_handle_t *srv)
{
    return (srv && srv->svc_agent && srv->svc_agent->log_adapter != nullptr) ? 1 : 0;
}

aeron_t *cluster_server_get_aeron(const cluster_server_handle_t *srv)
{
    return srv ? srv->aeron : nullptr;
}

void cluster_server_do_work(cluster_server_handle_t *srv, int64_t now_ns)
{
    if (!srv) { return; }
    if (srv->cm_agent)  { aeron_consensus_module_agent_do_work(srv->cm_agent, now_ns); }
    if (srv->svc_agent)
    {
        static int dw_count = 0;
        if (dw_count < 3) { fprintf(stderr, "[do_work] svc_agent log_adapter=%p\n", static_cast<void*>(srv->svc_agent->log_adapter)); }
        aeron_clustered_service_agent_do_work(srv->svc_agent, now_ns);
        if (dw_count++ < 3) { fprintf(stderr, "[do_work] svc done\n"); }
    }
}

void cluster_server_stop(cluster_server_handle_t *srv)
{
    if (!srv) { return; }
    if (srv->svc_agent) { aeron_clustered_service_agent_close(srv->svc_agent); }
    if (srv->svc_ctx)   { srv->svc_ctx->service = nullptr; aeron_cluster_service_context_close(srv->svc_ctx); }
    if (srv->cm_agent)  { aeron_consensus_module_agent_close(srv->cm_agent); }
    /* aeron is caller-owned — don't close it */
    delete srv;
}
