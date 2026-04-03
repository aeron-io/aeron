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

#include "aeron_test_clustered_media_driver.h"

#include <cstring>
#include <cstdio>
#include <thread>
#include <atomic>
#include <chrono>

extern "C"
{
#include "aeronc.h"
#include "aeronmd.h"
#include "server/aeron_archiving_media_driver.h"
#include "aeron_archive.h"
#include "aeron_cm_context.h"
#include "aeron_consensus_module_agent.h"
#include "util/aeron_fileutil.h"
}

struct TestClusteredMediaDriver::Impl
{
    int                  node_index;
    std::string          aeron_dir;
    std::string          archive_dir;
    std::string          cluster_dir;
    std::string          cluster_members;
    std::string          ingress_endpoints;
    std::ostream        &stream;

    aeron_archiving_media_driver_t  *archiving_driver = nullptr;
    aeron_consensus_module_agent_t  *cm_agent         = nullptr;
    aeron_cm_context_t              *cm_ctx           = nullptr;

    std::thread          cm_thread;
    std::atomic<bool>    cm_running{false};

    Impl(int idx, std::ostream &s) : node_index(idx), stream(s) {}

    int launch_archiving_media_driver()
    {
        if (aeron_mkdir_recursive(aeron_dir.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: mkdir aeron_dir failed: " << aeron_errmsg() << std::endl;
            return -1;
        }
        if (aeron_mkdir_recursive(archive_dir.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: mkdir archive_dir failed: " << aeron_errmsg() << std::endl;
            return -1;
        }
        if (aeron_mkdir_recursive(cluster_dir.c_str(), S_IRWXU | S_IRWXG | S_IRWXO) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: mkdir cluster_dir failed: " << aeron_errmsg() << std::endl;
            return -1;
        }

        stream << "[CMD " << node_index << "] Launching ArchivingMediaDriver..." << std::endl;

        aeron_driver_context_t *driver_ctx = nullptr;
        if (aeron_driver_context_init(&driver_ctx) < 0) { return -1; }

        aeron_driver_context_set_dir(driver_ctx, aeron_dir.c_str());
        aeron_driver_context_set_dir_delete_on_start(driver_ctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driver_ctx, true);
        aeron_driver_context_set_term_buffer_sparse_file(driver_ctx, true);
        aeron_driver_context_set_perform_storage_checks(driver_ctx, false);
        aeron_driver_context_set_term_buffer_length(driver_ctx, 64 * 1024);
        aeron_driver_context_set_ipc_term_buffer_length(driver_ctx, 64 * 1024);
        aeron_driver_context_set_threading_mode(driver_ctx, AERON_THREADING_MODE_SHARED);

        aeron_archive_server_context_t *archive_ctx = nullptr;
        if (aeron_archive_server_context_init(&archive_ctx) < 0) { return -1; }

        snprintf(archive_ctx->archive_dir, sizeof(archive_ctx->archive_dir), "%s", archive_dir.c_str());
        snprintf(archive_ctx->mark_file_dir, sizeof(archive_ctx->mark_file_dir), "%s", aeron_dir.c_str());
        archive_ctx->delete_archive_on_start = true;
        archive_ctx->archive_id = node_index;
        archive_ctx->recording_events_enabled = false;

        if (archive_ctx->control_channel != nullptr) { free(archive_ctx->control_channel); }
        archive_ctx->control_channel = strdup("aeron:ipc");
        archive_ctx->control_stream_id = AERON_ARCHIVE_SERVER_CONTROL_STREAM_ID_DEFAULT;

        if (archive_ctx->replication_channel != nullptr) { free(archive_ctx->replication_channel); }
        archive_ctx->replication_channel = strdup("aeron:udp?endpoint=localhost:0");

        if (aeron_archiving_media_driver_launch(&archiving_driver, driver_ctx, archive_ctx) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: archiving media driver launch failed" << std::endl;
            return -1;
        }

        stream << "[CMD " << node_index << "] ArchivingMediaDriver ready" << std::endl;
        return 0;
    }

    int launch_consensus_module()
    {
        stream << "[CMD " << node_index << "] Launching ConsensusModule..." << std::endl;

        if (aeron_cm_context_init(&cm_ctx) < 0) { return -1; }

        snprintf(cm_ctx->aeron_directory_name, sizeof(cm_ctx->aeron_directory_name),
                 "%s", aeron_dir.c_str());
        cm_ctx->member_id           = 0;
        cm_ctx->appointed_leader_id = 0;
        cm_ctx->service_count       = 1;
        cm_ctx->app_version         = 1;

        if (cm_ctx->cluster_members) { free(cm_ctx->cluster_members); }
        cm_ctx->cluster_members = strdup(cluster_members.c_str());
        strncpy(cm_ctx->cluster_dir, cluster_dir.c_str(), sizeof(cm_ctx->cluster_dir) - 1);

        if (cm_ctx->consensus_channel) { free(cm_ctx->consensus_channel); }
        cm_ctx->consensus_channel   = strdup("aeron:udp?alias=consensus");
        cm_ctx->consensus_stream_id = 108;
        if (cm_ctx->log_channel) { free(cm_ctx->log_channel); }
        cm_ctx->log_channel         = strdup("aeron:ipc");
        cm_ctx->log_stream_id       = 100;
        if (cm_ctx->snapshot_channel) { free(cm_ctx->snapshot_channel); }
        cm_ctx->snapshot_channel    = strdup("aeron:ipc?term-length=128k");
        cm_ctx->snapshot_stream_id  = 107;
        if (cm_ctx->control_channel) { free(cm_ctx->control_channel); }
        cm_ctx->control_channel     = strdup("aeron:ipc?term-length=128k");
        cm_ctx->consensus_module_stream_id = 105;
        cm_ctx->service_stream_id          = 104;
        if (cm_ctx->ingress_channel) { free(cm_ctx->ingress_channel); }
        cm_ctx->ingress_channel     = strdup("aeron:ipc");
        cm_ctx->ingress_stream_id   = 101;

        cm_ctx->startup_canvass_timeout_ns   = INT64_C(200000000);
        cm_ctx->election_timeout_ns          = INT64_C(500000000);
        cm_ctx->election_status_interval_ns  = INT64_C(50000000);
        cm_ctx->leader_heartbeat_timeout_ns  = INT64_C(5000000000);
        cm_ctx->leader_heartbeat_interval_ns = INT64_C(200000000);
        cm_ctx->session_timeout_ns           = INT64_C(10000000000);
        cm_ctx->termination_timeout_ns       = INT64_C(5000000000);

        /* Archive context for CM — match Java ConsensusModule.Context.conclude():
         * controlRequestChannel  = localControlChannel()  = "aeron:ipc?term-length=64k"
         * controlResponseChannel = localControlChannel()  = "aeron:ipc?term-length=64k"
         * controlRequestStreamId = localControlStreamId() = 10
         * controlResponseStreamId = clusterId*100 + 100 + 20 = 120
         * aeron(aeron) + ownsAeronClient(false) — shared in on_start */
        aeron_archive_context_t *cm_arch_ctx = nullptr;
        if (aeron_archive_context_init(&cm_arch_ctx) < 0) { return -1; }
        aeron_archive_context_set_aeron_directory_name(cm_arch_ctx, aeron_dir.c_str());
        aeron_archive_context_set_control_request_channel(cm_arch_ctx, "aeron:ipc?term-length=64k");
        aeron_archive_context_set_control_response_channel(cm_arch_ctx, "aeron:ipc?term-length=64k");
        aeron_archive_context_set_control_request_stream_id(cm_arch_ctx, 10);
        aeron_archive_context_set_control_response_stream_id(cm_arch_ctx, 120);
        cm_ctx->archive_ctx      = cm_arch_ctx;
        cm_ctx->owns_archive_ctx = true;

        if (aeron_consensus_module_agent_create(&cm_agent, cm_ctx) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: CM agent create failed: " << aeron_errmsg() << std::endl;
            return -1;
        }

        if (aeron_consensus_module_agent_on_start(cm_agent) < 0)
        {
            stream << "[CMD " << node_index << "] ERROR: CM on_start failed: " << aeron_errmsg() << std::endl;
            return -1;
        }

        cm_running.store(true);
        cm_thread = std::thread([this]()
        {
            while (cm_running.load())
            {
                int64_t now_ns = aeron_nano_clock();
                if (cm_agent != nullptr)
                {
                    aeron_consensus_module_agent_do_work(cm_agent, now_ns);
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
            }
        });

        stream << "[CMD " << node_index << "] ConsensusModule started on background thread" << std::endl;
        return 0;
    }

    void close()
    {
        if (cm_running.load())
        {
            cm_running.store(false);
            if (cm_thread.joinable()) { cm_thread.join(); }
        }

        if (cm_agent != nullptr)
        {
            aeron_consensus_module_agent_close(cm_agent);
            cm_agent = nullptr;
        }
        if (cm_ctx != nullptr)
        {
            aeron_cm_context_close(cm_ctx);
            cm_ctx = nullptr;
        }

        if (archiving_driver != nullptr)
        {
            stream << "[CMD " << node_index << "] Stopping ArchivingMediaDriver" << std::endl;
            aeron_archiving_media_driver_close(archiving_driver);
            archiving_driver = nullptr;
        }

        (void)aeron_delete_directory(aeron_dir.c_str());
        (void)aeron_delete_directory(archive_dir.c_str());
        (void)aeron_delete_directory(cluster_dir.c_str());
    }

    bool is_leader() const
    {
        return cm_agent != nullptr && AERON_CLUSTER_ROLE_LEADER == cm_agent->role;
    }
};

/* -----------------------------------------------------------------------
 * TestClusteredMediaDriver public API
 * ----------------------------------------------------------------------- */

TestClusteredMediaDriver::TestClusteredMediaDriver(
    int node_index,
    int node_count,
    const std::string &base_dir,
    std::ostream &stream)
{
    m_impl = new Impl(node_index, stream);
    m_impl->aeron_dir   = base_dir + "/node" + std::to_string(node_index) + "/aeron";
    m_impl->archive_dir = base_dir + "/node" + std::to_string(node_index) + "/archive";
    m_impl->cluster_dir = base_dir + "/node" + std::to_string(node_index) + "/cluster";

    int base = node_index - (node_index % ((node_count > 1) ? node_count : 1));
    if (node_count == 1) { base = node_index; }
    m_impl->cluster_members = "";
    for (int i = 0; i < node_count; i++)
    {
        int mn = base + i;
        if (i > 0) m_impl->cluster_members += "|";
        m_impl->cluster_members += std::to_string(i) +
            ",localhost:" + std::to_string(20110 + mn) +
            ",localhost:" + std::to_string(20220 + mn) +
            ",localhost:" + std::to_string(20330 + mn) +
            ",localhost:0" +
            ",localhost:" + std::to_string(8010 + mn);
    }

    m_impl->ingress_endpoints = "";
    for (int i = 0; i < node_count; i++)
    {
        int mn = base + i;
        if (i > 0) m_impl->ingress_endpoints += ",";
        m_impl->ingress_endpoints += std::to_string(i) + "=localhost:" + std::to_string(20110 + mn);
    }
}

TestClusteredMediaDriver::~TestClusteredMediaDriver()
{
    close();
    delete m_impl;
}

int TestClusteredMediaDriver::launch_driver_only()
{
    return m_impl->launch_archiving_media_driver();
}

int TestClusteredMediaDriver::launch()
{
    if (m_impl->launch_archiving_media_driver() < 0) { return -1; }
    return m_impl->launch_consensus_module();
}

void TestClusteredMediaDriver::close()
{
    m_impl->close();
}

bool TestClusteredMediaDriver::is_leader() const
{
    return m_impl->is_leader();
}

const std::string &TestClusteredMediaDriver::aeron_dir() const { return m_impl->aeron_dir; }
const std::string &TestClusteredMediaDriver::archive_dir() const { return m_impl->archive_dir; }
const std::string &TestClusteredMediaDriver::cluster_dir() const { return m_impl->cluster_dir; }
const std::string &TestClusteredMediaDriver::cluster_members() const { return m_impl->cluster_members; }
const std::string &TestClusteredMediaDriver::ingress_endpoints() const { return m_impl->ingress_endpoints; }
int TestClusteredMediaDriver::node_index() const { return m_impl->node_index; }
