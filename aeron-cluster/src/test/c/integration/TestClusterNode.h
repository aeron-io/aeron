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

#ifndef AERON_TEST_CLUSTER_NODE_H
#define AERON_TEST_CLUSTER_NODE_H

#include <string>
#include <iostream>
#include <thread>
#include <atomic>
#include <cstdlib>
#include <cstdint>

extern "C"
{
#include <signal.h>
#include "aeronc.h"
#include "aeron_archive.h"
#include "util/aeron_fileutil.h"
#include "concurrent/aeron_thread.h"
}

#ifndef _WIN32
#include "spawn.h"
#include <sys/wait.h>
#endif

#define CLUSTER_ARCHIVE_MARK_FILE_HEADER_LENGTH (8192)

/**
 * Launches a Java ArchivingMediaDriver configured for cluster use (one per node).
 * The C ConsensusModule and ClusteredService agents connect to the shared MediaDriver.
 *
 * Usage:
 *   TestClusterNode node(0, 3, baseDir, std::cout);
 *   node.start();
 *   // ... do_work loop ...
 *   node.stop();
 */
class TestClusterNode
{
public:
    TestClusterNode(
        int node_index,
        int node_count,
        const std::string &base_dir,
        std::ostream &stream)
        : m_node_index(node_index),
          m_stream(stream)
    {
        m_aeron_dir   = base_dir + "/node" + std::to_string(node_index) + "/aeron";
        m_archive_dir = base_dir + "/node" + std::to_string(node_index) + "/archive";
        m_cluster_dir = base_dir + "/node" + std::to_string(node_index) + "/cluster";

        /* Build cluster members string — ports use node_index directly.
         * For multi-node: caller creates TestClusterNode(base+0, N), (base+1, N), (base+2, N)
         * so all share the same [base, base+N-1] range and identical members string. */
        int base = node_index - (node_index % ((node_count > 1) ? node_count : 1));
        if (node_count == 1) { base = node_index; }
        m_cluster_members = "";
        for (int i = 0; i < node_count; i++)
        {
            int mn = base + i;
            if (i > 0) m_cluster_members += "|";
            m_cluster_members += std::to_string(i) +
                ",localhost:" + std::to_string(20110 + mn) +
                ",localhost:" + std::to_string(20220 + mn) +
                ",localhost:" + std::to_string(20330 + mn) +
                ",localhost:0" +
                ",localhost:" + std::to_string(8010 + mn);
        }

        m_ingress_endpoints = "";
        for (int i = 0; i < node_count; i++)
        {
            int mn = base + i;
            if (i > 0) m_ingress_endpoints += ",";
            m_ingress_endpoints += std::to_string(i) + "=localhost:" + std::to_string(20110 + mn);
        }
    }

    void start()
    {
        /* Create directories */
        std::system(("mkdir -p " + m_aeron_dir).c_str());
        std::system(("mkdir -p " + m_archive_dir).c_str());
        std::system(("mkdir -p " + m_cluster_dir).c_str());

        start_archiving_media_driver();
    }

    void stop()
    {
        stop_archiving_media_driver();
    }

    const std::string &aeron_dir() const { return m_aeron_dir; }
    const std::string &archive_dir() const { return m_archive_dir; }
    const std::string &cluster_dir() const { return m_cluster_dir; }
    const std::string &cluster_members() const { return m_cluster_members; }
    const std::string &ingress_endpoints() const { return m_ingress_endpoints; }
    int node_index() const { return m_node_index; }
    int consensus_port() const { return 20220 + m_node_index; }
    int archive_port() const { return 8010 + m_node_index; }

private:
    void start_archiving_media_driver()
    {
        m_stream << "[Node " << m_node_index << "] Starting ArchivingMediaDriver..." << std::endl;

        std::string aeronDirArg       = "-Daeron.dir=" + m_aeron_dir;
        std::string archiveDirArg     = "-Daeron.archive.dir=" + m_archive_dir;
        std::string archiveMarkDirArg = "-Daeron.archive.mark.file.dir=" + m_aeron_dir;
        std::string controlChannel    = "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:" + std::to_string(archive_port());
        std::string archiveIdArg      = "-Daeron.archive.id=" + std::to_string(m_node_index);

        const char *const argv[] =
        {
            JAVA_EXECUTABLE,
            "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens", "java.base/java.util.zip=ALL-UNNAMED",
            "-Daeron.dir.delete.on.start=true",
            "-Daeron.dir.delete.on.shutdown=true",
            "-Daeron.archive.dir.delete.on.start=true",
            "-Daeron.archive.max.catalog.entries=128",
            "-Daeron.term.buffer.sparse.file=true",
            "-Daeron.perform.storage.checks=false",
            "-Daeron.term.buffer.length=64k",
            "-Daeron.ipc.term.buffer.length=64k",
            "-Daeron.threading.mode=SHARED",
            "-Daeron.shared.idle.strategy=yield",
            "-Daeron.archive.threading.mode=SHARED",
            "-Daeron.archive.idle.strategy=yield",
            "-Daeron.archive.recording.events.enabled=false",
            "-Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator",
            "-Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0",
            "-Daeron.archive.replication.channel=aeron:udp?endpoint=localhost:0",
            controlChannel.c_str(),
            archiveIdArg.c_str(),
            archiveDirArg.c_str(),
            archiveMarkDirArg.c_str(),
            aeronDirArg.c_str(),
            "-cp", AERON_ALL_JAR,
            "io.aeron.archive.ArchivingMediaDriver",
            nullptr
        };

#if defined(_WIN32)
        m_pid = _spawnv(P_NOWAIT, JAVA_EXECUTABLE, &argv[0]);
#else
        if (0 != posix_spawn(&m_pid, JAVA_EXECUTABLE, nullptr, nullptr,
                             const_cast<char *const *>(&argv[0]), nullptr))
        {
            perror("spawn");
            ::exit(EXIT_FAILURE);
        }
#endif

        /* Wait for archive mark file to indicate the process is ready */
        const std::string mark_file = m_aeron_dir + "/archive-mark.dat";
        for (int spin = 0; spin < 30000; spin++)  /* up to 30 seconds */
        {
            int64_t file_length = aeron_file_length(mark_file.c_str());
            if (file_length >= CLUSTER_ARCHIVE_MARK_FILE_HEADER_LENGTH) { break; }
            aeron_micro_sleep(1000);
        }

        m_stream << "[Node " << m_node_index << "] ArchivingMediaDriver PID " << m_pid << " ready" << std::endl;
    }

    void stop_archiving_media_driver()
    {
        if (m_pid > 0)
        {
            m_stream << "[Node " << m_node_index << "] Stopping ArchivingMediaDriver PID " << m_pid << std::endl;
#ifndef _WIN32
            kill(m_pid, SIGTERM);
            int status = 0;
            waitpid(m_pid, &status, 0);
#endif
            m_pid = -1;

            /* Brief sleep to allow OS to release the archive port */
            aeron_micro_sleep(300000);  /* 300ms */
        }

        /* Clean up directories */
        std::system(("rm -rf " + m_aeron_dir).c_str());
        std::system(("rm -rf " + m_archive_dir).c_str());
        std::system(("rm -rf " + m_cluster_dir).c_str());
    }

    int          m_node_index;
    std::string  m_aeron_dir;
    std::string  m_archive_dir;
    std::string  m_cluster_dir;
    std::string  m_cluster_members;
    std::string  m_ingress_endpoints;
    pid_t        m_pid = -1;
    std::ostream &m_stream;
};

#endif /* AERON_TEST_CLUSTER_NODE_H */
