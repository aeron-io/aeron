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

#ifndef AERON_TESTARCHIVE_H
#define AERON_TESTARCHIVE_H

// Uncomment for logging
// #define ENABLE_AGENT_DEBUG_LOGGING 1

extern "C"
{
#include <atomic>
#include <signal.h>
}

#include <thread>

#define TERM_LENGTH AERON_LOGBUFFER_TERM_MIN_LENGTH
#define SEGMENT_LENGTH (TERM_LENGTH * 2)
#define ARCHIVE_MARK_FILE_HEADER_LENGTH (8192)

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <shellapi.h>
typedef intptr_t pid_t;

static void await_process_terminated(pid_t process_handle)
{
    WaitForSingleObject(reinterpret_cast<HANDLE>(process_handle), INFINITE);
}
#else
#include "ftw.h"
#include "spawn.h"

static void await_process_terminated(pid_t process_handle)
{
    int process_status = -1;
    while (true)
    {
        waitpid(process_handle, &process_status, WUNTRACED);
        if (WIFSIGNALED(process_status) || WIFEXITED(process_status))
        {
            break;
        }
    }
}
#endif

class TestArchive
{
public:
    TestArchive(
        std::string aeronDir,
        std::string archiveDir,
        std::ostream &stream,
        std::string controlChannel,
        std::string replicationChannel,
        std::int64_t archiveId,
        std::int32_t maxConcurrentReplays = 100)
        : m_archiveDir(archiveDir), m_aeronDir(aeronDir), m_stream(stream)
    {
        m_stream << aeron_epoch_clock() << " [SetUp] Starting ArchivingMediaDriver..." << std::endl;

        std::string aeronDirArg = "-Daeron.dir=" + aeronDir;
        std::string archiveDirArg = "-Daeron.archive.dir=" + archiveDir;
        std::string archiveMarkFileDirArg = "-Daeron.archive.mark.file.dir=" + aeronDir;
        m_stream << aeron_epoch_clock() << " [SetUp] " << aeronDirArg << std::endl;
        m_stream << aeron_epoch_clock() << " [SetUp] " << archiveDirArg << std::endl;
        std::string controlChannelArg = "-Daeron.archive.control.channel=" + controlChannel;
        std::string replicationChannelArg = "-Daeron.archive.replication.channel=" + replicationChannel;
        std::string archiveIdArg = "-Daeron.archive.id=" + std::to_string(archiveId);
        std::string segmentLength = "-Daeron.archive.segment.file.length=" + std::to_string(SEGMENT_LENGTH);
        std::string maxConcurrentReplaysArg = "-Daeron.archive.max.concurrent.replays=" + std::to_string(maxConcurrentReplays);

        const char *const argv[] =
        {
            "java",
            "--add-opens",
            "java.base/jdk.internal.misc=ALL-UNNAMED",
            "--add-opens",
            "java.base/java.util.zip=ALL-UNNAMED",
#if ENABLE_AGENT_DEBUG_LOGGING
            m_aeronAgentJar.c_str(),
            "-Daeron.event.log=admin",
            "-Daeron.event.archive.log=all",
#endif
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
            "-Daeron.archive.authenticator.supplier=io.aeron.samples.archive.SampleAuthenticatorSupplier",
            "-Daeron.enable.experimental.features=true",
            "-Daeron.spies.simulate.connection=true",
            maxConcurrentReplaysArg.c_str(),
            segmentLength.c_str(),
            archiveIdArg.c_str(),
            controlChannelArg.c_str(),
            replicationChannelArg.c_str(),
            "-Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0",
            archiveDirArg.c_str(),
            archiveMarkFileDirArg.c_str(),
            aeronDirArg.c_str(),
            "-cp",
            m_aeronAllJar.c_str(),
            "io.aeron.archive.ArchivingMediaDriver",
            nullptr
        };
        m_process_handle = -1;

#if defined(_WIN32)
        m_process_handle = _spawnv(P_NOWAIT, m_java.c_str(), &argv[0]);
#else
        if (0 != posix_spawn(&m_process_handle, m_java.c_str(), nullptr, nullptr, (char *const *)&argv[0], nullptr))
        {
            perror("spawn");
            ::exit(EXIT_FAILURE);
        }
#endif

        if (m_process_handle < 0)
        {
            perror("spawn");
            ::exit(EXIT_FAILURE);
        }

        m_pid = m_process_handle;
#ifdef _WIN32
        m_pid = GetProcessId((HANDLE)m_process_handle);
#endif

        const std::string mark_file = aeronDir + std::string(1, AERON_FILE_SEP) + "archive-mark.dat";

        // await mark file creation as an indicator that Archive process is running
        while (true)
        {
            int64_t file_length = aeron_file_length(mark_file.c_str());
            if (file_length >= ARCHIVE_MARK_FILE_HEADER_LENGTH)
            {
                break;
            }
            aeron_micro_sleep(1000);
        }
        m_stream << aeron_epoch_clock() << " [SetUp] ArchivingMediaDriver PID " << m_pid << std::endl;
    }

    ~TestArchive()
    {
        if (m_process_handle > 0)
        {
            m_stream << aeron_epoch_clock() << " [TearDown] Shutting down ArchivingMediaDriver PID " << m_pid << std::endl;

            bool archive_terminated = false;
#ifndef _WIN32
            if (0 == kill(m_process_handle, SIGTERM))
            {
                m_stream << aeron_epoch_clock() << " [TearDown] waiting for ArchivingMediaDriver termination..." << std::endl;
                await_process_terminated(m_process_handle);
                m_stream << aeron_epoch_clock() << " [TearDown] ArchivingMediaDriver terminated" << std::endl;
                archive_terminated = true;
            }
#endif

            if (!archive_terminated)
            {
                const std::string aeronPath = m_aeronDir;
                const std::string cncFilename = aeronPath + std::string(1, AERON_FILE_SEP) + "cnc.dat";

                if (aeron_context_request_driver_termination(aeronPath.c_str(), nullptr, 0))
                {
                    m_stream << aeron_epoch_clock() << " [TearDown] Waiting for driver termination" << std::endl;

                    while (aeron_file_length(cncFilename.c_str()) > 0)
                    {
                        aeron_micro_sleep(1000);
                    }

                    m_stream << aeron_epoch_clock() << " [TearDown] CnC file no longer exists" << std::endl;

                    await_process_terminated(m_process_handle);
                    m_stream << aeron_epoch_clock() << " [TearDown] Driver terminated" << std::endl;
                    archive_terminated = true;
                }
                else
                {
                    m_stream << aeron_epoch_clock() << " [TearDown] Failed to send driver terminate command" << std::endl;
                }
            }

            if (archive_terminated && aeron_is_directory(m_archiveDir.c_str()) >= 0)
            {
                m_stream << aeron_epoch_clock() << " [TearDown] Deleting " << m_archiveDir << std::endl;
                if (aeron_delete_directory(m_archiveDir.c_str()) != 0)
                {
                    m_stream << aeron_epoch_clock() << " [TearDown] Failed to delete " << m_archiveDir << std::endl;
                }
            }
            m_stream.flush();
        }
    }

private:
    const std::string m_java = JAVA_EXECUTABLE;          // Defined in CMakeLists.txt
    const std::string m_aeronAllJar = AERON_ALL_JAR;     // Defined in CMakeLists.txt
    const std::string m_aeronAgentJar = "-javaagent:" AERON_AGENT_JAR; // Defined in CMakeLists.txt
    const std::string m_archiveDir;
    const std::string m_aeronDir;
    std::ostream &m_stream;
    pid_t m_process_handle = -1;
    pid_t m_pid = 0;
};

#endif //AERON_TESTARCHIVE_H
