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

#ifndef AERON_TEST_STANDALONE_ARCHIVE_H
#define AERON_TEST_STANDALONE_ARCHIVE_H

#include <iostream>
#include <string>
#include "TestProcessUtils.h"

class TestStandaloneArchive
{
public:
    TestStandaloneArchive(
        std::string aeronDir,
        std::string archiveDir,
        std::ostream &stream,
        std::string controlChannel,
        std::string replicationChannel,
        std::int64_t archiveId,
        bool deleteOnStart = true)
        : m_archiveDir(std::move(archiveDir)), m_aeronDir(std::move(aeronDir)), m_stream(stream)
    {
        m_stream << aeron_epoch_clock() << " [SetUp] Starting standalone Archive..." << std::endl;

        std::string aeronDirArg = "-Daeron.dir=" + m_aeronDir;
        std::string archiveDirArg = "-Daeron.archive.dir=" + m_archiveDir;
        std::string archiveMarkFileDirArg = "-Daeron.archive.mark.file.dir=" + m_aeronDir;
        m_stream << aeron_epoch_clock() << " [SetUp] " << aeronDirArg << std::endl;
        m_stream << aeron_epoch_clock() << " [SetUp] " << archiveDirArg << std::endl;
        std::string controlChannelArg = "-Daeron.archive.control.channel=" + controlChannel;
        std::string replicationChannelArg = "-Daeron.archive.replication.channel=" + replicationChannel;
        std::string archiveIdArg = "-Daeron.archive.id=" + std::to_string(archiveId);
        std::string segmentLength = "-Daeron.archive.segment.file.length=" + std::to_string(SEGMENT_LENGTH);
        std::string deleteOnStartArg = std::string("-Daeron.archive.dir.delete.on.start=") + (deleteOnStart ? "true" : "false");

        const char *const argv[] =
            {
                "java",
                "--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.util.zip=ALL-UNNAMED",
                "-Daeron.archive.max.catalog.entries=128",
                "-Daeron.archive.threading.mode=SHARED",
                "-Daeron.archive.idle.strategy=yield",
                "-Daeron.archive.recording.events.enabled=false",
                "-Daeron.archive.authenticator.supplier=io.aeron.samples.archive.SampleAuthenticatorSupplier",
                "-Daeron.archive.control.response.channel=aeron:udp?endpoint=localhost:0",
                deleteOnStartArg.c_str(),
                segmentLength.c_str(),
                archiveIdArg.c_str(),
                controlChannelArg.c_str(),
                replicationChannelArg.c_str(),
                archiveDirArg.c_str(),
                archiveMarkFileDirArg.c_str(),
                aeronDirArg.c_str(),
                "-cp",
                m_aeronAllJar.c_str(),
                "io.aeron.archive.Archive",
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

        const std::string mark_file = m_aeronDir + std::string(1, AERON_FILE_SEP) + "archive-mark.dat";

        while (true)
        {
            int64_t file_length = aeron_file_length(mark_file.c_str());
            if (file_length >= ARCHIVE_MARK_FILE_HEADER_LENGTH)
            {
                break;
            }
            aeron_micro_sleep(1000);
        }
        m_stream << aeron_epoch_clock() << " [SetUp] Archive PID " << m_pid << std::endl;
    }

    ~TestStandaloneArchive()
    {
        if (m_process_handle > 0)
        {
            m_stream << aeron_epoch_clock() << " [TearDown] Shutting down Archive PID " << m_pid << std::endl;

#ifndef _WIN32
            if (0 == kill(m_process_handle, SIGTERM))
            {
                m_stream << aeron_epoch_clock() << " [TearDown] waiting for Archive termination..." << std::endl;
                await_process_terminated(m_process_handle);
                m_stream << aeron_epoch_clock() << " [TearDown] Archive terminated" << std::endl;
            }
            else
            {
                m_stream << aeron_epoch_clock() << " [TearDown] Failed to send SIGTERM to Archive" << std::endl;
            }
#else
            TerminateProcess(reinterpret_cast<HANDLE>(m_process_handle), 0);
            await_process_terminated(m_process_handle);
            m_stream << aeron_epoch_clock() << " [TearDown] Archive terminated" << std::endl;

            // TerminateProcess skips the JVM's shutdown hooks, so the 'archive-mark.dat' mark file
            // does not get cleaned up like it does on Unix, which causes tests that start the archive
            // back up quickly to fail on Windows. Ideally we should support graceeful archive shtudown
            // on Windows as well; this is here as a temporary fix.
            const std::string mark_file_to_remove =
                m_aeronDir + std::string(1, AERON_FILE_SEP) + "archive-mark.dat";
            aeron_delete_file(mark_file_to_remove.c_str());
#endif

            if (m_deleteDirOnTearDown && aeron_is_directory(m_archiveDir.c_str()) >= 0)
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

    void deleteDirOnTearDown(const bool deleteDirOnTearDown)
    {
        m_deleteDirOnTearDown = deleteDirOnTearDown;
    }

private:
    const std::string m_java = JAVA_EXECUTABLE;
    const std::string m_aeronAllJar = AERON_ALL_JAR;
    const std::string m_archiveDir;
    const std::string m_aeronDir;
    std::ostream &m_stream;
    pid_t m_process_handle = -1;
    pid_t m_pid = 0;
    bool m_deleteDirOnTearDown = true;
};

#endif //AERON_TEST_STANDALONE_ARCHIVE_H
