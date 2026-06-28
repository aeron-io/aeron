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

#include <atomic>
#include <cstring>
#include <iostream>
#include <string>
#include "TestProcessUtils.h"

extern "C"
{
#include "aeron_counters.h"
}

class TestStandaloneArchive
{
public:
    TestStandaloneArchive(
        std::string aeronDir,
        std::string archiveDir,
        std::ostream &stream,
        std::string controlChannel,
        std::string replicationChannel,
        bool deleteOnStart = true)
        : TestStandaloneArchive(
            std::move(aeronDir), std::move(archiveDir), stream,
            std::move(controlChannel), std::move(replicationChannel),
            nextArchiveId(), deleteOnStart, false /* explicit_id */)
    {
    }

    TestStandaloneArchive(
        std::string aeronDir,
        std::string archiveDir,
        std::ostream &stream,
        std::string controlChannel,
        std::string replicationChannel,
        std::int64_t archiveId,
        bool deleteOnStart)
        : TestStandaloneArchive(
            std::move(aeronDir), std::move(archiveDir), stream,
            std::move(controlChannel), std::move(replicationChannel),
            archiveId, deleteOnStart, true /* explicit_id */)
    {
    }

    // Reading archiveId() implies the caller may reuse the id on a successor archive
    // (the kill+restart pattern). Mark this instance so its destructor waits for the
    // driver to reap the error counter before returning, so the next archive starting
    // with the same id does not get rejected with "found existing archive for archiveId=".
    std::int64_t archiveId()
    {
#ifdef _WIN32
        m_idMayBeReused = true;
#endif
        return m_archiveId;
    }

private:
    static std::int64_t nextArchiveId()
    {
        static std::atomic<std::int64_t> s_next{10000};
        return s_next.fetch_add(1);
    }

    TestStandaloneArchive(
        std::string aeronDir,
        std::string archiveDir,
        std::ostream &stream,
        std::string controlChannel,
        std::string replicationChannel,
        std::int64_t archiveId,
        bool deleteOnStart,
        bool explicitId)
        : m_archiveDir(std::move(archiveDir)), m_aeronDir(std::move(aeronDir)),
          m_stream(stream)
#ifdef _WIN32
        , m_explicitId(explicitId)
#endif
    {
#ifndef _WIN32
        (void)explicitId;  // m_explicitId is Windows-only; unused on POSIX builds
#endif
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
        aeron_test_register_spawned_process(m_process_handle);
        m_archiveId = archiveId;

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

public:
    ~TestStandaloneArchive()
    {
        if (m_process_handle > 0)
        {
            aeron_test_unregister_spawned_process(m_process_handle);
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
            // Windows has no SIGTERM equivalent the JVM turns into a graceful shutdown for an
            // externally-launched archive (SIGBREAK is reserved by the JVM; CTRL_C_EVENT can't
            // be targeted at a single process group). Kill hard and then manually perform the
            // two cleanups the JVM's shutdown hooks would have done on Unix:
            //   1. Remove the stale archive-mark.dat (its liveness timestamp is still recent).
            //   2. Wait for the driver to detect the dead Aeron client and free the archive's
            //      error counter - otherwise the next archive startup with the same archiveId
            //      throws "found existing archive for archiveId=".
            TerminateProcess(reinterpret_cast<HANDLE>(m_process_handle), 0);
            await_process_terminated(m_process_handle);
            m_stream << aeron_epoch_clock() << " [TearDown] Archive terminated" << std::endl;

            const std::string mark_file_to_remove =
                m_aeronDir + std::string(1, AERON_FILE_SEP) + "archive-mark.dat";
            aeron_delete_file(mark_file_to_remove.c_str());

            // Wait for the driver to reap this archive's error counter when the id may
            // be reused: explicit-id construction (kill+restart with hardcoded id), or an
            // auto-id instance whose archiveId() was queried by the test (so a successor
            // can be constructed with the same id). Without the wait, the next archive
            // starting with the same id would be rejected with "found existing archive for
            // archiveId=".
            if (m_explicitId || m_idMayBeReused)
            {
                awaitArchiveCountersFreed();
            }
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
    std::int64_t m_archiveId = 0;
    bool m_deleteDirOnTearDown = true;
#ifdef _WIN32
    bool m_explicitId = false;
    bool m_idMayBeReused = false;
#endif

#ifdef _WIN32
    // Wait for the driver to reap the dead archive's client and free its counters
    // (matches the immediate cleanup that the JVM's shutdown hooks perform on Unix).
    // Without this, a subsequent archive startup with the same archiveId throws
    // "found existing archive for archiveId=" because the ghost error counter is
    // still in the driver's counter buffer.
    void awaitArchiveCountersFreed()
    {
        m_stream << aeron_epoch_clock()
            << " [TearDown] Waiting for driver to free archive counters..." << std::endl;

        aeron_context_t *ctx = nullptr;
        aeron_t *aeron = nullptr;
        if (aeron_context_init(&ctx) < 0)
        {
            m_stream << " [TearDown] aeron_context_init failed: " << aeron_errmsg() << std::endl;
            return;
        }
        aeron_context_set_dir(ctx, m_aeronDir.c_str());
        if (aeron_init(&aeron, ctx) < 0 || aeron_start(aeron) < 0)
        {
            m_stream << " [TearDown] aeron_init/start failed: " << aeron_errmsg() << std::endl;
            aeron_context_close(ctx);
            return;
        }

        aeron_counters_reader_t *reader = aeron_counters_reader(aeron);
        struct FindState { std::int64_t archiveId; bool found; } state{ m_archiveId, false };

        const uint64_t deadline_ms = aeron_epoch_clock() + 15000;
        while (true)
        {
            state.found = false;
            aeron_counters_reader_foreach_counter(
                reader,
                [](int64_t, int32_t, int32_t type_id,
                    const uint8_t *key, size_t key_length,
                    const char *, size_t, void *clientd)
                {
                    auto *s = static_cast<FindState *>(clientd);
                    if (AERON_COUNTER_ARCHIVE_ERROR_COUNT_TYPE_ID == type_id &&
                        key_length >= sizeof(std::int64_t))
                    {
                        std::int64_t id;
                        std::memcpy(&id, key, sizeof(id));
                        if (id == s->archiveId)
                        {
                            s->found = true;
                        }
                    }
                },
                &state);

            if (!state.found)
            {
                m_stream << aeron_epoch_clock() << " [TearDown] Archive counters freed" << std::endl;
                break;
            }
            if (aeron_epoch_clock() >= deadline_ms)
            {
                m_stream << aeron_epoch_clock()
                    << " [TearDown] Timed out waiting for archive counters to be freed" << std::endl;
                break;
            }
            aeron_micro_sleep(100 * 1000);
        }

        aeron_close(aeron);
        aeron_context_close(ctx);
    }
#endif
};

#endif //AERON_TEST_STANDALONE_ARCHIVE_H
