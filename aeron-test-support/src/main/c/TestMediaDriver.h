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

#ifndef AERON_TEST_MEDIA_DRIVER_H
#define AERON_TEST_MEDIA_DRIVER_H

#include <iostream>
#include <string>
#include "TestProcessUtils.h"

class TestMediaDriver
{
public:
    TestMediaDriver(
        std::string aeronDir,
        std::ostream &stream)
        : m_aeronDir(std::move(aeronDir)), m_stream(stream)
    {
        m_stream << aeron_epoch_clock() << " [SetUp] Starting standalone MediaDriver..." << std::endl;

        std::string aeronDirArg = "-Daeron.dir=" + m_aeronDir;
        m_stream << aeron_epoch_clock() << " [SetUp] " << aeronDirArg << std::endl;

        const char *const argv[] =
            {
                "java",
                "--add-opens",
                "java.base/jdk.internal.misc=ALL-UNNAMED",
                "--add-opens",
                "java.base/java.util.zip=ALL-UNNAMED",
                "-Daeron.dir.delete.on.start=true",
                "-Daeron.dir.delete.on.shutdown=true",
                "-Daeron.term.buffer.sparse.file=true",
                "-Daeron.perform.storage.checks=false",
                "-Daeron.term.buffer.length=64k",
                "-Daeron.ipc.term.buffer.length=64k",
                "-Daeron.threading.mode=SHARED",
                "-Daeron.shared.idle.strategy=yield",
                "-Daeron.driver.termination.validator=io.aeron.driver.DefaultAllowTerminationValidator",
                "-Daeron.enable.experimental.features=true",
                "-Daeron.spies.simulate.connection=true",
                aeronDirArg.c_str(),
                "-cp",
                m_aeronAllJar.c_str(),
                "io.aeron.driver.MediaDriver",
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

        const std::string cncFile = m_aeronDir + std::string(1, AERON_FILE_SEP) + "cnc.dat";

        while (true)
        {
            int64_t file_length = aeron_file_length(cncFile.c_str());
            if (file_length > 0)
            {
                break;
            }
            aeron_micro_sleep(1000);
        }
        m_stream << aeron_epoch_clock() << " [SetUp] MediaDriver PID " << m_pid << std::endl;
    }

    ~TestMediaDriver()
    {
        if (m_process_handle > 0)
        {
            m_stream << aeron_epoch_clock() << " [TearDown] Shutting down MediaDriver PID " << m_pid << std::endl;

            bool terminated = false;
#ifndef _WIN32
            if (0 == kill(m_process_handle, SIGTERM))
            {
                m_stream << aeron_epoch_clock() << " [TearDown] waiting for MediaDriver termination..." << std::endl;
                await_process_terminated(m_process_handle);
                m_stream << aeron_epoch_clock() << " [TearDown] MediaDriver terminated" << std::endl;
                terminated = true;
            }
#endif

            if (!terminated)
            {
                if (aeron_context_request_driver_termination(m_aeronDir.c_str(), nullptr, 0))
                {
                    const std::string cncFile = m_aeronDir + std::string(1, AERON_FILE_SEP) + "cnc.dat";
                    m_stream << aeron_epoch_clock() << " [TearDown] Waiting for driver termination" << std::endl;

                    while (aeron_file_length(cncFile.c_str()) > 0)
                    {
                        aeron_micro_sleep(1000);
                    }

                    await_process_terminated(m_process_handle);
                    m_stream << aeron_epoch_clock() << " [TearDown] MediaDriver terminated" << std::endl;
                }
                else
                {
                    m_stream << aeron_epoch_clock() << " [TearDown] Failed to send driver terminate command" << std::endl;
                }
            }
            m_stream.flush();
        }
    }

private:
    const std::string m_java = JAVA_EXECUTABLE;
    const std::string m_aeronAllJar = AERON_ALL_JAR;
    const std::string m_aeronDir;
    std::ostream &m_stream;
    pid_t m_process_handle = -1;
    pid_t m_pid = 0;
};

#endif //AERON_TEST_MEDIA_DRIVER_H
