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

#ifndef AERON_TEST_PROCESS_UTILS_H
#define AERON_TEST_PROCESS_UTILS_H

#include <atomic>
#include <cstdlib>

extern "C"
{
#include <signal.h>
#include "aeronc.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_fileutil.h"
}

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

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
#include <sys/wait.h>
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

// Best-effort cleanup of spawned child processes on signal-driven exit.
//
// On CTest timeout the test binary receives SIGTERM (then SIGKILL after a grace period).
// Without a handler our destructors never run, leaving Java archive children holding their
// TCP ports — notably the archive control channel — which breaks subsequent suites that
// bind the same port. TestArchive / TestStandaloneArchive register/unregister their PIDs;
// the SIGTERM handler SIGKILLs any still-live PIDs and re-raises with the default
// disposition. SIGKILL itself cannot be caught, so this is mitigation, not a guarantee.

static constexpr size_t AERON_TEST_PROCESS_REGISTRY_SIZE = 16;

inline std::atomic<pid_t> *aeron_test_process_registry()
{
    static std::atomic<pid_t> pids[AERON_TEST_PROCESS_REGISTRY_SIZE]{};
    return pids;
}

inline void aeron_test_kill_registered_processes()
{
    std::atomic<pid_t> *pids = aeron_test_process_registry();
    for (size_t i = 0; i < AERON_TEST_PROCESS_REGISTRY_SIZE; i++)
    {
        pid_t pid = pids[i].exchange(0);
        if (pid > 0)
        {
#ifdef _WIN32
            HANDLE h = OpenProcess(PROCESS_TERMINATE, FALSE, static_cast<DWORD>(pid));
            if (h != NULL)
            {
                TerminateProcess(h, 0);
                CloseHandle(h);
            }
#else
            kill(pid, SIGKILL);
#endif
        }
    }
}

#ifndef _WIN32
inline void aeron_test_signal_cleanup_handler(int sig)
{
    aeron_test_kill_registered_processes();
    // Restore default disposition and re-raise so CTest sees the expected signal exit status.
    struct sigaction sa{};
    sa.sa_handler = SIG_DFL;
    sigemptyset(&sa.sa_mask);
    sigaction(sig, &sa, nullptr);
    raise(sig);
}
#endif

inline void aeron_test_install_cleanup_handlers_once()
{
    static std::atomic<bool> installed{false};
    bool expected = false;
    if (!installed.compare_exchange_strong(expected, true))
    {
        return;
    }
    std::atexit(aeron_test_kill_registered_processes);
#ifndef _WIN32
    struct sigaction sa{};
    sa.sa_handler = aeron_test_signal_cleanup_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESETHAND;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGHUP, &sa, nullptr);
#endif
}

inline void aeron_test_register_spawned_process(pid_t pid)
{
    if (pid <= 0)
    {
        return;
    }
    aeron_test_install_cleanup_handlers_once();
    std::atomic<pid_t> *pids = aeron_test_process_registry();
    for (size_t i = 0; i < AERON_TEST_PROCESS_REGISTRY_SIZE; i++)
    {
        pid_t expected = 0;
        if (pids[i].compare_exchange_strong(expected, pid))
        {
            return;
        }
    }
    // Registry full — drop silently. The PID loses its safety net but the test still runs.
}

inline void aeron_test_unregister_spawned_process(pid_t pid)
{
    if (pid <= 0)
    {
        return;
    }
    std::atomic<pid_t> *pids = aeron_test_process_registry();
    for (size_t i = 0; i < AERON_TEST_PROCESS_REGISTRY_SIZE; i++)
    {
        pid_t expected = pid;
        if (pids[i].compare_exchange_strong(expected, 0))
        {
            return;
        }
    }
}

#endif //AERON_TEST_PROCESS_UTILS_H
