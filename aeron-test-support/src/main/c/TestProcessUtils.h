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

#endif //AERON_TEST_PROCESS_UTILS_H
