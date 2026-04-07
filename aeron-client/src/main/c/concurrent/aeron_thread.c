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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>
#include <inttypes.h>
#include <stdlib.h>
#include <assert.h>
#include "aeron_alloc.h"
#include "concurrent/aeron_thread.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"

#if !defined(_WIN32)
#include <unistd.h>
#else
#include <Windows.h>
#include <mmsystem.h>
#pragma comment(lib, "winmm.lib")

struct aeron_thread_stct
{
    HANDLE handle;
    void *(*callback)(void *);
    void *arg0;
    void *result;
};

#endif

#define SECOND_AS_NANOSECONDS (1000 * 1000 * 1000LL)

void aeron_nano_sleep(uint64_t nanoseconds)
{
#ifdef AERON_COMPILER_MSVC
    timeBeginPeriod(1);

    HANDLE timer = CreateWaitableTimer(NULL, TRUE, NULL);
    if (!timer)
    {
        goto cleanup;
    }

    LARGE_INTEGER li;
    li.QuadPart = -(int64_t)(nanoseconds / 100);

    if (!SetWaitableTimer(timer, &li, 0, NULL, NULL, FALSE))
    {
        CloseHandle(timer);
        goto cleanup;
    }

    WaitForSingleObject(timer, INFINITE);
    CloseHandle(timer);

cleanup:
    timeEndPeriod(1);
#else
    time_t seconds = nanoseconds / SECOND_AS_NANOSECONDS;
    struct timespec ts =
    {
        .tv_sec = seconds,
        .tv_nsec = (long)nanoseconds - (seconds * SECOND_AS_NANOSECONDS)
    };

    nanosleep(&ts, NULL);
#endif
}

void aeron_micro_sleep(unsigned int microseconds)
{
#ifdef _WIN32
    aeron_nano_sleep(UINT64_C(1000) * microseconds);
#else
    usleep(microseconds);
#endif
}

int aeron_thread_set_affinity(const char *name, uint8_t cpu_affinity_no)
{
#if defined(__linux__)
    cpu_set_t mask;
    const size_t size = sizeof(mask);
    CPU_ZERO(&mask);
    CPU_SET(cpu_affinity_no, &mask);
    if (sched_setaffinity(0, size, &mask) < 0)
    {
        AERON_SET_ERR(errno, "failed to set thread affinity name=%s, cpu_affinity_no=%" PRIu8, name, cpu_affinity_no);
        return -1;
    }
    return 0;
#else
    AERON_SET_ERR(EINVAL, "%s", "thread affinity not supported");
    return -1;
#endif
}

#if defined(AERON_COMPILER_GCC)

int aeron_thread_set_name(const char *name)
{
    char thread_name[AERON_THREAD_NAME_MAX_LENGTH + 1];
    size_t copy_len = AERON_MIN(strlen(name), AERON_THREAD_NAME_MAX_LENGTH);
    memcpy(thread_name, name, copy_len);
    thread_name[copy_len] = '\0';

    int rc;
#if defined(__APPLE__)
    rc = pthread_setname_np(thread_name);
#else
    rc = pthread_setname_np(pthread_self(), thread_name);
#endif
    if (0 != rc)
    {
        AERON_SET_ERR(-rc, "%s", "pthread_setname_np failed");
        return -1;
    }
    return 0;
}

int aeron_thread_get_name(char *name_buf, size_t name_buf_size)
{
    if (NULL == name_buf)
    {
        AERON_SET_ERR(EINVAL, "%s", "name_buf is NULL");
        return -1;
    }

    if (name_buf_size <= AERON_THREAD_NAME_MAX_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "name_buf is too small: %" PRIu64, (uint64_t)name_buf_size);
        return -1;
    }

    return pthread_getname_np(pthread_self(), name_buf, name_buf_size);
}

int aeron_mutex_init(aeron_mutex_t *mutex)
{
    pthread_mutexattr_t mutex_attr;
    int rc = pthread_mutexattr_init(&mutex_attr);
    if (0 != rc)
    {
        AERON_SET_ERR(rc, "%s", "pthread_mutexattr_init failed");
        return -1;
    }

    rc = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
    if (0 != rc)
    {
        AERON_SET_ERR(rc, "%s", "pthread_mutexattr_settype failed");
        goto error;
    }

    rc = pthread_mutex_init(mutex, &mutex_attr);
    if (0 != rc)
    {
        AERON_SET_ERR(rc, "%s", "failed to create mutex");
        goto error;
    }

    pthread_mutexattr_destroy(&mutex_attr);
    return 0;

error:
    pthread_mutexattr_destroy(&mutex_attr);
    return -1;
}

int aeron_mutex_destroy(aeron_mutex_t *mutex)
{
    return pthread_mutex_destroy(mutex);
}

int aeron_mutex_lock(aeron_mutex_t *mutex)
{
    return pthread_mutex_lock(mutex);
}

int aeron_mutex_unlock(aeron_mutex_t *mutex)
{
    return pthread_mutex_unlock(mutex);
}

#elif defined(AERON_COMPILER_MSVC)

static BOOL WINAPI aeron_thread_once_callback(PINIT_ONCE init_once, void (*callback)(void), void **context)
{
    callback();
    return TRUE;
}

void aeron_thread_once(AERON_INIT_ONCE *s_init_once, void *callback)
{
    InitOnceExecuteOnce((PINIT_ONCE)s_init_once, (PINIT_ONCE_FN)aeron_thread_once_callback, callback, NULL);
}

int aeron_mutex_init(aeron_mutex_t *mutex)
{
    InitializeCriticalSection(mutex);
    return 0;
}

int aeron_mutex_lock(aeron_mutex_t *mutex)
{
    EnterCriticalSection(mutex);
    return 0;
}

int aeron_mutex_unlock(aeron_mutex_t *mutex)
{
    LeaveCriticalSection(mutex);
    return 0;
}

int aeron_mutex_destroy(aeron_mutex_t *mutex)
{
    if (mutex)
    {
        DeleteCriticalSection(mutex);
    }

    return 0;
}

int aeron_thread_attr_init(aeron_thread_attr_t *attr)
{
    return 0;
}

static DWORD WINAPI aeron_thread_proc(LPVOID parameter)
{
    aeron_thread_t *thread = (aeron_thread_t *)parameter;
    (*thread)->result = (*thread)->callback((*thread)->arg0);

    return 0;
}

int aeron_thread_create(aeron_thread_t *thread_ptr, void *attr, void *(*callback)(void *), void *arg0)
{
    if (NULL == thread_ptr)
    {
        AERON_SET_ERR_WIN(EINVAL, "%s", "aeron_thread_t is NULL");
        return -1;
    }

    if (aeron_alloc((void **)thread_ptr, sizeof(struct aeron_thread_stct)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate memory for aeron_thread_t");
        return -1;
    }

    (*thread_ptr)->callback = callback;
    (*thread_ptr)->arg0 = arg0;
    DWORD id;

    (*thread_ptr)->handle = CreateThread(
        NULL,                 // default security attributes
        0,                    // use default stack size
        aeron_thread_proc,    // thread function name
        thread_ptr,           // argument to thread function
        0,                    // use default creation flags
        &id);                 // returns the thread identifier

    if (!(*thread_ptr)->handle)
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "CreateThread failed");
        aeron_free(*thread_ptr);
        return -1;
    }

    return 0;
}

int aeron_thread_set_name(const char *name)
{
    char thread_name[AERON_THREAD_NAME_MAX_LENGTH + 1];
    size_t copy_len = AERON_MIN(strlen(name), AERON_THREAD_NAME_MAX_LENGTH);
    memcpy(thread_name, name, copy_len);
    thread_name[copy_len] = '\0';

    size_t wchar_count;
    if (0 != mbstowcs_s(&wchar_count, NULL, 0, thread_name, 0))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "mbstowcs_s failed");
        return -1;
    }

    wchar_t *buf;
    if (aeron_alloc((void **)&buf, sizeof(wchar_t) * (wchar_count + 1)) < 0)  // value-initialize to 0 (see below)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate wchar_t buffer");
        return -1;
    }

    if (0 != mbstowcs_s(NULL, buf, wchar_count + 1, thread_name, copy_len))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "mbstowcs_s failed");
        goto error;
    }

    HRESULT hr = SetThreadDescription(GetCurrentThread(), buf);
    if (FAILED(hr))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "SetThreadDescription failed");
        goto error;
    }

    aeron_free(buf);
    return 0;

error:
    aeron_free(buf);
    return -1;
}

int aeron_thread_get_name(char *name_buf, size_t name_buf_size)
{
    if (NULL == name_buf)
    {
        AERON_SET_ERR(EINVAL, "%s", "name_buf is NULL");
        return -1;
    }

    if (name_buf_size <= AERON_THREAD_NAME_MAX_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "name_buf is too small: %" PRIu64, (uint64_t)name_buf_size);
        return -1;
    }

    wchar_t *buf;
    HRESULT hr = GetThreadDescription(GetCurrentThread(), &buf);
    if (FAILED(hr))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "GetThreadDescription failed");
        return -1;
    }

    size_t size;
    if (0 != wcstombs_s(&size, name_buf, name_buf_size, buf, _TRUNCATE))
    {
        AERON_SET_ERR_WIN(GetLastError(), "%s", "wcstombs_s failed");
        LocalFree(buf);
        return -1;
    }

    LocalFree(buf);
    return 0;
}

int aeron_thread_join(aeron_thread_t thread, void **value_ptr)
{
    if (!thread)
    {
        return EINVAL;
    }

    int result = 0;
    if (thread->handle)
    {
        WaitForSingleObject(thread->handle, INFINITE);
        CloseHandle(thread->handle);
        if (value_ptr)
        {
            *value_ptr = thread->result;
        }
    }
    else
    {
        result = EINVAL;
    }

    aeron_free(thread);

    return result;
}

int aeron_thread_key_create(pthread_key_t *key_ptr, void (*destr_func)(void *))
{
    DWORD dkey = TlsAlloc();
    if (dkey != TLS_OUT_OF_INDEXES)
    {
        *key_ptr = dkey;
        return 0;
    }
    else
    {
        return EAGAIN;
    }
}

int aeron_thread_key_delete(pthread_key_t key)
{
    if (TlsFree(key))
    {
        return 0;
    }
    else
    {
        return EINVAL;
    }
}

int aeron_thread_set_specific(pthread_key_t key, const void *pointer)
{
    if (TlsSetValue(key, (LPVOID)pointer))
    {
        return 0;
    }
    else
    {
        return EINVAL;
    }
}

void *aeron_thread_get_specific(pthread_key_t key)
{
    return TlsGetValue(key);
}

int sched_yield(void)
{
    SwitchToThread();
    return 0;
}

int aeron_cond_init(aeron_cond_t *cv, void *attr)
{
    InitializeConditionVariable(cv);
    return 0;
}

int aeron_cond_destroy(aeron_cond_t *cv)
{
    // there's no delete for windows condition variables
    return 0;
}

int aeron_cond_wait(aeron_cond_t *cv, aeron_mutex_t *mutex)
{
    SleepConditionVariableCS(cv, mutex, INFINITE);
    return 0;
}

int aeron_cond_signal(aeron_cond_t *cv)
{
    WakeConditionVariable(cv);
    return 0;
}

#else
#error Unsupported platform!
#endif

 // sched

#if defined(AERON_COMPILER_GCC)

#include <sched.h>

void proc_yield(void)
{
#if !defined(AERON_CPU_ARM)
    __asm__ __volatile__("pause\n": : : "memory");
#endif
}

#elif defined(AERON_COMPILER_MSVC)

#else
#error Unsupported platform!
#endif
