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

#ifndef AERON_THREAD_H
#define AERON_THREAD_H

#include <stdint.h>
#include <stddef.h>

#include "util/aeron_platform.h"

void aeron_thread_set_name(const char *role_name);

void aeron_nano_sleep(uint64_t nanoseconds);
void aeron_micro_sleep(unsigned int microseconds);
int aeron_thread_set_affinity(const char *role_name, uint8_t cpu_affinity_no);

#if defined(AERON_COMPILER_GCC)

#include <pthread.h>
typedef pthread_mutex_t aeron_mutex_t;
#define AERON_INIT_ONCE pthread_once_t
#define AERON_INIT_ONCE_VALUE PTHREAD_ONCE_INIT

typedef pthread_t aeron_thread_t;
typedef pthread_attr_t aeron_thread_attr_t;

int aeron_mutex_init(aeron_mutex_t *mutex, void *attr);
#define aeron_mutex_lock pthread_mutex_lock
#define aeron_mutex_unlock pthread_mutex_unlock
#define aeron_mutex_destroy pthread_mutex_destroy
#define aeron_thread_once pthread_once
#define aeron_thread_attr_init pthread_attr_init
#define aeron_thread_create pthread_create
#define aeron_thread_join pthread_join
#define aeron_thread_key_create pthread_key_create
#define aeron_thread_key_delete pthread_key_delete
#define aeron_thread_get_specific pthread_getspecific
#define aeron_thread_set_specific pthread_setspecific

typedef pthread_cond_t aeron_cond_t;
#define aeron_cond_init pthread_cond_init
#define aeron_cond_destroy pthread_cond_destroy
#define aeron_cond_signal pthread_cond_signal
#define aeron_cond_wait pthread_cond_wait

#elif defined(AERON_COMPILER_MSVC)

#define WIN32_LEAN_AND_MEAN
#include <Windows.h>

typedef CRITICAL_SECTION aeron_mutex_t;

struct aeron_thread_stct;
typedef struct aeron_thread_stct *aeron_thread_t;

typedef union aeron_init_once_union
{
    void *ptr;
}
AERON_INIT_ONCE;

typedef unsigned long aeron_thread_attr_t;
typedef unsigned long pthread_key_t;

typedef CONDITION_VARIABLE aeron_cond_t;

#define AERON_INIT_ONCE_VALUE {0}

void aeron_thread_once(AERON_INIT_ONCE *s_init_once, void *callback);

int aeron_mutex_init(aeron_mutex_t *mutex, void *attr);
int aeron_mutex_destroy(aeron_mutex_t *mutex);
int aeron_mutex_lock(aeron_mutex_t *mutex);
int aeron_mutex_unlock(aeron_mutex_t *mutex);

int aeron_thread_attr_init(aeron_thread_attr_t *attr);

int aeron_thread_create(aeron_thread_t *thread_ptr, void *attr, void *(*callback)(void *), void *arg0);

int aeron_thread_join(aeron_thread_t thread, void **value_ptr);

int aeron_thread_key_create(pthread_key_t *key_ptr, void (*destr_func)(void *));

int aeron_thread_key_delete(pthread_key_t key);

int aeron_thread_set_specific(pthread_key_t key, const void *pointer);

void *aeron_thread_get_specific(pthread_key_t key);

int aeron_cond_init(aeron_cond_t *cv, void *attr);

int aeron_cond_destroy(aeron_cond_t *cv);

int aeron_cond_wait(aeron_cond_t *cv, aeron_mutex_t *mutex);

int aeron_cond_signal(aeron_cond_t *cv);

#else
#error Unsupported platform!
#endif

// sched

#if defined(AERON_COMPILER_GCC)

void proc_yield(void);

#elif defined(AERON_COMPILER_MSVC)

int sched_yield(void);
#define proc_yield _mm_pause

#else
#error Unsupported platform!
#endif

#endif //AERON_THREAD_H
