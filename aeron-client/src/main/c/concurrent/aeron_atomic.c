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

#include "concurrent/aeron_atomic.h"

extern uint32_t aeron_get_acquire_uint32(const volatile uint32_t *src);

extern int32_t aeron_get_acquire_int32(const volatile int32_t *src);

extern int aeron_get_acquire_int(const volatile int *src);

extern bool aeron_get_acquire_bool(const volatile bool *src);

extern uint64_t aeron_get_acquire_uint64(const volatile uint64_t *src);

extern int64_t aeron_get_acquire_int64(const volatile int64_t *src);

extern void aeron_set_release_uint32(volatile uint32_t *dst, uint32_t src);

extern void aeron_set_release_int32(volatile int32_t *dst, int32_t src);

extern void aeron_set_release_int(volatile int *dst, int src);

extern void aeron_set_release_bool(volatile bool *dst, bool src);

extern void aeron_set_release_uint64(volatile uint64_t *dst, uint64_t src);

extern void aeron_set_release_int64(volatile int64_t *dst, int64_t src);

extern int64_t aeron_get_and_add_int64(volatile int64_t *dst, int64_t value);

extern int32_t aeron_get_and_add_int32(volatile int32_t *dst, int32_t value);

extern bool aeron_cas_int64(volatile int64_t *dst, int64_t expected, int64_t desired);

extern bool aeron_cas_uint64(volatile uint64_t *dst, uint64_t expected, uint64_t desired);

extern bool aeron_cas_int32(volatile int32_t *dst, int32_t expected, int32_t desired);

extern void aeron_acquire(void);

extern void aeron_release(void);
