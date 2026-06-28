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

#ifndef AERON_ARRAYUTIL_H
#define AERON_ARRAYUTIL_H

#include "aeron_platform.h"

#if defined(AERON_COMPILER_MSVC)
#define restrict __restrict
#endif

#include <inttypes.h>
#include <stddef.h>
#include <string.h>
#include <errno.h>
#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define AERON_ARRAY_ENSURE_CAPACITY(r, a, t) \
if (a.length >= a.capacity) \
{ \
    size_t new_capacity = 0 == a.capacity ? 2 : (a.capacity + (a.capacity / 2)); \
    r = aeron_array_ensure_capacity((uint8_t **)&a.array, sizeof(t), a.capacity, new_capacity); \
    if (r >= 0) \
    { \
       a.capacity = new_capacity; \
    } \
}

inline int aeron_array_ensure_capacity(uint8_t **array, size_t element_size, size_t old_capacity, size_t new_capacity)
{
    if (aeron_reallocf((void **)array, new_capacity * element_size) < 0)
    {
        AERON_APPEND_ERR("could not ensure capacity of: %" PRIu64, (uint64_t)(new_capacity * element_size));
        return -1;
    }

    memset(*array + (old_capacity * element_size), 0, (new_capacity - old_capacity) * element_size);
    return 0;
}

inline void aeron_array_fast_unordered_remove(
    uint8_t *restrict array, size_t element_size, size_t index, size_t last_index)
{
    memcpy(array + (index * element_size), array + (last_index * element_size), element_size);
}

inline int aeron_array_add(uint8_t **array, size_t element_size, size_t new_length, uint8_t *restrict element_to_add)
{
    if (aeron_reallocf((void **)array, element_size * new_length) < 0)
    {
        AERON_APPEND_ERR("%s", "could not array add");
        return -1;
    }

    memcpy(*array + ((new_length - 1) * element_size), element_to_add, element_size);
    return 0;
}

inline int aeron_array_remove(uint8_t **array, size_t element_size, size_t index, size_t old_length)
{
    for (size_t i = index; i < (old_length - 1); i++)
    {
        memcpy(*array + (i * element_size), *array + ((i + 1) * element_size), element_size);
    }

    if (aeron_reallocf((void **)array, (old_length - 1) * element_size) < 0)
    {
        AERON_APPEND_ERR("%s", "could not array remove realloc");
        return -1;
    }

    return 0;
}

#endif //AERON_ARRAYUTIL_H
