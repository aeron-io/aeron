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
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include "util/aeron_platform.h"

#include <stdlib.h>
#include <errno.h>
#include <inttypes.h>

#if defined(AERON_COMPILER_MSVC)
#include <windows.h>
#endif

#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "aeron_alloc.h"

int aeron_alloc(void **ptr, size_t size)
{
    void *bytes = malloc(size);
    if (NULL == bytes)
    {
        *ptr = NULL;
        AERON_SET_ERR(ENOMEM, "Failed to allocate %" PRIu64 " bytes", (uint64_t)size);
        return -1;
    }

    memset(bytes, 0, size);
    *ptr = bytes;

    return 0;
}

int aeron_alloc_aligned(void **ptr, size_t *offset, size_t size, size_t alignment)
{
    if (!(AERON_IS_POWER_OF_TWO(alignment)))
    {
        AERON_SET_ERR(EINVAL, "Alignment must be a power of two: %" PRIu64, (uint64_t)alignment);
        return -1;
    }

#ifdef HAVE_POSIX_MEMALIGN
    int rc = posix_memalign(ptr, alignment, size);
    if (rc != 0)
    {
        // any non zero value is a failure.
        AERON_SET_ERR(rc, "Failed to allocate %" PRIu64 " bytes, alignment %" PRIu64, (uint64_t)size, (uint64_t)alignment);
        return -1;
    }
    memset(*ptr, 0, size);
    *offset = 0;
#else
    if (aeron_alloc(ptr, size + alignment) < 0)
    {
        AERON_APPEND_ERR("alignment=%" PRIu64, alignment);
        return -1;
    }

    intptr_t addr = (intptr_t)*ptr;
    *offset = alignment - (addr & (alignment - 1));
#endif

    return 0;
}

int aeron_reallocf(void **ptr, size_t size)
{
    if (0 == size)
    {
        aeron_free(*ptr);
        *ptr = NULL;
        return 0;
    }

#ifdef HAVE_REALLOCF
    if (NULL == (*ptr = reallocf(*ptr, size)))
    {
        AERON_SET_ERR(ENOMEM, "Failed to re-allocate memory with a new size %" PRIu64, (uint64_t)size);
        return -1;
    }
#else
    void *new_ptr = NULL;
    /* mimic reallocf */
    if (NULL == (new_ptr = realloc(*ptr, size)))
    {
        free(*ptr);
        *ptr = NULL;
        AERON_SET_ERR(ENOMEM, "Failed to re-allocate memory with a new size %" PRIu64, (uint64_t)size);
        return -1;
    }

    *ptr = new_ptr;
#endif
    return 0;
}

void aeron_free(void *ptr)
{
    free(ptr);
}
