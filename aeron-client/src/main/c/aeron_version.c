/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
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

#include <stdint.h>

#include "aeron_version_gitsha.h"

const char aeron_version_full_str[] = "aeron version=" AERON_VERSION_TXT " commit=" AERON_VERSION_GITSHA;

const char *aeron_version_full(void)
{
    return aeron_version_full_str;
}

const char *aeron_version_text(void)
{
    return AERON_VERSION_TXT;
}

int aeron_version_major(void)
{
    return AERON_VERSION_MAJOR;
}

int aeron_version_minor(void)
{
    return AERON_VERSION_MINOR;
}

int aeron_version_patch(void)
{
    return AERON_VERSION_PATCH;
}

const char *aeron_version_gitsha(void)
{
    return AERON_VERSION_GITSHA;
}

int32_t aeron_semantic_version_compose(uint8_t major, uint8_t minor, uint8_t patch)
{
    return (major << 16) | (minor << 8) | patch;
}

uint8_t aeron_semantic_version_major(int32_t version)
{
    return (uint8_t)((version >> 16) & 0xFF);
}

uint8_t aeron_semantic_version_minor(int32_t version)
{
    return (uint8_t)((version >> 8) & 0xFF);
}

uint8_t aeron_semantic_version_patch(int32_t version)
{
    return (uint8_t)(version & 0xFF);
}
