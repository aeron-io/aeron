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

#include "aeron_cluster_configuration.h"
#include "aeron_common.h"

int32_t aeron_cluster_semantic_version(void)
{
    return aeron_semantic_version_compose(
        AERON_CLUSTER_PROTOCOL_MAJOR_VERSION,
        AERON_CLUSTER_PROTOCOL_MINOR_VERSION,
        AERON_CLUSTER_PROTOCOL_PATCH_VERSION);
}
