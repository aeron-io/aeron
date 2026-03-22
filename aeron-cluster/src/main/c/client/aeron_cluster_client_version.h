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

#ifndef AERON_CLUSTER_CLIENT_VERSION_H
#define AERON_CLUSTER_CLIENT_VERSION_H

#ifdef __cplusplus
extern "C"
{
#endif

const char *aeron_cluster_client_version_text(void);
const char *aeron_cluster_client_version_git_sha(void);
int aeron_cluster_client_version_major(void);
int aeron_cluster_client_version_minor(void);
int aeron_cluster_client_version_patch(void);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CLIENT_VERSION_H */
