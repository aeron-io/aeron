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

#ifndef AERON_CLUSTER_SERVICE_CONTEXT_H
#define AERON_CLUSTER_SERVICE_CONTEXT_H

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_cluster_service.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_SERVICE_ID_DEFAULT                0
#define AERON_CLUSTER_CONTROL_CHANNEL_DEFAULT           "aeron:ipc"
#define AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_DEFAULT 104
#define AERON_CLUSTER_SERVICE_STREAM_ID_DEFAULT         105
#define AERON_CLUSTER_SNAPSHOT_CHANNEL_DEFAULT          "aeron:ipc"
#define AERON_CLUSTER_SNAPSHOT_STREAM_ID_DEFAULT        107
#define AERON_CLUSTER_SNAPSHOT_TYPE_ID                  INT64_C(2)

/* Environment variable names */
#define AERON_CLUSTER_SERVICE_ID_ENV_VAR                    "AERON_CLUSTER_SERVICE_ID"
#define AERON_CLUSTER_CONTROL_CHANNEL_ENV_VAR               "AERON_CLUSTER_CONTROL_CHANNEL"
#define AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID_ENV_VAR    "AERON_CLUSTER_CONSENSUS_MODULE_STREAM_ID"
#define AERON_CLUSTER_SERVICE_STREAM_ID_ENV_VAR_SVC         "AERON_CLUSTER_SERVICE_STREAM_ID"
#define AERON_CLUSTER_SNAPSHOT_CHANNEL_ENV_VAR              "AERON_CLUSTER_SNAPSHOT_CHANNEL"
#define AERON_CLUSTER_SNAPSHOT_STREAM_ID_ENV_VAR            "AERON_CLUSTER_SNAPSHOT_STREAM_ID"
#define AERON_CLUSTER_DIR_ENV_VAR                           "AERON_CLUSTER_DIR"
#define AERON_CLUSTER_SERVICE_APP_VERSION_ENV_VAR           "AERON_CLUSTER_APP_VERSION"

typedef struct aeron_cluster_service_context_stct aeron_cluster_service_context_t;

struct aeron_cluster_service_context_stct
{
    aeron_t *aeron;
    char     aeron_directory_name[AERON_MAX_PATH];
    bool     owns_aeron_client;

    int32_t  service_id;

    char    *control_channel;
    size_t   control_channel_length;
    int32_t  consensus_module_stream_id;

    char    *service_channel;
    size_t   service_channel_length;
    int32_t  service_stream_id;

    char    *snapshot_channel;
    size_t   snapshot_channel_length;
    int32_t  snapshot_stream_id;

    char     cluster_dir[AERON_MAX_PATH];

    int32_t  app_version;

    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    bool                       owns_idle_strategy;

    aeron_error_handler_t      error_handler;
    void                      *error_handler_clientd;

    aeron_clustered_service_t *service;
};

int  aeron_cluster_service_context_init(aeron_cluster_service_context_t **ctx);
int  aeron_cluster_service_context_close(aeron_cluster_service_context_t *ctx);
int  aeron_cluster_service_context_conclude(aeron_cluster_service_context_t *ctx);

void aeron_cluster_service_context_idle(aeron_cluster_service_context_t *ctx);
void aeron_cluster_service_context_invoke_aeron_client(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_aeron(aeron_cluster_service_context_t *ctx, aeron_t *aeron);
aeron_t *aeron_cluster_service_context_get_aeron(aeron_cluster_service_context_t *ctx);
int  aeron_cluster_service_context_set_aeron_directory_name(aeron_cluster_service_context_t *ctx, const char *dir);
const char *aeron_cluster_service_context_get_aeron_directory_name(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_service_id(aeron_cluster_service_context_t *ctx, int32_t id);
int32_t aeron_cluster_service_context_get_service_id(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_control_channel(aeron_cluster_service_context_t *ctx, const char *channel);
const char *aeron_cluster_service_context_get_control_channel(aeron_cluster_service_context_t *ctx);
int  aeron_cluster_service_context_set_consensus_module_stream_id(aeron_cluster_service_context_t *ctx, int32_t id);
int32_t aeron_cluster_service_context_get_consensus_module_stream_id(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_service_channel(aeron_cluster_service_context_t *ctx, const char *channel);
const char *aeron_cluster_service_context_get_service_channel(aeron_cluster_service_context_t *ctx);
int  aeron_cluster_service_context_set_service_stream_id(aeron_cluster_service_context_t *ctx, int32_t id);
int32_t aeron_cluster_service_context_get_service_stream_id(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_snapshot_channel(aeron_cluster_service_context_t *ctx, const char *channel);
const char *aeron_cluster_service_context_get_snapshot_channel(aeron_cluster_service_context_t *ctx);
int  aeron_cluster_service_context_set_snapshot_stream_id(aeron_cluster_service_context_t *ctx, int32_t id);
int32_t aeron_cluster_service_context_get_snapshot_stream_id(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_cluster_dir(aeron_cluster_service_context_t *ctx, const char *dir);
const char *aeron_cluster_service_context_get_cluster_dir(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_app_version(aeron_cluster_service_context_t *ctx, int32_t version);
int32_t aeron_cluster_service_context_get_app_version(aeron_cluster_service_context_t *ctx);

int  aeron_cluster_service_context_set_idle_strategy(aeron_cluster_service_context_t *ctx,
         aeron_idle_strategy_func_t func, void *state);
int  aeron_cluster_service_context_set_error_handler(aeron_cluster_service_context_t *ctx,
         aeron_error_handler_t handler, void *clientd);
int  aeron_cluster_service_context_set_service(aeron_cluster_service_context_t *ctx,
         aeron_clustered_service_t *service);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_CONTEXT_H */
