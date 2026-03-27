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

#ifndef AERON_CLUSTER_CONTEXT_H
#define AERON_CLUSTER_CONTEXT_H

#include "aeron_cluster.h"
#include "aeron_cluster_credentials_supplier.h"

#ifdef __cplusplus
extern "C"
{
#endif


struct aeron_cluster_context_stct
{
    aeron_t *aeron;
    char     aeron_directory_name[AERON_MAX_PATH];
    char     client_name[AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1];
    bool     owns_aeron_client;

    char    *ingress_channel;
    size_t   ingress_channel_length;
    int32_t  ingress_stream_id;

    char    *ingress_endpoints;     /* comma-separated "memberId=host:port,..." for multi-member */
    size_t   ingress_endpoints_length;

    char    *egress_channel;
    size_t   egress_channel_length;
    int32_t  egress_stream_id;

    uint64_t message_timeout_ns;
    uint64_t new_leader_timeout_ns;     /* derived from server's heartbeat; used for reconnect deadline */
    uint32_t message_retry_attempts;

    bool     is_ingress_exclusive;  /* use ExclusivePublication for ingress */

    aeron_idle_strategy_func_t idle_strategy_func;
    void                      *idle_strategy_state;
    bool                       owns_idle_strategy;

    aeron_cluster_credentials_supplier_t credentials_supplier;

    aeron_cluster_egress_message_listener_func_t  on_message;
    void                                         *on_message_clientd;

    aeron_cluster_session_event_listener_func_t  on_session_event;
    void                                        *on_session_event_clientd;

    aeron_cluster_new_leader_event_listener_func_t  on_new_leader_event;
    void                                           *on_new_leader_event_clientd;

    aeron_cluster_admin_response_listener_func_t  on_admin_response;
    void                                         *on_admin_response_clientd;

    aeron_error_handler_t error_handler;
    void                 *error_handler_clientd;

    aeron_cluster_delegating_invoker_func_t delegating_invoker_func;
    void                                   *delegating_invoker_func_clientd;
};

/* lifecycle */
int  aeron_cluster_context_init(aeron_cluster_context_t **ctx);
int  aeron_cluster_context_close(aeron_cluster_context_t *ctx);
int  aeron_cluster_context_conclude(aeron_cluster_context_t *ctx);
int  aeron_cluster_context_duplicate(aeron_cluster_context_t **dest_p, aeron_cluster_context_t *src);

/* internal helpers */
void aeron_cluster_context_idle(aeron_cluster_context_t *ctx);
void aeron_cluster_context_invoke_aeron_client(aeron_cluster_context_t *ctx);

/* aeron */
int      aeron_cluster_context_set_aeron(aeron_cluster_context_t *ctx, aeron_t *aeron);
aeron_t *aeron_cluster_context_get_aeron(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_owns_aeron_client(aeron_cluster_context_t *ctx, bool owns_aeron_client);
bool     aeron_cluster_context_get_owns_aeron_client(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_aeron_directory_name(aeron_cluster_context_t *ctx, const char *value);
const char *aeron_cluster_context_get_aeron_directory_name(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_client_name(aeron_cluster_context_t *ctx, const char *value);
const char *aeron_cluster_context_get_client_name(aeron_cluster_context_t *ctx);

/* ingress */
int      aeron_cluster_context_set_ingress_channel(aeron_cluster_context_t *ctx, const char *channel);
const char *aeron_cluster_context_get_ingress_channel(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_ingress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int32_t  aeron_cluster_context_get_ingress_stream_id(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_ingress_endpoints(aeron_cluster_context_t *ctx, const char *endpoints);
const char *aeron_cluster_context_get_ingress_endpoints(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_is_ingress_exclusive(aeron_cluster_context_t *ctx, bool value);
bool     aeron_cluster_context_get_is_ingress_exclusive(aeron_cluster_context_t *ctx);

/* egress */
int      aeron_cluster_context_set_egress_channel(aeron_cluster_context_t *ctx, const char *channel);
const char *aeron_cluster_context_get_egress_channel(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_egress_stream_id(aeron_cluster_context_t *ctx, int32_t stream_id);
int32_t  aeron_cluster_context_get_egress_stream_id(aeron_cluster_context_t *ctx);

/* timeouts */
int      aeron_cluster_context_set_message_timeout_ns(aeron_cluster_context_t *ctx, uint64_t timeout_ns);
uint64_t aeron_cluster_context_get_message_timeout_ns(aeron_cluster_context_t *ctx);
int      aeron_cluster_context_set_message_retry_attempts(aeron_cluster_context_t *ctx, uint32_t attempts);
uint32_t aeron_cluster_context_get_message_retry_attempts(aeron_cluster_context_t *ctx);

/* idle strategy */
int aeron_cluster_context_set_idle_strategy(
    aeron_cluster_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

/* credentials */
int aeron_cluster_context_set_credentials_supplier(
    aeron_cluster_context_t *ctx,
    aeron_cluster_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_cluster_credentials_challenge_supplier_func_t on_challenge,
    aeron_cluster_credentials_free_func_t on_free,
    void *clientd);

/* egress listeners */
int aeron_cluster_context_set_on_message(
    aeron_cluster_context_t *ctx,
    aeron_cluster_egress_message_listener_func_t on_message,
    void *clientd);
int aeron_cluster_context_set_on_session_event(
    aeron_cluster_context_t *ctx,
    aeron_cluster_session_event_listener_func_t on_session_event,
    void *clientd);
int aeron_cluster_context_set_on_new_leader_event(
    aeron_cluster_context_t *ctx,
    aeron_cluster_new_leader_event_listener_func_t on_new_leader_event,
    void *clientd);
int aeron_cluster_context_set_on_admin_response(
    aeron_cluster_context_t *ctx,
    aeron_cluster_admin_response_listener_func_t on_admin_response,
    void *clientd);

/* error handler */
int aeron_cluster_context_set_error_handler(
    aeron_cluster_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd);

/* delegating invoker */
int aeron_cluster_context_set_delegating_invoker(
    aeron_cluster_context_t *ctx,
    aeron_cluster_delegating_invoker_func_t delegating_invoker_func,
    void *clientd);


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_CONTEXT_H */
