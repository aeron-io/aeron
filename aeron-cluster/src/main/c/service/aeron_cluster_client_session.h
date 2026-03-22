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

#ifndef AERON_CLUSTER_CLIENT_SESSION_H
#define AERON_CLUSTER_CLIENT_SESSION_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_cluster_client_session_stct
{
    int64_t                  cluster_session_id;
    int32_t                  response_stream_id;
    char                    *response_channel;

    uint8_t                 *encoded_principal;
    size_t                   encoded_principal_length;

    aeron_publication_t     *response_publication;   /* lazily opened on first offer */
    bool                     is_closing;
    bool                     is_open;

    aeron_t                 *aeron;                  /* borrowed */
}
aeron_cluster_client_session_t;

int  aeron_cluster_client_session_create(
    aeron_cluster_client_session_t **session,
    int64_t cluster_session_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t encoded_principal_length,
    aeron_t *aeron);

int  aeron_cluster_client_session_close_and_free(aeron_cluster_client_session_t *session);

/* Lazily open the response publication if not already open */
int  aeron_cluster_client_session_connect(aeron_cluster_client_session_t *session);

int64_t aeron_cluster_client_session_id(aeron_cluster_client_session_t *session);
int32_t aeron_cluster_client_session_response_stream_id(aeron_cluster_client_session_t *session);
const char *aeron_cluster_client_session_response_channel(aeron_cluster_client_session_t *session);
const uint8_t *aeron_cluster_client_session_encoded_principal(aeron_cluster_client_session_t *session);
size_t  aeron_cluster_client_session_encoded_principal_length(aeron_cluster_client_session_t *session);
bool    aeron_cluster_client_session_is_closing(aeron_cluster_client_session_t *session);

/**
 * Offer a reply message to this client.  Prepends the session header automatically.
 * Returns offer position (> 0), or AERON_PUBLICATION_* error codes.
 */
int64_t aeron_cluster_client_session_offer(
    aeron_cluster_client_session_t *session,
    int64_t leadership_term_id,
    const uint8_t *buffer,
    size_t length);

/**
 * Mark the session as closing — the agent will send CloseSession to the CM.
 */
void    aeron_cluster_client_session_mark_closing(aeron_cluster_client_session_t *session);
void    aeron_cluster_client_session_reset_closing(aeron_cluster_client_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CLIENT_SESSION_H */
