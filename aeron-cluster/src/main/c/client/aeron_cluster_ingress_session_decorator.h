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

#ifndef AERON_CLUSTER_INGRESS_SESSION_DECORATOR_H
#define AERON_CLUSTER_INGRESS_SESSION_DECORATOR_H

#include <stdint.h>
#include <stddef.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Thin wrapper around an aeron_publication_t that automatically prepends the
 * cluster session header before each message.
 *
 * Mirrors Java IngressSessionDecorator. Useful when the application holds a
 * raw ingress publication and wants to send messages without building the
 * header manually.
 */
typedef struct aeron_cluster_ingress_session_decorator_stct
{
    aeron_publication_t *publication;
    int64_t              cluster_session_id;
    int64_t              leadership_term_id;
}
aeron_cluster_ingress_session_decorator_t;

/**
 * Initialise the decorator.  Does not take ownership of the publication.
 */
void aeron_cluster_ingress_session_decorator_init(
    aeron_cluster_ingress_session_decorator_t *decorator,
    aeron_publication_t *publication,
    int64_t cluster_session_id,
    int64_t leadership_term_id);

/**
 * Update the leadership term ID (call after a leader-redirect event).
 */
void aeron_cluster_ingress_session_decorator_update_leadership_term_id(
    aeron_cluster_ingress_session_decorator_t *decorator,
    int64_t leadership_term_id);

/**
 * Offer a message with the session header prepended.
 * Returns the publication offer position or a AERON_PUBLICATION_* error code.
 */
int64_t aeron_cluster_ingress_session_decorator_offer(
    aeron_cluster_ingress_session_decorator_t *decorator,
    const uint8_t *buffer, size_t length);

/**
 * Scatter-gather offer with session header prepended (up to 8 user vectors).
 */
int64_t aeron_cluster_ingress_session_decorator_offer_vectors(
    aeron_cluster_ingress_session_decorator_t *decorator,
    const aeron_iovec_t *vectors, size_t vector_count);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_INGRESS_SESSION_DECORATOR_H */
