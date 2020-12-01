/*
 * Copyright 2014-2020 Real Logic Limited.
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

#ifndef AERON_AERON_URI_DRIVER_H
#define AERON_AERON_URI_DRIVER_H

#include "uri/aeron_uri.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

typedef struct aeron_uri_driver_publication_params_stct
{
    bool has_position;
    bool is_sparse;
    bool signal_eos;
    bool spies_simulate_connection;
    size_t mtu_length;
    size_t term_length;
    size_t term_offset;
    int32_t initial_term_id;
    int32_t term_id;
    uint64_t linger_timeout_ns;
    bool has_session_id;
    int32_t session_id;
    int64_t entity_tag;
}
aeron_driver_uri_publication_params_t;

typedef struct aeron_uri_driver_subscription_params_stct
{
    bool is_reliable;
    bool is_sparse;
    bool is_tether;
    bool is_rejoin;
    aeron_inferable_boolean_t group;
    bool has_session_id;
    int32_t session_id;
}
aeron_uri_subscription_params_t;

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

int aeron_uri_driver_publication_params(
    aeron_uri_t *uri,
    aeron_driver_uri_publication_params_t *params,
    aeron_driver_conductor_t *conductor,
    bool is_exclusive);

int aeron_uri_driver_subscription_params(
    aeron_uri_t *uri,
    aeron_uri_subscription_params_t *params,
    aeron_driver_conductor_t *conductor);

#endif //AERON_AERON_URI_DRIVER_H
