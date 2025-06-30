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

#ifndef AERON_AERON_DRIVER_URI_H
#define AERON_AERON_DRIVER_URI_H

#include "uri/aeron_uri.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

#define AERON_URI_PROTOTYPE_VALUE_CORRELATION_ID (-2)

typedef struct aeron_driver_uri_publication_params_stct
{
    bool has_position;
    bool is_sparse;
    bool signal_eos;
    bool spies_simulate_connection;
    bool has_mtu_length;
    size_t mtu_length;
    bool has_term_length;
    size_t term_length;
    size_t term_offset;
    int32_t initial_term_id;
    int32_t term_id;
    uint64_t linger_timeout_ns;
    uint64_t untethered_window_limit_timeout_ns;
    int64_t untethered_linger_timeout_ns;
    uint64_t untethered_resting_timeout_ns;
    bool has_session_id;
    int32_t session_id;
    int64_t entity_tag;
    int64_t response_correlation_id;
    bool has_max_resend;
    uint32_t max_resend;
    bool has_publication_window_length;
    int32_t publication_window_length;
    bool is_response;
}
aeron_driver_uri_publication_params_t;

typedef struct aeron_driver_uri_subscription_params_stct
{
    bool is_reliable;
    bool is_sparse;
    bool is_tether;
    bool is_rejoin;
    bool has_session_id;
    bool is_response;
    aeron_inferable_boolean_t group;
    int32_t session_id;
    size_t initial_window_length;
    uint64_t untethered_window_limit_timeout_ns;
    int64_t untethered_linger_timeout_ns;
    uint64_t untethered_resting_timeout_ns;
}
aeron_driver_uri_subscription_params_t;

typedef struct aeron_driver_context_stct aeron_driver_context_t;
typedef struct aeron_driver_conductor_stct aeron_driver_conductor_t;

int aeron_diver_uri_publication_params(
    aeron_uri_t *uri,
    aeron_driver_uri_publication_params_t *params,
    aeron_driver_conductor_t *conductor,
    bool is_exclusive);

int aeron_driver_uri_subscription_params(
    aeron_uri_t *uri,
    aeron_driver_uri_subscription_params_t *params,
    aeron_driver_conductor_t *conductor);

int aeron_publication_params_validate_mtu_for_sndbuf(
    aeron_driver_uri_publication_params_t *params,
    size_t endpoint_socket_sndbuf,
    size_t channel_socket_sndbuf,
    size_t context_socket_sndbuf,
    size_t os_default_socket_sndbuf);

int aeron_driver_uri_get_timestamp_offset(aeron_uri_t *uri, const char *key, int32_t *offset);
const char *aeron_driver_uri_get_offset_info(int32_t offset);


#endif //AERON_AERON_DRIVER_URI_H
