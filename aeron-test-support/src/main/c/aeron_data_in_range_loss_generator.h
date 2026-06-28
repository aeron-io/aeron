/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#ifndef AERON_DATA_IN_RANGE_LOSS_GENERATOR_H
#define AERON_DATA_IN_RANGE_LOSS_GENERATOR_H

#include "media/aeron_loss_generator.h"
#include "uri/aeron_uri.h"

// Drops incoming DATA frames whose `(stream_id, active_term_id)` matches and whose
// term_offset falls in `[term_offset_inclusive_min, term_offset_exclusive_max)` — but
// only on endpoints whose original_uri does NOT contain `endpoint_skip_substring`.
// Used to deterministically punch a contiguous gap into one subscriber's reception
// while letting another subscriber on the same publication receive the same range
// normally (e.g. exempting the archive's recording subscription so the recording
// stays contiguous while a client subscriber is forced to recover via replay).
//
// Generators are per-endpoint; the supplier callback creates one per receive endpoint
// and the per-instance state holds a pointer to the endpoint plus the shared config.
// At frame-check time the generator inspects endpoint->conductor_fields.udp_channel
// (which the driver assigns AFTER the supplier returns, but BEFORE any frame arrives)
// and decides whether to drop.
typedef struct aeron_data_in_range_loss_config_stct
{
    int32_t stream_id;
    int32_t active_term_id;
    int32_t term_offset_inclusive_min;
    int32_t term_offset_exclusive_max;

    // If non-empty, endpoints whose original_uri contains this substring are SKIPPED
    // (their DATA always passes, regardless of the position match). Used to exempt
    // the archive's recording subscription, whose URI carries `init-term-id=` while
    // a plain application subscription does not.
    char endpoint_skip_substring[AERON_URI_MAX_LENGTH];

    volatile bool enabled;
    volatile int frames_dropped;
}
aeron_data_in_range_loss_config_t;

void aeron_data_in_range_loss_config_init(aeron_data_in_range_loss_config_t *config);

void aeron_data_in_range_loss_config_set_target(
    aeron_data_in_range_loss_config_t *config,
    int32_t stream_id,
    int32_t active_term_id,
    int32_t term_offset_inclusive_min,
    int32_t term_offset_exclusive_max);

void aeron_data_in_range_loss_config_set_endpoint_skip_substring(
    aeron_data_in_range_loss_config_t *config,
    const char *substring);

void aeron_data_in_range_loss_config_enable(aeron_data_in_range_loss_config_t *config);

void aeron_data_in_range_loss_config_disable(aeron_data_in_range_loss_config_t *config);

int aeron_data_in_range_loss_config_frames_dropped(aeron_data_in_range_loss_config_t *config);

// Forward decl — callers include the driver header for the full type.
struct aeron_receive_channel_endpoint_stct;

// Allocates a new generator bound to (config, endpoint). The config must outlive the
// generator (and is not owned/freed by it). Caller transfers ownership of the returned
// generator to the receive endpoint's data_loss_generator field; the driver invokes
// gen->close on endpoint teardown, which frees the generator allocation.
int aeron_data_in_range_loss_generator_create(
    aeron_loss_generator_t **generator,
    aeron_data_in_range_loss_config_t *config,
    struct aeron_receive_channel_endpoint_stct *endpoint);

#endif //AERON_DATA_IN_RANGE_LOSS_GENERATOR_H
