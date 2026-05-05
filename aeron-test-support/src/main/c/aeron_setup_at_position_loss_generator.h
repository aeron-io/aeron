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

#ifndef AERON_SETUP_AT_POSITION_LOSS_GENERATOR_H
#define AERON_SETUP_AT_POSITION_LOSS_GENERATOR_H

#include "media/aeron_loss_generator.h"
#include "uri/aeron_uri.h"

// Drops incoming SETUP frames whose (initial_term_id, active_term_id, term_offset)
// tuple matches a target — but only on endpoints whose original_uri does NOT contain
// `endpoint_skip_substring`. This lets a test deterministically force a publication-image
// match-time race: SETUPs at the publisher's join position are dropped on one
// subscriber's endpoint (forcing it to attach via a later SETUP at an advanced
// position), while another subscriber's endpoint sees the first SETUP normally.
//
// Generators are per-endpoint; the supplier callback creates one per receive endpoint
// and the per-instance state holds a pointer to the endpoint plus the shared config.
// At frame-check time the generator inspects endpoint->conductor_fields.udp_channel
// (which the driver assigns AFTER the supplier returns, but BEFORE any frame arrives)
// and decides whether to drop.
typedef struct aeron_setup_at_position_loss_config_stct
{
    // Target SETUP fields. SETUPs whose stream_id matches AND whose
    // initial_term_id/active_term_id/term_offset all match are candidates for dropping.
    // Other SETUPs always pass.
    int32_t stream_id;
    int32_t initial_term_id;
    int32_t active_term_id;
    int32_t term_offset;

    // If non-empty, endpoints whose original_uri contains this substring are SKIPPED
    // (their SETUPs always pass, regardless of position match). Used to exempt the
    // archive's recording subscription, whose URI carries `init-term-id=` while a plain
    // application subscription does not.
    char endpoint_skip_substring[AERON_URI_MAX_LENGTH];

    volatile bool enabled;
    volatile int setups_dropped;
}
aeron_setup_at_position_loss_config_t;

void aeron_setup_at_position_loss_config_init(aeron_setup_at_position_loss_config_t *config);

void aeron_setup_at_position_loss_config_set_target(
    aeron_setup_at_position_loss_config_t *config,
    int32_t stream_id,
    int32_t initial_term_id,
    int32_t active_term_id,
    int32_t term_offset);

void aeron_setup_at_position_loss_config_set_endpoint_skip_substring(
    aeron_setup_at_position_loss_config_t *config,
    const char *substring);

void aeron_setup_at_position_loss_config_enable(aeron_setup_at_position_loss_config_t *config);

void aeron_setup_at_position_loss_config_disable(aeron_setup_at_position_loss_config_t *config);

int aeron_setup_at_position_loss_config_setups_dropped(aeron_setup_at_position_loss_config_t *config);

// Forward decl — callers include the driver header for the full type.
struct aeron_receive_channel_endpoint_stct;

// Allocates a new generator bound to (config, endpoint). The config must outlive the
// generator (and is not owned/freed by it). Caller transfers ownership of the returned
// generator to the receive endpoint's data_loss_generator field; the driver invokes
// gen->close on endpoint teardown, which frees the generator allocation.
int aeron_setup_at_position_loss_generator_create(
    aeron_loss_generator_t **generator,
    aeron_setup_at_position_loss_config_t *config,
    struct aeron_receive_channel_endpoint_stct *endpoint);

#endif //AERON_SETUP_AT_POSITION_LOSS_GENERATOR_H
