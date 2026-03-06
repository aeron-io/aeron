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

#ifndef AERON_LOSS_DETECTOR_H
#define AERON_LOSS_DETECTOR_H

#include "aeron_driver_common.h"
#include "concurrent/aeron_term_gap_scanner.h"

typedef struct aeron_loss_detector_gap_stct
{
    int32_t term_id;
    int32_t term_offset;
    int32_t length;
    int64_t expiry_ns;
} aeron_loss_detector_gap_t;

#define AERON_LOSS_DETECTOR_TIMER_INACTIVE (-1)
#define AERON_LOSS_DETECTOR_MAX_LOSSES (4096)

typedef void (*aeron_loss_detector_on_gap_detected_func_t)(void *clientd, int32_t term_id, int32_t term_offset, size_t length);

typedef struct aeron_loss_detector_stct
{
    aeron_loss_detector_on_gap_detected_func_t on_gap_detected;
    aeron_feedback_delay_generator_state_t *feedback_delay_state;
    void *on_gap_detected_clientd;
    aeron_loss_detector_gap_t *gaps;
    aeron_loss_detector_gap_t *tmp_gaps;
    int32_t gaps_count;
} aeron_loss_detector_t;

int aeron_loss_detector_init(
    aeron_loss_detector_t *detector,
    aeron_feedback_delay_generator_state_t *feedback_delay_state,
    aeron_loss_detector_on_gap_detected_func_t on_gap_detected,
    void *on_gap_detected_clientd);

void aeron_loss_detector_close(aeron_loss_detector_t *detector);

int64_t aeron_loss_detector_scan(
    aeron_loss_detector_t *detector,
    bool *loss_found,
    const uint8_t *buffer,
    const uint8_t *next_buffer,
    int64_t rebuild_position,
    int64_t hwm_position,
    int64_t now_ns,
    size_t term_length_mask,
    size_t position_bits_to_shift,
    int32_t initial_term_id);

#define AERON_LOSS_DETECTOR_NAK_UNICAST_DELAY_NS (60 * 1000 * 1000LL)

inline int64_t aeron_loss_detector_nak_unicast_delay_generator(aeron_feedback_delay_generator_state_t *state, bool retry)
{
    if (retry)
    {
        return state->static_delay.retry_ns;
    }
    else
    {
        return state->static_delay.delay_ns;
    }
}

#define AERON_LOSS_DETECTOR_NAK_MULTICAST_GROUP_SIZE (10.0)
#define AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF_NS (60.0 * 1000.0 * 1000.0)

int aeron_feedback_delay_state_init(
    aeron_feedback_delay_generator_state_t *state,
    aeron_feedback_delay_generator_func_t delay_generator,
    int64_t delay_ns,
    int64_t retry_ns,
    size_t multicast_group_size);

int64_t aeron_loss_detector_nak_multicast_delay_generator(aeron_feedback_delay_generator_state_t *state, bool retry);

#endif // AERON_LOSS_DETECTOR_H
