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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <math.h>
#include <stdlib.h>
#include "aeronmd.h"
#include "aeron_windows.h"
#include "aeron_loss_detector.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_loss_detector_init(
    aeron_loss_detector_t *detector,
    aeron_feedback_delay_generator_state_t *feedback_delay_state,
    aeron_loss_detector_on_gap_detected_func_t on_gap_detected,
    void *on_gap_detected_clientd)
{
    detector->on_gap_detected = on_gap_detected;
    detector->on_gap_detected_clientd = on_gap_detected_clientd;
    detector->feedback_delay_state = feedback_delay_state;
    detector->gaps = NULL;
    detector->gaps_count = 0;
    detector->tmp_gaps = NULL;

    if (aeron_alloc((void **)&detector->gaps, AERON_LOSS_DETECTOR_MAX_LOSSES * sizeof(aeron_loss_detector_gap_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to alloc gaps");
        goto error;
    }

    if (aeron_alloc((void **)&detector->tmp_gaps, AERON_LOSS_DETECTOR_MAX_LOSSES * sizeof(aeron_loss_detector_gap_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to alloc tmp_gaps");
        goto error;
    }

    return 0;

error:
    aeron_free(detector->gaps);
    aeron_free(detector->tmp_gaps);

    return -1;
}

static void aeron_loss_detector_check_timer_expiry(aeron_loss_detector_t *detector, aeron_loss_detector_gap_t *gap, int64_t now_ns);

void aeron_loss_detector_close(aeron_loss_detector_t *detector)
{
    aeron_free(detector->gaps);
    aeron_free(detector->tmp_gaps);
}

static inline int64_t aeron_loss_detector_scan_for_first_gap_position(
    const uint8_t *buffer,
    const uint8_t *next_buffer,
    int64_t rebuild_position,
    int64_t hwm_position,
    size_t term_length_mask,
    size_t position_bits_to_shift,
    int32_t initial_term_id,
    int32_t *gap_length)
{
    const int32_t rebuild_term_count = (int32_t)(rebuild_position >> position_bits_to_shift);
    const int32_t hwm_term_count = (int32_t)(hwm_position >> position_bits_to_shift);

    const int32_t rebuild_term_offset = (int32_t)(rebuild_position & term_length_mask);
    const int32_t hwm_term_offset = (int32_t)(hwm_position & term_length_mask);

    const int32_t limit_offset = rebuild_term_count == hwm_term_count ? hwm_term_offset : (int32_t)(term_length_mask + 1);

    int32_t gap_start_offset = aeron_term_gap_scanner_scan_for_gap(
        buffer,
        rebuild_term_offset,
        limit_offset,
        gap_length);

    if (gap_start_offset < 0)
    {
        return (int64_t)gap_start_offset;
    }

    if (gap_start_offset != limit_offset || rebuild_term_count == hwm_term_count)
    {
        return (((int64_t)rebuild_term_count) << position_bits_to_shift) + gap_start_offset;
    }

    gap_start_offset = aeron_term_gap_scanner_scan_for_gap(
        next_buffer,
        0,
        hwm_term_offset,
        gap_length);

    if (gap_start_offset < 0)
    {
        return (int64_t)gap_start_offset;
    }

    return (((int64_t)hwm_term_count) << position_bits_to_shift) + gap_start_offset;
}

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
    int32_t initial_term_id)
{
    *loss_found = false;

    if (rebuild_position >= hwm_position)
    {
        return rebuild_position;
    }

    int32_t gap_length;
    int64_t first_gap_position = aeron_loss_detector_scan_for_first_gap_position(
        buffer,
        next_buffer,
        rebuild_position,
        hwm_position,
        term_length_mask,
        position_bits_to_shift,
        initial_term_id,
        &gap_length);

    if (first_gap_position < 0)
    {
        return rebuild_position;
    }

    const int32_t rebuild_term_count = (int32_t)(rebuild_position >> position_bits_to_shift);
    const int32_t hwm_term_count = (int32_t)(hwm_position >> position_bits_to_shift);
    const int32_t hwm_term_offset = (int32_t)(hwm_position & term_length_mask);

    int32_t gaps_count = 0;
    int32_t gap_index = 0;

    for (int64_t position = first_gap_position; position < hwm_position && gaps_count < AERON_LOSS_DETECTOR_MAX_LOSSES;)
    {
        const int32_t term_count = (int32_t)(position >> position_bits_to_shift);
        const int32_t term_id = aeron_add_wrap_i32(initial_term_id, term_count);
        const int32_t term_offset = (int32_t)(position & term_length_mask);

        aeron_loss_detector_gap_t *tmp_gap = &detector->tmp_gaps[gaps_count];
        tmp_gap->term_id = term_id;
        tmp_gap->term_offset = term_offset;
        tmp_gap->length = gap_length;

        while (gap_index < detector->gaps_count)
        {
            aeron_loss_detector_gap_t *gap = &detector->gaps[gaps_count];
            if (term_id == gap->term_id && term_offset < gap->term_offset + gap->length)
            {
                break;
            }
            gap_index++;
        }

        if (gap_index < detector->gaps_count)
        {
            aeron_loss_detector_gap_t *gap = &detector->gaps[gaps_count];
            if (term_offset + gap_length <= gap->term_offset + gap->length)
            {
                tmp_gap->expiry_ns = gap->expiry_ns;
            }
            else
            {
                tmp_gap->expiry_ns = now_ns + detector->feedback_delay_state->delay_generator(detector->feedback_delay_state, false);
                *loss_found = true;
            }
        }
        else
        {
            tmp_gap->expiry_ns = now_ns + detector->feedback_delay_state->delay_generator(detector->feedback_delay_state, false);
            *loss_found = true;
        }

        aeron_loss_detector_check_timer_expiry(detector, tmp_gap, now_ns);

        gaps_count++;

        const int64_t next_scan_position = position + gap_length;
        if (next_scan_position >= hwm_position)
        {
            break;
        }

        const int32_t next_scan_term_count = (int32_t)(next_scan_position >> position_bits_to_shift);
        const int32_t next_scan_term_offset = (int32_t)(next_scan_position & term_length_mask);
        const int32_t next_scan_limit_offset = next_scan_term_count == hwm_term_count ? hwm_term_offset : (int32_t)(term_length_mask + 1);
        const uint8_t *next_scan_buffer = next_scan_term_count == rebuild_term_count ? buffer : next_buffer;

        int32_t next_offset = aeron_term_gap_scanner_scan_for_gap(
            next_scan_buffer,
            next_scan_term_offset,
            next_scan_limit_offset,
            &gap_length);

        if (next_offset < 0)
        {
            break;
        }

        position = (((int64_t)next_scan_term_count) << position_bits_to_shift) + next_offset;
    }

    // swap the buffers
    aeron_loss_detector_gap_t *t = detector->tmp_gaps;
    detector->tmp_gaps = detector->gaps;
    detector->gaps = t;
    detector->gaps_count = gaps_count;

    return first_gap_position;
}

int aeron_feedback_delay_state_init(
    aeron_feedback_delay_generator_state_t *state,
    aeron_feedback_delay_generator_func_t delay_generator,
    int64_t delay_ns,
    int64_t retry_ns,
    size_t multicast_group_size)
{
    static bool is_seeded = false;
    double lambda = log((double)multicast_group_size) + 1;
    double max_backoff_T = (double)delay_ns;

    state->static_delay.delay_ns = delay_ns;
    state->static_delay.retry_ns = retry_ns;

    state->optimal_delay.rand_max = lambda / max_backoff_T;
    state->optimal_delay.base_x = lambda / (max_backoff_T * (exp(lambda) - 1));
    state->optimal_delay.constant_t = max_backoff_T / lambda;
    state->optimal_delay.factor_t = (exp(lambda) - 1) * (max_backoff_T / lambda);

    if (!is_seeded)
    {
        aeron_srand48((uint64_t)aeron_nano_clock());
        is_seeded = true;
    }

    state->delay_generator = delay_generator;
    return 0;
}

int64_t aeron_loss_detector_nak_multicast_delay_generator(aeron_feedback_delay_generator_state_t *state, bool retry)
{
    const double x = (aeron_drand48() * state->optimal_delay.rand_max) + state->optimal_delay.base_x;

    return (int64_t)(state->optimal_delay.constant_t * log(x * state->optimal_delay.factor_t));
}

extern int64_t aeron_loss_detector_nak_unicast_delay_generator(aeron_feedback_delay_generator_state_t *state, bool retry);

static void aeron_loss_detector_check_timer_expiry(aeron_loss_detector_t *detector, aeron_loss_detector_gap_t *gap, int64_t now_ns)
{
    if (now_ns >= gap->expiry_ns)
    {
        detector->on_gap_detected(
            detector->on_gap_detected_clientd,
            gap->term_id,
            gap->term_offset,
            gap->length);

        gap->expiry_ns = now_ns + detector->feedback_delay_state->delay_generator(detector->feedback_delay_state, true);
    }
}
