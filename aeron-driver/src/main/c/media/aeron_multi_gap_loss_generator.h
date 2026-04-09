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

#ifndef AERON_MULTI_GAP_LOSS_GENERATOR_H
#define AERON_MULTI_GAP_LOSS_GENERATOR_H

#include "media/aeron_loss_generator.h"

/*
 * Drops total_gaps periodic gaps of gap_length bytes, spaced at gap_radix
 * intervals on a specific term_id. Each gap drops only once per
 * stream-id/session-id pair; retransmissions are not dropped again. Mirrors
 * io.aeron.driver.ext.MultiGapLossGenerator.
 *
 * gap_radix is rounded up to the next power of two at construction. Fails
 * with -1 if gap_length >= actual_gap_radix.
 *
 * Requires the detailed overload; no-op on hooks that dispatch through the
 * simple overload. Matches Java DebugSend/ReceiveChannelEndpoint behaviour.
 *
 * Not thread-safe: one generator per endpoint.
 */
int aeron_multi_gap_loss_generator_create(
    aeron_loss_generator_t **generator,
    int32_t term_id,
    int32_t gap_radix,
    int32_t gap_length,
    int32_t total_gaps);

void aeron_multi_gap_loss_generator_delete(aeron_loss_generator_t *generator);

#endif //AERON_MULTI_GAP_LOSS_GENERATOR_H
