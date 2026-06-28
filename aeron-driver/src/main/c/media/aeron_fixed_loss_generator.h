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

#ifndef AERON_FIXED_LOSS_GENERATOR_H
#define AERON_FIXED_LOSS_GENERATOR_H

#include "media/aeron_loss_generator.h"

/*
 * Drops a fixed block [term_offset, term_offset + length) from a specific
 * term_id once per stream-id/session-id pair. Retransmissions of the same
 * block are not dropped again. Mirrors io.aeron.driver.ext.FixedLossGenerator.
 *
 * Requires the detailed overload; no-op on hooks that dispatch through the
 * simple overload. Matches Java DebugSend/ReceiveChannelEndpoint behaviour.
 *
 * Not thread-safe: one generator per endpoint.
 */
int aeron_fixed_loss_generator_create(
    aeron_loss_generator_t **generator,
    int32_t term_id,
    int32_t term_offset,
    int32_t length);

void aeron_fixed_loss_generator_delete(aeron_loss_generator_t *generator);

#endif //AERON_FIXED_LOSS_GENERATOR_H
