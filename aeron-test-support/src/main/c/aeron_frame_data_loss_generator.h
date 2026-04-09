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

#ifndef AERON_FRAME_DATA_LOSS_GENERATOR_H
#define AERON_FRAME_DATA_LOSS_GENERATOR_H

#include <stddef.h>

#include "media/aeron_loss_generator.h"

typedef bool (*aeron_test_frame_data_predicate_func_t)(
    const uint8_t *buffer, size_t length, void *clientd);

int aeron_frame_data_loss_generator_create(aeron_loss_generator_t **generator);

void aeron_frame_data_loss_generator_enable(
    aeron_loss_generator_t *generator,
    aeron_test_frame_data_predicate_func_t predicate,
    void *clientd);

void aeron_frame_data_loss_generator_disable(aeron_loss_generator_t *generator);

void aeron_frame_data_loss_generator_delete(aeron_loss_generator_t *generator);

#endif //AERON_FRAME_DATA_LOSS_GENERATOR_H
