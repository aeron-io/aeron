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

#ifndef AERON_PORT_LOSS_GENERATOR_H
#define AERON_PORT_LOSS_GENERATOR_H

#include "media/aeron_loss_generator.h"

int aeron_port_loss_generator_create(aeron_loss_generator_t **generator);

void aeron_port_loss_generator_start_dropping(
    aeron_loss_generator_t *generator, int port, int64_t duration_ns);

void aeron_port_loss_generator_delete(aeron_loss_generator_t *generator);

#endif //AERON_PORT_LOSS_GENERATOR_H
