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

#ifndef AERON_DEBUG_CHANNEL_ENDPOINT_CONFIGURATION_H
#define AERON_DEBUG_CHANNEL_ENDPOINT_CONFIGURATION_H

#include "aeron_driver_context.h"

#define AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR       "AERON_DEBUG_SEND_DATA_LOSS_RATE"
#define AERON_DEBUG_SEND_DATA_LOSS_SEED_ENV_VAR       "AERON_DEBUG_SEND_DATA_LOSS_SEED"
#define AERON_DEBUG_SEND_CONTROL_LOSS_RATE_ENV_VAR    "AERON_DEBUG_SEND_CONTROL_LOSS_RATE"
#define AERON_DEBUG_SEND_CONTROL_LOSS_SEED_ENV_VAR    "AERON_DEBUG_SEND_CONTROL_LOSS_SEED"
#define AERON_DEBUG_RECEIVE_DATA_LOSS_RATE_ENV_VAR    "AERON_DEBUG_RECEIVE_DATA_LOSS_RATE"
#define AERON_DEBUG_RECEIVE_DATA_LOSS_SEED_ENV_VAR    "AERON_DEBUG_RECEIVE_DATA_LOSS_SEED"
#define AERON_DEBUG_RECEIVE_CONTROL_LOSS_RATE_ENV_VAR "AERON_DEBUG_RECEIVE_CONTROL_LOSS_RATE"
#define AERON_DEBUG_RECEIVE_CONTROL_LOSS_SEED_ENV_VAR "AERON_DEBUG_RECEIVE_CONTROL_LOSS_SEED"

/*
 * Read the eight AERON_DEBUG_*_LOSS_{RATE,SEED} environment variables and
 * install random-loss generator suppliers on the driver context. Must be
 * called after aeron_driver_context_init and before endpoints are created;
 * cleanup runs automatically from aeron_driver_context_close.
 *
 * Returns 0 on success, -1 on allocation failure.
 */
int aeron_debug_channel_endpoint_configuration_install(aeron_driver_context_t *context);

/*
 * Release state installed by aeron_debug_channel_endpoint_configuration_install.
 * Safe to call if install was never called.
 */
void aeron_debug_channel_endpoint_configuration_cleanup(aeron_driver_context_t *context);

#endif //AERON_DEBUG_CHANNEL_ENDPOINT_CONFIGURATION_H
