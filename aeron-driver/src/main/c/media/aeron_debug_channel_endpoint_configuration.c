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

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "aeron_alloc.h"
#include "util/aeron_parse_util.h"
#include "media/aeron_debug_channel_endpoint_configuration.h"
#include "media/aeron_random_loss_generator.h"
#include "media/aeron_receive_channel_endpoint.h"
#include "media/aeron_send_channel_endpoint.h"

typedef struct aeron_debug_channel_endpoint_configuration_params_stct
{
    double data_rate;
    int64_t data_seed;
    double control_rate;
    int64_t control_seed;
}
aeron_debug_channel_endpoint_configuration_params_t;

static double aeron_debug_channel_endpoint_configuration_parse_rate(const char *name)
{
    const char *str = getenv(name);
    if (NULL == str || '\0' == *str)
    {
        return 0.0;
    }

    char *endptr = NULL;
    errno = 0;
    const double value = strtod(str, &endptr);

    if (0 != errno || '\0' != *endptr)
    {
        aeron_config_prop_warning(name, str);
        return 0.0;
    }

    return value < 0.0 ? 0.0 : (value > 1.0 ? 1.0 : value);
}

static int64_t aeron_debug_channel_endpoint_configuration_parse_seed(const char *name)
{
    const char *str = getenv(name);
    if (NULL == str || '\0' == *str)
    {
        return -1;
    }
    return aeron_config_parse_int64(name, str, -1, INT64_MIN, INT64_MAX);
}

static int aeron_debug_channel_endpoint_configuration_attach_random(
    const aeron_loss_generator_t **slot, double rate, int64_t seed)
{
    if (rate <= 0.0 || NULL != *slot)
    {
        return 0;
    }

    aeron_loss_generator_t *gen = NULL;
    if (aeron_random_loss_generator_create(&gen, rate, seed) < 0)
    {
        return -1;
    }

    gen->close = aeron_random_loss_generator_delete;
    *slot = gen;
    return 0;
}

static void aeron_debug_channel_endpoint_configuration_on_send_channel(
    void *clientd, aeron_send_channel_endpoint_t *endpoint)
{
    aeron_debug_channel_endpoint_configuration_params_t *params =
        (aeron_debug_channel_endpoint_configuration_params_t *)clientd;

    if (aeron_debug_channel_endpoint_configuration_attach_random(
        &endpoint->data_loss_generator, params->data_rate, params->data_seed) < 0)
    {
        fprintf(stderr, "INFO: failed to attach debug send data loss generator\n");
    }
    if (aeron_debug_channel_endpoint_configuration_attach_random(
        &endpoint->control_loss_generator, params->control_rate, params->control_seed) < 0)
    {
        fprintf(stderr, "INFO: failed to attach debug send control loss generator\n");
    }
}

static void aeron_debug_channel_endpoint_configuration_on_receive_channel(
    void *clientd, aeron_receive_channel_endpoint_t *endpoint)
{
    aeron_debug_channel_endpoint_configuration_params_t *params =
        (aeron_debug_channel_endpoint_configuration_params_t *)clientd;

    if (aeron_debug_channel_endpoint_configuration_attach_random(
        &endpoint->data_loss_generator, params->data_rate, params->data_seed) < 0)
    {
        fprintf(stderr, "INFO: failed to attach debug receive data loss generator\n");
    }
    if (aeron_debug_channel_endpoint_configuration_attach_random(
        &endpoint->control_loss_generator, params->control_rate, params->control_seed) < 0)
    {
        fprintf(stderr, "INFO: failed to attach debug receive control loss generator\n");
    }
}

int aeron_debug_channel_endpoint_configuration_install(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        return -1;
    }

    aeron_debug_channel_endpoint_configuration_cleanup(context);

    aeron_debug_channel_endpoint_configuration_params_t *send_params = NULL;
    aeron_debug_channel_endpoint_configuration_params_t *receive_params = NULL;

    if (aeron_alloc((void **)&send_params, sizeof(*send_params)) < 0)
    {
        return -1;
    }
    if (aeron_alloc((void **)&receive_params, sizeof(*receive_params)) < 0)
    {
        aeron_free(send_params);
        return -1;
    }

    send_params->data_rate = aeron_debug_channel_endpoint_configuration_parse_rate(
        AERON_DEBUG_SEND_DATA_LOSS_RATE_ENV_VAR);
    send_params->data_seed = aeron_debug_channel_endpoint_configuration_parse_seed(
        AERON_DEBUG_SEND_DATA_LOSS_SEED_ENV_VAR);
    send_params->control_rate = aeron_debug_channel_endpoint_configuration_parse_rate(
        AERON_DEBUG_SEND_CONTROL_LOSS_RATE_ENV_VAR);
    send_params->control_seed = aeron_debug_channel_endpoint_configuration_parse_seed(
        AERON_DEBUG_SEND_CONTROL_LOSS_SEED_ENV_VAR);

    receive_params->data_rate = aeron_debug_channel_endpoint_configuration_parse_rate(
        AERON_DEBUG_RECEIVE_DATA_LOSS_RATE_ENV_VAR);
    receive_params->data_seed = aeron_debug_channel_endpoint_configuration_parse_seed(
        AERON_DEBUG_RECEIVE_DATA_LOSS_SEED_ENV_VAR);
    receive_params->control_rate = aeron_debug_channel_endpoint_configuration_parse_rate(
        AERON_DEBUG_RECEIVE_CONTROL_LOSS_RATE_ENV_VAR);
    receive_params->control_seed = aeron_debug_channel_endpoint_configuration_parse_seed(
        AERON_DEBUG_RECEIVE_CONTROL_LOSS_SEED_ENV_VAR);

    aeron_driver_context_set_send_channel_loss_supplier(
        context, aeron_debug_channel_endpoint_configuration_on_send_channel, send_params);
    aeron_driver_context_set_receive_channel_loss_supplier(
        context, aeron_debug_channel_endpoint_configuration_on_receive_channel, receive_params);

    return 0;
}

void aeron_debug_channel_endpoint_configuration_cleanup(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        return;
    }

    if (aeron_debug_channel_endpoint_configuration_on_send_channel == context->send_channel_loss_supplier_func)
    {
        aeron_free(context->send_channel_loss_supplier_clientd);
        context->send_channel_loss_supplier_clientd = NULL;
        context->send_channel_loss_supplier_func = NULL;
    }

    if (aeron_debug_channel_endpoint_configuration_on_receive_channel == context->receive_channel_loss_supplier_func)
    {
        aeron_free(context->receive_channel_loss_supplier_clientd);
        context->receive_channel_loss_supplier_clientd = NULL;
        context->receive_channel_loss_supplier_func = NULL;
    }
}
