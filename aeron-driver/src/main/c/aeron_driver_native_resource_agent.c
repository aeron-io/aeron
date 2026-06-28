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

#include "aeron_driver_native_resource_agent.h"
#include "aeron_alloc.h"
#include "command/aeron_control_protocol.h"
#include "util/aeron_error.h"

typedef struct aeron_time_tracking_name_resolver_stct
{
    aeron_name_resolver_t delegate_resolver;
    aeron_driver_context_t *context;
}
aeron_time_tracking_name_resolver_t;

static int aeron_time_tracking_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    aeron_driver_context_t *context = time_tracking_resolver->context;
    int64_t begin_ns = context->nano_clock();
    aeron_duty_cycle_tracker_t *tracker = context->name_resolver_time_tracker;
    tracker->update(tracker->state, begin_ns);

    int result = time_tracking_resolver->delegate_resolver.resolve_func(
        &time_tracking_resolver->delegate_resolver,
        name,
        uri_param_name,
        is_re_resolution,
        address);

    int64_t end_ns = context->nano_clock();
    tracker->measure_and_update(tracker->state, end_ns);

    if (NULL != context->log.on_name_resolve)
    {
        struct sockaddr_storage *resolved_address = 0 <= result ? address : NULL;
        context->log.on_name_resolve(
            &time_tracking_resolver->delegate_resolver, end_ns - begin_ns, name, is_re_resolution, resolved_address);
    }

    return result;
}

static int aeron_time_tracking_name_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    aeron_driver_context_t *context = time_tracking_resolver->context;
    int64_t begin_ns = context->nano_clock();
    aeron_duty_cycle_tracker_t *tracker = context->name_resolver_time_tracker;
    tracker->update(tracker->state, begin_ns);

    int result = time_tracking_resolver->delegate_resolver.lookup_func(
        &time_tracking_resolver->delegate_resolver,
        name,
        uri_param_name,
        is_re_lookup,
        resolved_name);

    int64_t end_ns = context->nano_clock();
    tracker->measure_and_update(tracker->state, end_ns);

    if (NULL != context->log.on_name_lookup)
    {
        const char *result_name = 0 <= result ? *resolved_name : NULL;
        context->log.on_name_lookup(
            &time_tracking_resolver->delegate_resolver, end_ns - begin_ns, name, is_re_lookup, result_name);
    }

    return result;
}

static int aeron_time_tracking_name_resolver_start(aeron_name_resolver_t *resolver)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    int result = time_tracking_resolver->delegate_resolver.start_func(&time_tracking_resolver->delegate_resolver);
    if (result < 0)
    {
        AERON_APPEND_ERR("%s", "");
    }
    return result;
}

static int aeron_time_tracking_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    return time_tracking_resolver->delegate_resolver.do_work_func(&time_tracking_resolver->delegate_resolver, now_ms);
}

static int aeron_time_tracking_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_time_tracking_name_resolver_t *time_tracking_resolver = (aeron_time_tracking_name_resolver_t *)resolver->state;
    time_tracking_resolver->delegate_resolver.close_func(&time_tracking_resolver->delegate_resolver);
    aeron_free(time_tracking_resolver);
    return 0;
}

static void aeron_driver_native_resource_agent_signal_error(
    aeron_driver_native_resource_agent_t *native_resource_agent,
    aeron_driver_native_resource_agent_command_result_t *result)
{
    int errcode = aeron_errcode();
    const char* errmsg = aeron_errmsg();
    size_t errmsg_len = strlen(errmsg);

    if (aeron_alloc((void **)&result->payload.error.message, errmsg_len + 1) < 0)
    {
        // FIXME: It over-writes the original error message/code
        AERON_APPEND_ERR("failed to allocate error message: %s", aeron_errmsg());
        aeron_distinct_error_log_record(
            native_resource_agent->context->error_log, errcode, errmsg); // will be freed....
        aeron_err_clear();
    }
    else
    {
        memcpy(result->payload.error.message, errmsg, errmsg_len);
        result->payload.error.message[errmsg_len] = '\0';
    }
    result->payload.error.code = errcode;

    aeron_err_clear();

    AERON_SET_RELEASE(result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_FAILED);
}

static void aeron_driver_native_resource_agent_on_command(
    int32_t msg_type_id, const void *message, size_t size, void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;
    aeron_driver_native_resource_agent_proxy_cmd_t *cmd = (aeron_driver_native_resource_agent_proxy_cmd_t *)message;
    cmd->execute(native_resource_agent, cmd);
}

static int aeron_driver_native_resource_agent_free_end_of_life_resources(
    aeron_driver_native_resource_agent_t *native_resource_agent)
{
    const uint32_t limit = native_resource_agent->context->resource_free_limit;
    aeron_end_of_life_resource_t end_of_life_resource = { 0 };
    uint32_t count = 0;

    for (; count < limit; count++)
    {
        if (0 == aeron_deque_remove_first(&native_resource_agent->end_of_life_queue, (void *)&end_of_life_resource))
        {
            break;
        }

        if (!end_of_life_resource.free_func(end_of_life_resource.resource))
        {
            int64_t *counter = aeron_system_counter_addr(native_resource_agent->context->system_counters, AERON_SYSTEM_COUNTER_FREE_FAILS);
            aeron_counter_increment_release(counter);
            aeron_deque_add_last(&native_resource_agent->end_of_life_queue, (void *)&end_of_life_resource);
        }
    }

    return (int)count;
}

void aeron_driver_native_resource_agent_on_start(void *state, const char *role_name)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)state;

    if (NULL != native_resource_agent->name_resolver.start_func &&
        native_resource_agent->name_resolver.start_func(&native_resource_agent->name_resolver) < 0)
    {
        if (0 != aeron_errcode())
        {
            AERON_APPEND_ERR("%s", "failed to start name resolver");
            aeron_distinct_error_log_record(
                native_resource_agent->context->error_log, aeron_errcode(), aeron_errmsg());
            aeron_err_clear();
        }
        else
        {
            aeron_distinct_error_log_record(
                native_resource_agent->context->error_log,
                -AERON_ERROR_CODE_GENERIC_ERROR,
                "failed to start name resolver");
        }
    }
}

int aeron_driver_native_resource_agent_do_work(void *clientd)
{
    int work_count = 0;

    aeron_driver_native_resource_agent_t *native_resource_agent = clientd;

    const int64_t now = native_resource_agent->context->epoch_clock();
    work_count += native_resource_agent->name_resolver.do_work_func(&native_resource_agent->name_resolver, now);

    work_count += (int)aeron_spsc_rb_read(
        native_resource_agent->native_resource_agent_proxy.command_queue,
        aeron_driver_native_resource_agent_on_command,
        native_resource_agent,
    AERON_COMMAND_DRAIN_LIMIT);

    work_count += aeron_driver_native_resource_agent_free_end_of_life_resources(native_resource_agent);

    return work_count;
}

int aeron_driver_native_resource_agent_init(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_context_t *context)
{
    if (aeron_deque_init(&native_resource_agent->end_of_life_queue, 1024, sizeof(aeron_end_of_life_resource_t)))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    aeron_time_tracking_name_resolver_t *time_tracking_name_resolver = NULL;
    if (aeron_alloc((void **)&time_tracking_name_resolver, sizeof(aeron_time_tracking_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_time_tracking_name_resolver_t");
        return -1;
    }
    time_tracking_name_resolver->context = context;

    if (aeron_name_resolver_init(
        &time_tracking_name_resolver->delegate_resolver,
        context->name_resolver_init_args,
        context) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to init name resolver");
        aeron_free(time_tracking_name_resolver);
        return -1;
    }

    native_resource_agent->name_resolver.name = "time_tracking_name_resolver";
    native_resource_agent->name_resolver.resolve_func = aeron_time_tracking_name_resolver_resolve;
    native_resource_agent->name_resolver.lookup_func = aeron_time_tracking_name_resolver_lookup;
    native_resource_agent->name_resolver.start_func = aeron_time_tracking_name_resolver_start;
    native_resource_agent->name_resolver.do_work_func = aeron_time_tracking_name_resolver_do_work;
    native_resource_agent->name_resolver.close_func = aeron_time_tracking_name_resolver_close;
    native_resource_agent->name_resolver.state = time_tracking_name_resolver;

    native_resource_agent->context = context;

    native_resource_agent->native_resource_agent_proxy.native_resource_agent = native_resource_agent;
    native_resource_agent->native_resource_agent_proxy.command_queue = &context->native_resource_agent_command_queue;
    native_resource_agent->native_resource_agent_proxy.fail_counter =
        aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_NATIVE_RESOURCE_AGENT_PROXY_FAILS);

    return 0;
}

void aeron_driver_native_resource_agent_on_close(void *clientd)
{
    aeron_driver_native_resource_agent_t *native_resource_agent = (aeron_driver_native_resource_agent_t *)clientd;
    native_resource_agent->name_resolver.close_func(&native_resource_agent->name_resolver);

    aeron_end_of_life_resource_t end_of_life_resource;
    while (0 != aeron_deque_remove_first(&native_resource_agent->end_of_life_queue, &end_of_life_resource))
    {
        end_of_life_resource.free_func(end_of_life_resource.resource);
    }
    aeron_deque_close(&native_resource_agent->end_of_life_queue);
}

void aeron_driver_native_resource_agent_on_resolve_address(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd)
{
    aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t *resolve_cmd =
        (aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t *)cmd;
    if (aeron_name_resolver_resolve_host_and_port(
        &native_resource_agent->name_resolver,
        resolve_cmd->address_resolution_params->endpoint_name,
        resolve_cmd->address_resolution_params->uri_param_name,
        resolve_cmd->address_resolution_params->is_re_resolution,
        &resolve_cmd->address_resolution_params->resolved_address) < 0)
    {
        AERON_APPEND_ERR("%s", "address re-resolution failed");
        aeron_driver_native_resource_agent_signal_error(native_resource_agent, resolve_cmd->result);
    }
    else
    {
        // FIXME: Store resolved address in the success state instead of `aeron_name_resolver_async_resolve_t->resolved_address`
        AERON_SET_RELEASE(resolve_cmd->result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED);
    }
}

void aeron_driver_native_resource_agent_on_parse_udp_channel(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd)
{
    aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t *channel_cmd =
        (aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t *)cmd;
    if (aeron_udp_channel_finish_parse(&native_resource_agent->name_resolver, channel_cmd->async_parse) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to parse channel");
        aeron_driver_native_resource_agent_signal_error(native_resource_agent, channel_cmd->result);
    }
    else
    {
        AERON_SET_RELEASE(channel_cmd->result->state, AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED);
    }
}

void aeron_driver_native_resource_agent_on_free_resource(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd)
{
    aeron_driver_native_resource_agent_proxy_cmd_free_resource_t *free_cmd =
        (aeron_driver_native_resource_agent_proxy_cmd_free_resource_t *)cmd;

    if (aeron_deque_add_last(&native_resource_agent->end_of_life_queue, &free_cmd->resource) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to append EOL resource");
        aeron_distinct_error_log_record(
            native_resource_agent->context->error_log,
            aeron_errcode(),
            aeron_errmsg());
    }
}
