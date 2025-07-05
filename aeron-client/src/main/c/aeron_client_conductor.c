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

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

#include "aeron_client_conductor.h"
#include "aeron_publication.h"
#include "aeron_exclusive_publication.h"
#include "aeron_subscription.h"
#include "aeron_log_buffer.h"
#include "util/aeron_arrayutil.h"
#include "aeron_cnc_file_descriptor.h"
#include "aeron_image.h"
#include "aeron_counter.h"
#include "aeron_counters.h"

int aeron_client_conductor_init(aeron_client_conductor_t *conductor, aeron_context_t *context)
{
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)context->cnc_map.addr;

    if (aeron_broadcast_receiver_init(
        &conductor->to_client_buffer, aeron_cnc_to_clients_buffer(metadata), (size_t)metadata->to_clients_buffer_length) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init to_client_buffer");
        return -1;
    }

    if (aeron_mpsc_rb_init(
        &conductor->to_driver_buffer, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init to_driver_buffer");
        return -1;
    }

    if (aeron_counters_reader_init(
        &conductor->counters_reader,
        aeron_cnc_counters_metadata_buffer(metadata),
        (size_t)metadata->counter_metadata_buffer_length,
        aeron_cnc_counters_values_buffer(metadata),
        (size_t)metadata->counter_values_buffer_length) < 0)
    {
        AERON_SET_ERR(errno, "%s", "Unable to init counters_reader");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &conductor->log_buffer_by_id_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init log_buffer_by_id_map");
        return -1;
    }

    if (aeron_int64_to_ptr_hash_map_init(
        &conductor->resource_by_id_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init resource_by_id_map");
        return -1;
    }

    if (aeron_array_to_ptr_hash_map_init(
        &conductor->image_by_key_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init image_by_key_map");
        return -1;
    }

    conductor->client_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    conductor->client_name = aeron_context_get_client_name(context);

    conductor->available_counter_handlers.array = NULL;
    conductor->available_counter_handlers.capacity = 0;
    conductor->available_counter_handlers.length = 0;

    if (NULL != context->on_available_counter)
    {
        int result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(result, conductor->available_counter_handlers, aeron_on_available_counter_pair_t)
        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "Unable to ensure capacity for available_counter_handlers");
            return -1;
        }

        aeron_on_available_counter_pair_t *pair = &conductor->available_counter_handlers.array[0];

        pair->handler = context->on_available_counter;
        pair->clientd = context->on_available_counter_clientd;
        conductor->available_counter_handlers.length++;
    }

    conductor->unavailable_counter_handlers.array = NULL;
    conductor->unavailable_counter_handlers.capacity = 0;
    conductor->unavailable_counter_handlers.length = 0;

    if (NULL != context->on_unavailable_counter)
    {
        int result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(
            result, conductor->unavailable_counter_handlers, aeron_on_unavailable_counter_pair_t)
        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "Unable to ensure capacity for unavailable_counter_handlers");
            return -1;
        }

        aeron_on_unavailable_counter_pair_t *pair = &conductor->unavailable_counter_handlers.array[0];

        pair->handler = context->on_unavailable_counter;
        pair->clientd = context->on_unavailable_counter_clientd;
        conductor->unavailable_counter_handlers.length++;
    }

    conductor->close_handlers.array = NULL;
    conductor->close_handlers.capacity = 0;
    conductor->close_handlers.length = 0;

    if (NULL != context->on_close_client)
    {
        int result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(result, conductor->close_handlers, aeron_on_close_client_pair_t)
        if (result < 0)
        {
            AERON_APPEND_ERR("%s", "Unable to ensure capacity for close_handlers");
            return -1;
        }

        aeron_on_close_client_pair_t *pair = &conductor->close_handlers.array[0];

        pair->handler = context->on_close_client;
        pair->clientd = context->on_close_client_clientd;
        conductor->close_handlers.length++;
    }

    conductor->registering_resources.array = NULL;
    conductor->registering_resources.capacity = 0;
    conductor->registering_resources.length = 0;

    conductor->lingering_resources.array = NULL;
    conductor->lingering_resources.capacity = 0;
    conductor->lingering_resources.length = 0;

    conductor->heartbeat_timestamp.addr = NULL;
    conductor->heartbeat_timestamp.counter_id = AERON_NULL_COUNTER_ID;

    conductor->command_queue = &context->command_queue;
    conductor->epoch_clock = context->epoch_clock;
    conductor->nano_clock = context->nano_clock;

    conductor->error_handler = context->error_handler;
    conductor->error_handler_clientd = context->error_handler_clientd;

    conductor->error_frame_handler = context->error_frame_handler;
    conductor->error_frame_handler_clientd = context->error_frame_handler_clientd;

    conductor->on_new_publication = context->on_new_publication;
    conductor->on_new_publication_clientd = context->on_new_publication_clientd;

    conductor->on_new_exclusive_publication = context->on_new_exclusive_publication;
    conductor->on_new_exclusive_publication_clientd = context->on_new_exclusive_publication_clientd;

    conductor->on_new_subscription = context->on_new_subscription;
    conductor->on_new_subscription_clientd = context->on_new_subscription_clientd;

    conductor->driver_timeout_ms = context->driver_timeout_ms;
    conductor->driver_timeout_ns = context->driver_timeout_ms * 1000000;
    conductor->inter_service_timeout_ns = (uint64_t)metadata->client_liveness_timeout;
    conductor->keepalive_interval_ns = context->keepalive_interval_ns;
    conductor->resource_linger_duration_ns = context->resource_linger_duration_ns;
    conductor->idle_sleep_duration_ns = context->idle_sleep_duration_ns;

    long long now_ns = context->nano_clock();

    conductor->time_of_last_keepalive_ns = now_ns;
    conductor->time_of_last_service_ns = now_ns;

    conductor->invoker_mode = context->use_conductor_agent_invoker;
    conductor->pre_touch = context->pre_touch_mapped_memory;
    conductor->is_terminating = false;

    return 0;
}

void aeron_client_conductor_on_command(void *clientd, void *item)
{
    aeron_client_command_base_t *cmd = (aeron_client_command_base_t *)item;

    cmd->func(clientd, cmd);
}

void aeron_client_conductor_on_driver_response(int32_t type_id, uint8_t *buffer, size_t length, void *clientd)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    int result = 0;
    char error_message[AERON_ERROR_MAX_TOTAL_LENGTH] = "\0";

    switch (type_id)
    {
        case AERON_RESPONSE_ON_ERROR:
        {
            aeron_error_response_t *response = (aeron_error_response_t *)buffer;

            if (length < sizeof(aeron_error_response_t) ||
                length < (sizeof(aeron_error_response_t) + response->error_message_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_error(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *response = (aeron_publication_buffers_ready_t *)buffer;

            if (length < sizeof(aeron_publication_buffers_ready_t) ||
                length < (sizeof(aeron_publication_buffers_ready_t) + response->log_file_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_publication_ready(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_SUBSCRIPTION_READY:
        {
            aeron_subscription_ready_t *response = (aeron_subscription_ready_t *)buffer;

            if (length < sizeof(aeron_subscription_ready_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_subscription_ready(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
        {
            aeron_image_buffers_ready_t *response = (aeron_image_buffers_ready_t *)buffer;
            uint8_t *ptr = buffer + sizeof(aeron_image_buffers_ready_t);
            int32_t log_file_length, source_identity_length, aligned_log_file_length;
            const char *log_file = (const char *)(ptr + sizeof(int32_t));
            const char *source_identity;

            memcpy(&log_file_length, ptr, sizeof(int32_t));

            if (length < (sizeof(aeron_image_buffers_ready_t) + 2 * sizeof(int32_t)) ||
                length < (sizeof(aeron_image_buffers_ready_t) + 2 * sizeof(int32_t) + log_file_length))
            {
                goto malformed_command;
            }

            aligned_log_file_length = AERON_ALIGN(log_file_length, sizeof(int32_t));
            ptr += sizeof(int32_t) + aligned_log_file_length;
            memcpy(&source_identity_length, ptr, sizeof(int32_t));

            if (length <
                (sizeof(aeron_image_buffers_ready_t) +
                2 * sizeof(int32_t) +
                aligned_log_file_length +
                source_identity_length))
            {
                goto malformed_command;
            }

            source_identity = (const char *)(ptr + sizeof(int32_t));

            result = aeron_client_conductor_on_available_image(
                conductor, response, log_file_length, log_file, source_identity_length, source_identity);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
        {
            aeron_image_message_t *response = (aeron_image_message_t *)buffer;

            if (length < sizeof(aeron_image_message_t) ||
                length < (sizeof(aeron_image_message_t) + response->channel_length))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_unavailable_image(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_COUNTER_READY:
        {
            aeron_counter_update_t *response = (aeron_counter_update_t *)buffer;

            if (length < sizeof(aeron_counter_update_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_counter_ready(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_COUNTER:
        {
            aeron_counter_update_t *response = (aeron_counter_update_t *)buffer;

            if (length < sizeof(aeron_counter_update_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_unavailable_counter(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_CLIENT_TIMEOUT:
        {
            aeron_client_timeout_t *response = (aeron_client_timeout_t *)buffer;

            if (length < sizeof(aeron_client_timeout_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_client_timeout(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_OPERATION_SUCCESS:
        {
            aeron_operation_succeeded_t *response = (aeron_operation_succeeded_t *)buffer;

            if (length < sizeof(aeron_operation_succeeded_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_operation_success(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_ERROR:
        {
            aeron_publication_error_t *response = (aeron_publication_error_t *)buffer;

            if (length < sizeof(aeron_publication_error_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_error_frame(conductor, response);
            break;
        }

        case AERON_RESPONSE_ON_STATIC_COUNTER:
        {
            aeron_static_counter_response_t *response = (aeron_static_counter_response_t *)buffer;

            if (length < sizeof(aeron_static_counter_response_t))
            {
                goto malformed_command;
            }

            result = aeron_client_conductor_on_static_counter(conductor, response);
            break;
        }

        default:
        {
            AERON_CLIENT_FORMAT_BUFFER(error_message, "response=%x unknown", type_id);
            conductor->error_handler(
                conductor->error_handler_clientd, AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID, error_message);
            break;
        }
    }

    if (result < 0)
    {
        int os_errno = aeron_errcode();
        int code = os_errno < 0 ? -os_errno : AERON_ERROR_CODE_GENERIC_ERROR;
        conductor->error_handler(conductor->error_handler_clientd, code, aeron_errmsg());
    }

    return;

malformed_command:
    AERON_CLIENT_FORMAT_BUFFER(error_message, "command=%x too short: length=%" PRIu64, type_id, (uint64_t)length);
    conductor->error_handler(conductor->error_handler_clientd, AERON_ERROR_CODE_MALFORMED_COMMAND, error_message);
}

int aeron_client_conductor_check_liveness(aeron_client_conductor_t *conductor, long long now_ns)
{
    if (now_ns > (conductor->time_of_last_keepalive_ns + (long long)conductor->keepalive_interval_ns))
    {
        const long long last_keepalive_ms = aeron_mpsc_rb_consumer_heartbeat_time_value(&conductor->to_driver_buffer);
        const long long now_ms = conductor->epoch_clock();

        if (AERON_NULL_VALUE == last_keepalive_ms)
        {
            char buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            conductor->is_terminating = true;
            aeron_client_conductor_force_close_resources(conductor);
            snprintf(buffer, sizeof(buffer) - 1, "MediaDriver has been shutdown");
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_DRIVER_TIMEOUT, buffer);
        }
        else if (now_ms > (last_keepalive_ms + (long long)conductor->driver_timeout_ms))
        {
            char buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            conductor->is_terminating = true;
            aeron_client_conductor_force_close_resources(conductor);
            snprintf(buffer, sizeof(buffer) - 1,
                "MediaDriver keepalive: age=%" PRId64 "ms > timeout=%" PRId64 "ms",
                (int64_t)(now_ms - last_keepalive_ms),
                (int64_t)conductor->driver_timeout_ms);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_DRIVER_TIMEOUT, buffer);
        }

        if (AERON_NULL_COUNTER_ID == conductor->heartbeat_timestamp.counter_id)
        {
            const int32_t id = aeron_counter_heartbeat_timestamp_find_counter_id_by_registration_id(
                &conductor->counters_reader, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, conductor->client_id);

            if (AERON_NULL_COUNTER_ID != id)
            {
                aeron_counter_metadata_descriptor_t *counter_metadata = (aeron_counter_metadata_descriptor_t *)(
                    conductor->counters_reader.metadata + AERON_COUNTER_METADATA_OFFSET(id));

                char *extended_info;
                size_t available_label_length = AERON_COUNTER_MAX_LABEL_LENGTH - counter_metadata->label_length;
                if (aeron_alloc((void **)&extended_info, available_label_length) < 0)
                {
                    AERON_APPEND_ERR("%s", "");
                }
                size_t total_length = snprintf(
                    extended_info,
                    available_label_length,
                    " name=%s version=%s commit=%s",
                    conductor->client_name,
                    aeron_version_text(),
                    aeron_version_gitsha());
                size_t extended_info_length = AERON_MIN(total_length, available_label_length);

                memcpy(counter_metadata->label + counter_metadata->label_length, extended_info, extended_info_length);
                counter_metadata->label_length += (int32_t)extended_info_length;

                aeron_free(extended_info);

                conductor->heartbeat_timestamp.counter_id = id;
                conductor->heartbeat_timestamp.addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, conductor->heartbeat_timestamp.counter_id);

                aeron_counter_set_release(conductor->heartbeat_timestamp.addr, now_ms);
                conductor->time_of_last_keepalive_ns = now_ns;
            }
        }
        else
        {
            const int32_t id = conductor->heartbeat_timestamp.counter_id;
            if (!aeron_counter_heartbeat_timestamp_is_active(
                &conductor->counters_reader, id, AERON_COUNTER_CLIENT_HEARTBEAT_TIMESTAMP_TYPE_ID, conductor->client_id))
            {
                char buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

                conductor->is_terminating = true;
                aeron_client_conductor_force_close_resources(conductor);
                snprintf(buffer, sizeof(buffer) - 1, "unexpected close of heartbeat timestamp counter: %" PRId32, id);
                conductor->error_handler(conductor->error_handler_clientd, ETIMEDOUT, buffer);
                AERON_SET_ERR(ETIMEDOUT, "%s", buffer);
                return -1;
            }

            aeron_counter_set_release(conductor->heartbeat_timestamp.addr, now_ms);
            conductor->time_of_last_keepalive_ns = now_ns;
        }

        return 1;
    }

    return 0;
}

int aeron_client_conductor_check_lingering_resources(aeron_client_conductor_t *conductor, long long now_ns)
{
    int work_count = 0;

    /*
     * Currently, only images are lingered and only until refcnt is 0. Even in the case of client timeout,
     * etc. we let the application call aeron_close to clean up and shutdown the thread, etc.
     */
    for (size_t size = conductor->lingering_resources.length, last_index = size - 1, i = size; i > 0; i--)
    {
        size_t index = i - 1;
        aeron_client_managed_resource_t *resource = &conductor->lingering_resources.array[index];

        if (AERON_CLIENT_TYPE_IMAGE == resource->type)
        {
            aeron_image_t *image = resource->resource.image;

            if (INT64_MIN != image->removal_change_number)
            {
                if (!aeron_image_is_in_use_by_subscription(
                    image, aeron_subscription_last_image_list_change_number(image->subscription)))
                {
                    aeron_client_conductor_subscription_prune_image_lists(image->subscription);
                    aeron_image_decr_refcnt(image);
                    image->subscription = NULL;
                    image->removal_change_number = INT64_MIN;
                }
            }

            if (aeron_image_refcnt_acquire(image) <= 0)
            {
                aeron_client_conductor_release_log_buffer(conductor, image->log_buffer);
                aeron_image_delete(image);

                aeron_array_fast_unordered_remove(
                    (uint8_t *)conductor->lingering_resources.array,
                    sizeof(aeron_client_managed_resource_t),
                    index,
                    last_index);
                conductor->lingering_resources.length--;
                last_index--;
                work_count += 1;
            }
        }
    }

    return work_count;
}

int aeron_client_conductor_check_registering_resources(aeron_client_conductor_t *conductor, long long now_ns)
{
    int work_count = 0;

    for (size_t size = conductor->registering_resources.length, last_index = size - 1, i = size; i > 0; i--)
    {
        size_t index = i - 1;
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[index].resource;

        if (now_ns > resource->registration_deadline_ns)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                index,
                last_index);
            conductor->registering_resources.length--;
            last_index--;

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_TIMEOUT_MEDIA_DRIVER);

            work_count += 1;
        }
    }

    return work_count;
}

int aeron_client_conductor_on_check_timeouts(aeron_client_conductor_t *conductor)
{
    int work_count = 0, result = 0;
    const long long now_ns = conductor->nano_clock();

    if (now_ns > (conductor->time_of_last_service_ns + (long long)conductor->idle_sleep_duration_ns))
    {
        if (now_ns > (conductor->time_of_last_service_ns + (long long)conductor->inter_service_timeout_ns))
        {
            char buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            conductor->is_terminating = true;
            aeron_client_conductor_force_close_resources(conductor);
            snprintf(buffer, sizeof(buffer) - 1,
                "service interval exceeded in ns: timeout=%" PRId64 ", interval=%" PRId64,
                (int64_t)conductor->inter_service_timeout_ns,
                (int64_t)(now_ns - conductor->time_of_last_service_ns));
            conductor->error_handler(
                conductor->error_handler_clientd, AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT, buffer);
            return -1;
        }

        conductor->time_of_last_service_ns = now_ns;

        if ((result = aeron_client_conductor_check_liveness(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;

        if ((result = aeron_client_conductor_check_lingering_resources(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;

        if ((result = aeron_client_conductor_check_registering_resources(conductor, now_ns)) < 0)
        {
            return -1;
        }
        work_count += result;
    }

    return work_count;
}

int aeron_client_conductor_do_work(aeron_client_conductor_t *conductor)
{
    int work_count = 0, result = 0;

    if (conductor->is_terminating)
    {
        return 0;
    }

    work_count += (int)aeron_mpsc_concurrent_array_queue_drain(
        conductor->command_queue, aeron_client_conductor_on_command, conductor, 10);

    work_count += aeron_broadcast_receiver_receive(
        &conductor->to_client_buffer, aeron_client_conductor_on_driver_response, conductor);

    if ((result = aeron_client_conductor_on_check_timeouts(conductor)) < 0)
    {
        return work_count;
    }
    work_count += result;

    return work_count;
}

void aeron_client_conductor_delete_log_buffer(void *clientd, int64_t key, void *value)
{
    aeron_log_buffer_delete((aeron_log_buffer_t *)value);
}

void aeron_client_conductor_delete_image(void *clientd, const uint8_t *key, size_t key_len, void *value)
{
    aeron_image_delete((aeron_image_t *)value);
}

void aeron_client_conductor_delete_resource(void *clientd, int64_t key, void *value)
{
    aeron_client_command_base_t *base = (aeron_client_command_base_t *)value;

    switch (base->type)
    {
        case AERON_CLIENT_TYPE_PUBLICATION:
            aeron_publication_delete((aeron_publication_t *)value);
            break;

        case AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION:
            aeron_exclusive_publication_delete((aeron_exclusive_publication_t *)value);
            break;

        case AERON_CLIENT_TYPE_SUBSCRIPTION:
            aeron_subscription_delete((aeron_subscription_t *)value);
            break;

        case AERON_CLIENT_TYPE_COUNTER:
            aeron_counter_delete((aeron_counter_t *)value);
            break;

        case AERON_CLIENT_TYPE_IMAGE:
            aeron_image_delete((aeron_image_t *)value);
            break;

        case AERON_CLIENT_TYPE_LOGBUFFER:
        case AERON_CLIENT_TYPE_DESTINATION:
            break;
    }
}

void aeron_client_conductor_delete_lingering_resource(aeron_client_managed_resource_t *resource)
{
    if (AERON_CLIENT_TYPE_IMAGE == resource->type)
    {
        aeron_image_delete(resource->resource.image);
    }
}

void aeron_client_conductor_on_close(aeron_client_conductor_t *conductor)
{
    aeron_client_conductor_notify_close_handlers(conductor);

    aeron_client_conductor_on_cmd_client_close(conductor);

    aeron_int64_to_ptr_hash_map_for_each(
        &conductor->log_buffer_by_id_map, aeron_client_conductor_delete_log_buffer, NULL);
    aeron_int64_to_ptr_hash_map_for_each(
        &conductor->resource_by_id_map, aeron_client_conductor_delete_resource, NULL);
    aeron_array_to_ptr_hash_map_for_each(
        &conductor->image_by_key_map, aeron_client_conductor_delete_image, NULL);

    for (size_t i = 0, length = conductor->lingering_resources.length; i < length; i++)
    {
        aeron_client_conductor_delete_lingering_resource(&conductor->lingering_resources.array[i]);
    }

    aeron_broadcast_receiver_close(&conductor->to_client_buffer);

    aeron_int64_to_ptr_hash_map_delete(&conductor->log_buffer_by_id_map);
    aeron_int64_to_ptr_hash_map_delete(&conductor->resource_by_id_map);
    aeron_array_to_ptr_hash_map_delete(&conductor->image_by_key_map);
    aeron_free(conductor->registering_resources.array);
    aeron_free(conductor->lingering_resources.array);
    aeron_free(conductor->available_counter_handlers.array);
    aeron_free(conductor->unavailable_counter_handlers.array);
    aeron_free(conductor->close_handlers.array);
}

void aeron_client_conductor_force_close_image(void *clientd, const uint8_t *key, size_t key_len, void *value)
{
    aeron_image_close((aeron_image_t *)value);
}

void aeron_client_conductor_force_close_resource(void *clientd, int64_t key, void *value)
{
    aeron_client_command_base_t *base = (aeron_client_command_base_t *)value;

    switch (base->type)
    {
        case AERON_CLIENT_TYPE_PUBLICATION:
            aeron_publication_force_close((aeron_publication_t *)value);
            break;

        case AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION:
            aeron_exclusive_publication_force_close((aeron_exclusive_publication_t *)value);
            break;

        case AERON_CLIENT_TYPE_SUBSCRIPTION:
            aeron_subscription_force_close((aeron_subscription_t *)value);
            break;

        case AERON_CLIENT_TYPE_COUNTER:
            aeron_counter_force_close((aeron_counter_t *)value);
            break;

        case AERON_CLIENT_TYPE_IMAGE:
            aeron_image_close((aeron_image_t *)value);
            break;

        case AERON_CLIENT_TYPE_LOGBUFFER:
        case AERON_CLIENT_TYPE_DESTINATION:
            break;
    }
}

void aeron_client_conductor_force_close_resources(aeron_client_conductor_t *conductor)
{
    /*
     * loop will be terminating. So, will not process lingering, etc. Let the aeron_close() cleanup
     * everything when the app is ready, but we want to mark everything as closed so that it won't be used.
     */
    AERON_SET_RELEASE(conductor->is_closed, true);

    aeron_array_to_ptr_hash_map_for_each(
        &conductor->image_by_key_map, aeron_client_conductor_force_close_image, NULL);
    aeron_int64_to_ptr_hash_map_for_each(
        &conductor->resource_by_id_map, aeron_client_conductor_force_close_resource, NULL);
}

int aeron_client_conductor_linger_image(aeron_client_conductor_t *conductor, aeron_image_t *image)
{
    int ensure_capacity_result = 0;

    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, conductor->lingering_resources, aeron_client_managed_resource_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "lingering image: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return -1;
    }

    aeron_client_managed_resource_t *resource =
        &conductor->lingering_resources.array[conductor->lingering_resources.length++];
    resource->type = AERON_CLIENT_TYPE_IMAGE;
    resource->time_of_last_state_change_ns = conductor->nano_clock();
    resource->registration_id = image->key.correlation_id;
    resource->resource.image = image;
    image->is_lingering = true;

    return 0;
}

int aeron_client_conductor_linger_or_delete_all_images(
    aeron_client_conductor_t *conductor, aeron_subscription_t *subscription)
{
    volatile aeron_image_list_t *current_image_list = aeron_client_conductor_subscription_image_list(subscription);

    for (size_t i = 0; i < current_image_list->length; i++)
    {
        aeron_image_t *image = current_image_list->array[i];
        int64_t refcnt = aeron_image_decr_refcnt(image);

        aeron_array_to_ptr_hash_map_remove(
            &conductor->image_by_key_map,
            (const uint8_t *)&image->key,
            sizeof(aeron_image_key_t));

        if (refcnt <= 0)
        {
            if (NULL != subscription->on_unavailable_image)
            {
                subscription->on_unavailable_image(subscription->on_unavailable_image_clientd, subscription, image);
            }

            aeron_client_conductor_release_log_buffer(conductor, image->log_buffer);
            aeron_image_delete(image);
        }
        else if (!image->is_lingering)
        {
            if (aeron_client_conductor_linger_image(conductor, image) < 0)
            {
                return -1;
            }
        }
    }

    for (size_t i = 0, length = conductor->lingering_resources.length; i < length; i++)
    {
        aeron_client_managed_resource_t *resource = &conductor->lingering_resources.array[i];

        if (AERON_CLIENT_TYPE_IMAGE == resource->type)
        {
            aeron_image_t *lingering_image = resource->resource.image;

            if (subscription == lingering_image->subscription)
            {
                if (INT64_MIN != lingering_image->removal_change_number)
                {
                    aeron_image_decr_refcnt(lingering_image);
                    lingering_image->subscription = NULL;
                    lingering_image->removal_change_number = INT64_MIN;
                }
            }
        }
    }

    return 0;
}

void aeron_client_conductor_on_cmd_add_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_publication_t *async = (aeron_async_add_publication_t *)item;

    const size_t command_length = sizeof(aeron_publication_command_t) + async->uri_length;

    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_PUBLICATION,
        command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(
                err_buffer, sizeof(err_buffer) - 1, "ADD_PUBLICATION could not be sent (%s:%d)", __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_publication_command_t *command = (aeron_publication_command_t *)ptr;
    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(ptr + sizeof(aeron_publication_command_t), async->uri, (size_t)async->uri_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "publication registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

void aeron_client_conductor_on_cmd_close_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_publication_t *publication = (aeron_publication_t *)item;
    aeron_notification_t on_close_complete = publication->on_close_complete;
    void *on_close_complete_clientd = publication->on_close_complete_clientd;

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, publication->registration_id);

    aeron_client_conductor_release_log_buffer(conductor, publication->log_buffer);
    aeron_publication_delete(publication);

    if (NULL != on_close_complete)
    {
        on_close_complete(on_close_complete_clientd);
    }
}

void aeron_client_conductor_on_cmd_add_exclusive_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_exclusive_publication_t *async = (aeron_async_add_exclusive_publication_t *)item;

    const size_t command_length = sizeof(aeron_publication_command_t) + async->uri_length;

    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION,
        command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_EXCLUSIVE_PUBLICATION could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_publication_command_t *command = (aeron_publication_command_t *)ptr;
    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(ptr + sizeof(aeron_publication_command_t), async->uri, (size_t)async->uri_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "exclusive_publication registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

void aeron_client_conductor_on_cmd_close_exclusive_publication(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_exclusive_publication_t *publication = (aeron_exclusive_publication_t *)item;
    aeron_notification_t on_close_complete = publication->on_close_complete;
    void *on_close_complete_clientd = publication->on_close_complete_clientd;

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, publication->registration_id);

    aeron_client_conductor_release_log_buffer(conductor, publication->log_buffer);
    aeron_exclusive_publication_delete(publication);

    if (NULL != on_close_complete)
    {
        on_close_complete(on_close_complete_clientd);
    }
}

void aeron_client_conductor_on_cmd_add_subscription(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_subscription_t *async = (aeron_async_add_subscription_t *)item;

    const size_t command_length = sizeof(aeron_subscription_command_t) + async->uri_length;

    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer,
        AERON_COMMAND_ADD_SUBSCRIPTION,
        command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_SUBSCRIPTION could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_subscription_command_t *command = (aeron_subscription_command_t *)ptr;
    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->stream_id = async->stream_id;
    command->channel_length = async->uri_length;
    memcpy(ptr + sizeof(aeron_subscription_command_t), async->uri, (size_t)async->uri_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "subscription registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

void aeron_client_conductor_on_cmd_close_subscription(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_subscription_t *subscription = (aeron_subscription_t *)item;
    aeron_notification_t on_close_complete = subscription->on_close_complete;
    void *on_close_complete_clientd = subscription->on_close_complete_clientd;

    aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, subscription->registration_id);

    aeron_client_conductor_linger_or_delete_all_images(conductor, subscription);
    aeron_subscription_delete(subscription);

    if (NULL != on_close_complete)
    {
        on_close_complete(on_close_complete_clientd);
    }
}

void aeron_client_conductor_on_cmd_add_counter(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_counter_t *async = (aeron_async_add_counter_t *)item;

    const size_t command_length =
        sizeof(aeron_counter_command_t) +
        sizeof(int32_t) +
        (size_t)(AERON_ALIGN(async->counter.key_buffer_length, sizeof(int32_t))) +
        sizeof(int32_t) +
        (size_t)async->counter.label_buffer_length;

    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer, AERON_COMMAND_ADD_COUNTER, command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_COUNTER could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_counter_command_t *command = (aeron_counter_command_t *)ptr;
    char *cursor = (char *)(ptr + sizeof(aeron_counter_command_t));

    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->type_id = async->counter.type_id;

    memcpy(cursor, &async->counter.key_buffer_length, sizeof(int32_t));
    cursor += sizeof(int32_t);
    memcpy(cursor, async->counter.key_buffer, (size_t)async->counter.key_buffer_length);
    cursor += AERON_ALIGN(async->counter.key_buffer_length, sizeof(int32_t));
    memcpy(cursor, &async->counter.label_buffer_length, sizeof(int32_t));
    cursor += sizeof(int32_t);
    memcpy(cursor, async->counter.label_buffer, (size_t)async->counter.label_buffer_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "counter registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

void aeron_client_conductor_on_cmd_close_counter(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_counter_t *counter = (aeron_counter_t *)item;
    aeron_notification_t on_close_complete = counter->on_close_complete;
    void *on_close_complete_clientd = counter->on_close_complete_clientd;

    if (NULL != aeron_int64_to_ptr_hash_map_remove(&conductor->resource_by_id_map, counter->registration_id))
    {
        if (aeron_client_conductor_offer_remove_counter_command(conductor, counter->registration_id) < 0)
        {
            return;
        }
    }

    aeron_counter_delete(counter);

    if (NULL != on_close_complete)
    {
        on_close_complete(on_close_complete_clientd);
    }
}

void aeron_client_conductor_on_cmd_add_static_counter(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_add_counter_t *async = (aeron_async_add_counter_t *)item;

    const size_t command_length =
        sizeof(aeron_static_counter_command_t) +
        sizeof(int32_t) +
        (size_t)(AERON_ALIGN(async->counter.key_buffer_length, sizeof(int32_t))) +
        sizeof(int32_t) +
        (size_t)async->counter.label_buffer_length;

    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer, AERON_COMMAND_ADD_STATIC_COUNTER, command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "ADD_STATIC_COUNTER could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_static_counter_command_t *command = (aeron_static_counter_command_t *)ptr;
    char *cursor = (char *)(ptr + sizeof(aeron_static_counter_command_t));

    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->registration_id = async->counter.registration_id;
    command->type_id = async->counter.type_id;

    memcpy(cursor, &async->counter.key_buffer_length, sizeof(int32_t));
    cursor += sizeof(int32_t);
    memcpy(cursor, async->counter.key_buffer, (size_t)async->counter.key_buffer_length);
    cursor += AERON_ALIGN(async->counter.key_buffer_length, sizeof(int32_t));
    memcpy(cursor, &async->counter.label_buffer_length, sizeof(int32_t));
    cursor += sizeof(int32_t);
    memcpy(cursor, async->counter.label_buffer, (size_t)async->counter.label_buffer_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t)
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "static counter registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

static int aeron_client_conductor_get_async_registration_id(
    aeron_client_conductor_t *conductor,
    aeron_client_registering_resource_t *async,
    int64_t *resource_registration_id)
{
    int result = 0;

    switch (async->resource.base_resource->type)
    {
        case AERON_CLIENT_TYPE_PUBLICATION:
            *resource_registration_id = async->resource.publication->registration_id;
            break;

        case AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION:
            *resource_registration_id = async->resource.exclusive_publication->registration_id;
            break;

        case AERON_CLIENT_TYPE_SUBSCRIPTION:
            *resource_registration_id = async->resource.subscription->registration_id;
            break;

        case AERON_CLIENT_TYPE_IMAGE:
        case AERON_CLIENT_TYPE_LOGBUFFER:
        case AERON_CLIENT_TYPE_COUNTER:
        case AERON_CLIENT_TYPE_DESTINATION:
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];
            snprintf(
                err_buffer, sizeof(err_buffer) - 1, "DESTINATION command invalid resource (%s:%d)", __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, EINVAL, err_buffer);
            result = -1;
        }
    }

    return result;
}

static void aeron_client_conductor_on_cmd_destination(const void *clientd, const void *item, int32_t msg_type_id)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_destination_t *async = (aeron_async_destination_t *)item;

    int64_t resource_registration_id = 0;
    if (aeron_client_conductor_get_async_registration_id(conductor, async, &resource_registration_id) < 0)
    {
        return;
    }

    const size_t command_length = sizeof(aeron_destination_command_t) + async->uri_length;
    int ensure_capacity_result = 0, rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer,
        msg_type_id,
        command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(
                err_buffer, sizeof(err_buffer) - 1, "DESTINATION command could not be sent (%s:%d)", __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_destination_command_t *command = (aeron_destination_command_t *)ptr;
    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->registration_id = resource_registration_id;
    command->channel_length = async->uri_length;
    memcpy(ptr + sizeof(aeron_destination_command_t), async->uri, (size_t)async->uri_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "DESTINATION registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

static void aeron_client_conductor_on_cmd_destination_by_id(const void *clientd, const void *item, int32_t msg_type_id)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_async_destination_t *async = (aeron_async_destination_t *)item;

    int64_t resource_registration_id = 0;
    if (aeron_client_conductor_get_async_registration_id(conductor, async, &resource_registration_id) < 0)
    {
        return;
    }

    const size_t command_length = sizeof(aeron_destination_by_id_command_t);
    int ensure_capacity_result = 0;
    int rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer,
        msg_type_id,
        command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(
                err_buffer, sizeof(err_buffer) - 1, "DESTINATION command could not be sent (%s:%d)", __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            return;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_destination_by_id_command_t *command = (aeron_destination_by_id_command_t *)ptr;
    command->correlated.correlation_id = async->registration_id;
    command->correlated.client_id = conductor->client_id;
    command->resource_registration_id = resource_registration_id;
    command->destination_registration_id = resource_registration_id;

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    AERON_ARRAY_ENSURE_CAPACITY(
        ensure_capacity_result, conductor->registering_resources, aeron_client_registering_resource_entry_t);
    if (ensure_capacity_result < 0)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        snprintf(err_buffer, sizeof(err_buffer) - 1, "DESTINATION registering_resources: %s", aeron_errmsg());
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer);
        return;
    }

    conductor->registering_resources.array[conductor->registering_resources.length++].resource = async;
    async->registration_deadline_ns = (long long)(conductor->nano_clock() + conductor->driver_timeout_ns);
}

void aeron_client_conductor_on_cmd_add_destination(void *clientd, void *item)
{
    aeron_client_conductor_on_cmd_destination(clientd, item, AERON_COMMAND_ADD_DESTINATION);
}

void aeron_client_conductor_on_cmd_remove_destination(void *clientd, void *item)
{
    aeron_client_conductor_on_cmd_destination(clientd, item, AERON_COMMAND_REMOVE_DESTINATION);
}

void aeron_client_conductor_on_cmd_remove_destination_by_id(void *clientd, void *item)
{
    aeron_client_conductor_on_cmd_destination_by_id(clientd, item, AERON_COMMAND_REMOVE_DESTINATION_BY_ID);
}

void aeron_client_conductor_on_cmd_add_rcv_destination(void *clientd, void *item)
{
    aeron_client_conductor_on_cmd_destination(clientd, item, AERON_COMMAND_ADD_RCV_DESTINATION);
}

void aeron_client_conductor_on_cmd_remove_rcv_destination(void *clientd, void *item)
{
    aeron_client_conductor_on_cmd_destination(clientd, item, AERON_COMMAND_REMOVE_RCV_DESTINATION);
}

#define AERON_ON_HANDLER_ADD(p, c, m, t) \
{ \
    int ensure_capacity_result = 0; \
    AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, c->m, t) \
    if (ensure_capacity_result < 0) \
    { \
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH]; \
        snprintf(err_buffer, sizeof(err_buffer) - 1, "add " #t ": %s", aeron_errmsg()); \
        conductor->error_handler(conductor->error_handler_clientd, aeron_errcode(), err_buffer); \
        p = NULL; \
    } \
    p = &c->m.array[c->m.length++]; \
}

void aeron_client_conductor_on_cmd_handler(void *clientd, void *item)
{
    aeron_client_conductor_t *conductor = (aeron_client_conductor_t *)clientd;
    aeron_client_handler_cmd_t *cmd = (aeron_client_handler_cmd_t *)item;

    switch (cmd->type)
    {
        case AERON_CLIENT_HANDLER_ADD_AVAILABLE_COUNTER:
        {
            aeron_on_available_counter_pair_t *pair;

            AERON_ON_HANDLER_ADD(pair, conductor, available_counter_handlers, aeron_on_available_counter_pair_t);
            if (NULL != pair)
            {
                pair->handler = cmd->handler.on_available_counter;
                pair->clientd = cmd->clientd;
            }
            break;
        }

        case AERON_CLIENT_HANDLER_REMOVE_AVAILABLE_COUNTER:
        {
            for (size_t i = 0, size = conductor->available_counter_handlers.length, last_index = size - 1;
                i < size;
                i++)
            {
                aeron_on_available_counter_pair_t *pair = &conductor->available_counter_handlers.array[i];

                if (cmd->handler.on_available_counter == pair->handler && cmd->clientd == pair->clientd)
                {
                    aeron_array_fast_unordered_remove(
                        (uint8_t *)conductor->available_counter_handlers.array,
                        sizeof(aeron_on_available_counter_pair_t),
                        i,
                        last_index);
                    conductor->available_counter_handlers.length--;
                    break;
                }
            }
            break;
        }

        case AERON_CLIENT_HANDLER_ADD_UNAVAILABLE_COUNTER:
        {
            aeron_on_unavailable_counter_pair_t *pair;

            AERON_ON_HANDLER_ADD(pair, conductor, unavailable_counter_handlers, aeron_on_unavailable_counter_pair_t);
            if (NULL != pair)
            {
                pair->handler = cmd->handler.on_unavailable_counter;
                pair->clientd = cmd->clientd;
            }
            break;
        }

        case AERON_CLIENT_HANDLER_REMOVE_UNAVAILABLE_COUNTER:
        {
            for (size_t i = 0, size = conductor->unavailable_counter_handlers.length, last_index = size - 1;
                i < size;
                i++)
            {
                aeron_on_unavailable_counter_pair_t *pair = &conductor->unavailable_counter_handlers.array[i];

                if (cmd->handler.on_unavailable_counter == pair->handler && cmd->clientd == pair->clientd)
                {
                    aeron_array_fast_unordered_remove(
                        (uint8_t *)conductor->unavailable_counter_handlers.array,
                        sizeof(aeron_on_unavailable_counter_pair_t),
                        i,
                        last_index);
                    conductor->unavailable_counter_handlers.length--;
                    break;
                }
            }
            break;
        }

        case AERON_CLIENT_HANDLER_ADD_CLOSE_HANDLER:
        {
            aeron_on_close_client_pair_t *pair;

            AERON_ON_HANDLER_ADD(pair, conductor, close_handlers, aeron_on_close_client_pair_t);
            if (NULL != pair)
            {
                pair->handler = cmd->handler.on_close_handler;
                pair->clientd = cmd->clientd;
            }
            break;
        }

        case AERON_CLIENT_HANDLER_REMOVE_CLOSE_HANDLER:
        {
            for (size_t i = 0, size = conductor->close_handlers.length, last_index = size - 1; i < size; i++)
            {
                aeron_on_close_client_pair_t *pair = &conductor->close_handlers.array[i];

                if (cmd->handler.on_close_handler == pair->handler && cmd->clientd == pair->clientd)
                {
                    aeron_array_fast_unordered_remove(
                        (uint8_t *)conductor->close_handlers.array,
                        sizeof(aeron_on_close_client_pair_t),
                        i,
                        last_index);
                    conductor->close_handlers.length--;
                    break;
                }
            }
            break;
        }
    }

    AERON_SET_RELEASE(cmd->processed, true);
}

int aeron_client_conductor_command_offer(aeron_mpsc_concurrent_array_queue_t *command_queue, void *cmd)
{
    int fail_count = 0;

    while (AERON_OFFER_SUCCESS != aeron_mpsc_concurrent_array_queue_offer(command_queue, cmd))
    {
        if (++fail_count > AERON_CLIENT_COMMAND_QUEUE_FAIL_THRESHOLD)
        {
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", "could not offer to conductor command queue");
            return -1;
        }

        sched_yield();
    }

    return 0;
}

int aeron_client_conductor_async_add_publication(
    aeron_async_add_publication_t **async, aeron_client_conductor_t *conductor, const char *uri, int32_t stream_id)
{
    aeron_async_add_publication_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_publication_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate publication and uri_copy");
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_publication;
    cmd->command_base.item = NULL;
    cmd->resource.publication = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->stream_id = stream_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_PUBLICATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_publication(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_publication(
    aeron_client_conductor_t *conductor,
    aeron_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd)
{
    publication->command_base.func = aeron_client_conductor_on_cmd_close_publication;
    publication->command_base.item = NULL;
    publication->on_close_complete = on_close_complete;
    publication->on_close_complete_clientd = on_close_complete_clientd;

    if (aeron_client_conductor_offer_remove_publication_command(conductor, publication->registration_id, false) < 0)
    {
        return -1;
    }

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_publication(conductor, publication);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, publication) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async,
    aeron_client_conductor_t *conductor,
    const char *uri,
    int32_t stream_id)
{
    aeron_async_add_exclusive_publication_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_exclusive_publication_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate exclusive_publication and uri_copy");
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_exclusive_publication;
    cmd->command_base.item = NULL;
    cmd->resource.exclusive_publication = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->stream_id = stream_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_exclusive_publication(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_exclusive_publication(
    aeron_client_conductor_t *conductor,
    aeron_exclusive_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd)
{
    publication->command_base.func = aeron_client_conductor_on_cmd_close_exclusive_publication;
    publication->command_base.item = NULL;
    publication->on_close_complete = on_close_complete;
    publication->on_close_complete_clientd = on_close_complete_clientd;

    if (aeron_client_conductor_offer_remove_publication_command(
        conductor, publication->registration_id, publication->revoke_on_close) < 0)
    {
        return -1;
    }

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_exclusive_publication(conductor, publication);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, publication) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_client_conductor_t *conductor,
    const char *uri,
    int32_t stream_id,
    aeron_on_available_image_t on_available_image_handler,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image_handler,
    void *on_unavailable_image_clientd)
{
    aeron_async_add_subscription_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_subscription_t)) < 0 ||
        aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate subscription and uri_copy");
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_subscription;
    cmd->command_base.item = NULL;
    cmd->resource.subscription = NULL;
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->stream_id = stream_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->on_available_image = on_available_image_handler;
    cmd->on_available_image_clientd = on_available_image_clientd;
    cmd->on_unavailable_image = on_unavailable_image_handler;
    cmd->on_unavailable_image_clientd = on_unavailable_image_clientd;
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_SUBSCRIPTION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_subscription(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_close_subscription(
    aeron_client_conductor_t *conductor,
    aeron_subscription_t *subscription,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd)
{
    subscription->command_base.func = aeron_client_conductor_on_cmd_close_subscription;
    subscription->command_base.item = NULL;
    subscription->on_close_complete = on_close_complete;
    subscription->on_close_complete_clientd = on_close_complete_clientd;

    if (aeron_client_conductor_offer_remove_subscription_command(conductor, subscription->registration_id) < 0)
    {
        return -1;
    }

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_subscription(conductor, subscription);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, subscription) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_counter(
    aeron_async_add_counter_t **async,
    aeron_client_conductor_t *conductor,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length)
{
    aeron_async_add_counter_t *cmd = NULL;
    uint8_t *key_buffer_copy = NULL;
    char *label_buffer_copy = NULL;

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_counter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate counter");
        return -1;
    }

    if (aeron_alloc((void **)&key_buffer_copy, key_buffer_length) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate counter");
        goto error;
    }

    if (aeron_alloc((void **)&label_buffer_copy, label_buffer_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate counter");
        goto error;
    }

    if (key_buffer && key_buffer_length > 0)
    {
        memcpy(key_buffer_copy, key_buffer, key_buffer_length);
    }

    if (label_buffer && label_buffer_length > 0)
    {
        memcpy(label_buffer_copy, label_buffer, label_buffer_length);
    }

    label_buffer_copy[label_buffer_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_counter;
    cmd->command_base.item = NULL;
    cmd->resource.counter = NULL;
    cmd->error_message = NULL;
    cmd->uri = NULL;
    cmd->counter.key_buffer = key_buffer_copy;
    cmd->counter.label_buffer = label_buffer_copy;
    cmd->counter.key_buffer_length = key_buffer_length;
    cmd->counter.label_buffer_length = label_buffer_length;
    cmd->counter.type_id = type_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_COUNTER;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_counter(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd);
            aeron_free(key_buffer_copy);
            aeron_free(label_buffer_copy);
            return -1;
        }

        *async = cmd;
    }

    return 0;

error:
    aeron_free(cmd);
    aeron_free(key_buffer_copy);
    aeron_free(label_buffer_copy);
    return -1;
}

int aeron_client_conductor_async_close_counter(
    aeron_client_conductor_t *conductor,
    aeron_counter_t *counter,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd)
{
    counter->command_base.func = aeron_client_conductor_on_cmd_close_counter;
    counter->command_base.item = NULL;
    counter->on_close_complete = on_close_complete;
    counter->on_close_complete_clientd = on_close_complete_clientd;

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_close_counter(conductor, counter);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, counter) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_async_add_static_counter(
    aeron_async_add_counter_t **async,
    aeron_client_conductor_t *conductor,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length,
    int64_t registration_id)
{
    aeron_async_add_counter_t *cmd = NULL;
    uint8_t *key_buffer_copy = NULL;
    char *label_buffer_copy = NULL;

    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_add_counter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate static counter");
        return -1;
    }

    if (aeron_alloc((void **)&key_buffer_copy, key_buffer_length) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate static counter");
        aeron_free(cmd);
        return -1;
    }

    if (aeron_alloc((void **)&label_buffer_copy, label_buffer_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate static counter");
        aeron_free(cmd);
        aeron_free(key_buffer_copy);
        return -1;
    }

    if (key_buffer && key_buffer_length > 0)
    {
        memcpy(key_buffer_copy, key_buffer, key_buffer_length);
    }

    if (label_buffer && label_buffer_length > 0)
    {
        memcpy(label_buffer_copy, label_buffer, label_buffer_length);
    }

    label_buffer_copy[label_buffer_length] = '\0';

    cmd->command_base.func = aeron_client_conductor_on_cmd_add_static_counter;
    cmd->command_base.item = NULL;
    cmd->resource.counter = NULL;
    cmd->error_message = NULL;
    cmd->uri = NULL;
    cmd->counter.key_buffer = key_buffer_copy;
    cmd->counter.label_buffer = label_buffer_copy;
    cmd->counter.key_buffer_length = key_buffer_length;
    cmd->counter.label_buffer_length = label_buffer_length;
    cmd->counter.type_id = type_id;
    cmd->counter.registration_id = registration_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_COUNTER;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        aeron_client_conductor_on_cmd_add_static_counter(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd);
            aeron_free(key_buffer_copy);
            aeron_free(label_buffer_copy);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

static int aeron_client_conductor_async_destination(
    aeron_async_destination_t **async,
    union aeron_client_registering_resource_un *resource,
    const char *uri,
    aeron_client_conductor_t *conductor,
    void (*destination_func)(void *clientd, void *command))
{
    aeron_async_destination_t *cmd = NULL;
    char *uri_copy = NULL;
    size_t uri_length = strlen(uri);

    *async = NULL;

    if (aeron_alloc((void **)&uri_copy, uri_length + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate uri_copy");
        return -1;
    }

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_destination_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate destination");
        return -1;
    }

    memcpy(uri_copy, uri, uri_length);
    uri_copy[uri_length] = '\0';

    cmd->command_base.func = destination_func;
    cmd->command_base.item = NULL;
    memcpy(&cmd->resource, resource, sizeof(union aeron_client_registering_resource_un));
    cmd->error_message = NULL;
    cmd->uri = uri_copy;
    cmd->uri_length = (int32_t)uri_length;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_DESTINATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        destination_func(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

static int aeron_client_conductor_async_destination_by_id(
    aeron_async_destination_t **async,
    union aeron_client_registering_resource_un *resource,
    int64_t destination_registration_id,
    aeron_client_conductor_t *conductor,
    void (*destination_func)(void *clientd, void *command))
{
    aeron_async_destination_by_id_t *cmd = NULL;
    *async = NULL;

    if (aeron_alloc((void **)&cmd, sizeof(aeron_async_destination_by_id_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate destination");
        return -1;
    }

    cmd->command_base.func = destination_func;
    cmd->command_base.item = NULL;
    memcpy(&cmd->resource, resource, sizeof(union aeron_client_registering_resource_un));
    cmd->error_message = NULL;
    cmd->destination_registration_id = destination_registration_id;
    cmd->registration_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    cmd->registration_status = AERON_CLIENT_AWAITING_MEDIA_DRIVER;
    cmd->type = AERON_CLIENT_TYPE_DESTINATION;

    if (conductor->invoker_mode)
    {
        *async = cmd;
        destination_func(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            aeron_free(cmd->uri);
            aeron_free(cmd);
            return -1;
        }

        *async = cmd;
    }

    return 0;
}

int aeron_client_conductor_async_add_publication_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_publication_t *publication,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.publication = publication;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_add_destination);
}

int aeron_client_conductor_async_remove_publication_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_publication_t *publication,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.publication = publication;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_remove_destination);
}

int aeron_client_conductor_async_remove_publication_destination_by_id(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_publication_t *publication,
    int64_t destination_registration_id)
{
    union aeron_client_registering_resource_un resource;
    resource.publication = publication;

    return aeron_client_conductor_async_destination_by_id(
        async,
        &resource,
        destination_registration_id,
        conductor,
        aeron_client_conductor_on_cmd_remove_destination_by_id);
}

int aeron_client_conductor_async_add_exclusive_publication_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_exclusive_publication_t *publication,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.exclusive_publication = publication;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_add_destination);
}

int aeron_client_conductor_async_remove_exclusive_publication_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_exclusive_publication_t *publication,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.exclusive_publication = publication;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_remove_destination);
}

int aeron_client_conductor_async_remove_exclusive_publication_destination_by_id(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_exclusive_publication_t *publication,
    int64_t destination_registration_id)
{
    union aeron_client_registering_resource_un resource;
    resource.exclusive_publication = publication;

    return aeron_client_conductor_async_destination_by_id(
        async,
        &resource,
        destination_registration_id,
        conductor,
        aeron_client_conductor_on_cmd_remove_destination_by_id);
}

int aeron_client_conductor_async_add_subscription_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_subscription_t *subscription,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.subscription = subscription;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_add_rcv_destination);
}

int aeron_client_conductor_async_remove_subscription_destination(
    aeron_async_destination_t **async,
    aeron_client_conductor_t *conductor,
    aeron_subscription_t *subscription,
    const char *uri)
{
    union aeron_client_registering_resource_un resource;
    resource.subscription = subscription;

    return aeron_client_conductor_async_destination(
        async, &resource, uri, conductor, aeron_client_conductor_on_cmd_remove_rcv_destination);
}

int aeron_client_conductor_async_handler(aeron_client_conductor_t *conductor, aeron_client_handler_cmd_t *cmd)
{
    cmd->command_base.func = aeron_client_conductor_on_cmd_handler;
    cmd->command_base.item = NULL;

    if (conductor->invoker_mode)
    {
        aeron_client_conductor_on_cmd_handler(conductor, cmd);
    }
    else
    {
        if (aeron_client_conductor_command_offer(conductor->command_queue, cmd) < 0)
        {
            return -1;
        }
    }

    return 0;
}

int aeron_client_conductor_on_error(aeron_client_conductor_t *conductor, aeron_error_response_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->offending_command_correlation_id == resource->registration_id)
        {
            resource->error_message_length = response->error_message_length;
            resource->error_code = response->error_code;

            if (aeron_alloc((void **)&resource->error_message, (size_t)response->error_message_length + 1) < 0)
            {
                AERON_APPEND_ERR("%s", "Unable to allocate error message");
                return -1;
            }

            memcpy(
                resource->error_message,
                (const char *)response + sizeof(aeron_error_response_t),
                (size_t)resource->error_message_length);
            resource->error_message[resource->error_message_length] = '\0';

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_ERRORED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}

int aeron_client_conductor_get_or_create_log_buffer(
    aeron_client_conductor_t *conductor,
    aeron_log_buffer_t **log_buffer,
    const char *log_file,
    int64_t original_registration_id,
    bool pre_touch)
{
    if (NULL == (*log_buffer = aeron_int64_to_ptr_hash_map_get(
        &conductor->log_buffer_by_id_map, original_registration_id)))
    {
        if (aeron_log_buffer_create(log_buffer, log_file, original_registration_id, pre_touch) < 0)
        {
            return -1;
        }

        if (aeron_int64_to_ptr_hash_map_put(
            &conductor->log_buffer_by_id_map, original_registration_id, *log_buffer) < 0)
        {
            AERON_SET_ERR(errno, "%s", "Unable to insert into log_buffer_by_id_map");
            aeron_log_buffer_delete(*log_buffer);
            return -1;
        }
    }

    (*log_buffer)->refcnt++;

    return 0;
}

int aeron_client_conductor_release_log_buffer(aeron_client_conductor_t *conductor, aeron_log_buffer_t *log_buffer)
{
    if (--log_buffer->refcnt <= 0)
    {
        int64_t correlationId = log_buffer->correlation_id;
        aeron_int64_to_ptr_hash_map_remove(&conductor->log_buffer_by_id_map, correlationId);

        aeron_log_buffer_delete(log_buffer);
    }

    return 0;
}

int aeron_client_conductor_on_publication_ready(
    aeron_client_conductor_t *conductor, aeron_publication_buffers_ready_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            char log_file[AERON_MAX_PATH];
            const char *channel = resource->uri;
            bool is_exclusive = (AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION == resource->type) ? true : false;

            memcpy(
                log_file,
                (const char *)response + sizeof(aeron_publication_buffers_ready_t),
                (size_t)response->log_file_length);
            log_file[response->log_file_length] = '\0';

            aeron_log_buffer_t *log_buffer;

            if (aeron_client_conductor_get_or_create_log_buffer(
                conductor, &log_buffer, log_file, response->registration_id, conductor->pre_touch) < 0)
            {
                return -1;
            }

            if (is_exclusive)
            {
                aeron_exclusive_publication_t *publication = NULL;
                int64_t *position_limit_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->position_limit_counter_id);
                int64_t *channel_status_indicator_addr = 0 <= response->channel_status_indicator_id ?
                    aeron_counters_reader_addr(&conductor->counters_reader, response->channel_status_indicator_id) :
                    NULL;


                if (aeron_exclusive_publication_create(
                    &publication,
                    conductor,
                    resource->uri,
                    resource->stream_id,
                    response->session_id,
                    response->position_limit_counter_id,
                    position_limit_addr,
                    response->channel_status_indicator_id,
                    channel_status_indicator_addr,
                    log_buffer,
                    response->registration_id,
                    response->correlation_id) < 0)
                {
                    return -1;
                }

                resource->uri = NULL;
                resource->resource.exclusive_publication = publication;

                if (aeron_int64_to_ptr_hash_map_put(
                    &conductor->resource_by_id_map, resource->registration_id, publication) < 0)
                {
                    AERON_APPEND_ERR(
                        "Unable to insert publication into resource_by_id_map: resource_id: %" PRId64,
                        resource->registration_id);
                    return -1;
                }
            }
            else
            {
                aeron_publication_t *publication = NULL;
                int64_t *position_limit_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->position_limit_counter_id);
                int64_t *channel_status_indicator_addr = aeron_counters_reader_addr(
                    &conductor->counters_reader, response->channel_status_indicator_id);

                if (aeron_publication_create(
                    &publication,
                    conductor,
                    resource->uri,
                    resource->stream_id,
                    response->session_id,
                    response->position_limit_counter_id,
                    position_limit_addr,
                    response->channel_status_indicator_id,
                    channel_status_indicator_addr,
                    log_buffer,
                    response->registration_id,
                    response->correlation_id) < 0)
                {
                    return -1;
                }

                resource->uri = NULL;
                resource->resource.publication = publication;

                if (aeron_int64_to_ptr_hash_map_put(
                    &conductor->resource_by_id_map, resource->registration_id, publication) < 0)
                {
                    AERON_APPEND_ERR(
                        "Unable to insert publication into resource_by_id_map: resource_id: %" PRId64,
                        resource->registration_id);
                    return -1;
                }
            }

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);

            if (is_exclusive && NULL != conductor->on_new_exclusive_publication)
            {
                conductor->on_new_exclusive_publication(
                    conductor->on_new_exclusive_publication_clientd,
                    resource,
                    channel,
                    response->stream_id,
                    response->session_id,
                    response->correlation_id);
            }
            else if (NULL != conductor->on_new_publication)
            {
                conductor->on_new_publication(
                    conductor->on_new_publication_clientd,
                    resource,
                    channel,
                    response->stream_id,
                    response->session_id,
                    response->correlation_id);
            }
            break;
        }
    }

    return 0;
}

int aeron_client_conductor_on_subscription_ready(
    aeron_client_conductor_t *conductor, aeron_subscription_ready_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            const char *channel = resource->uri;
            aeron_subscription_t *subscription;
            int64_t *channel_status_indicator_addr = 0 <= response->channel_status_indicator_id ?
                aeron_counters_reader_addr(&conductor->counters_reader, response->channel_status_indicator_id) :
                NULL;
            int32_t stream_id = resource->stream_id;

            if (aeron_subscription_create(
                &subscription,
                conductor,
                resource->uri,
                stream_id,
                resource->registration_id,
                response->channel_status_indicator_id,
                channel_status_indicator_addr,
                resource->on_available_image,
                resource->on_available_image_clientd,
                resource->on_unavailable_image,
                resource->on_unavailable_image_clientd) < 0)
            {
                return -1;
            }

            resource->uri = NULL;
            resource->resource.subscription = subscription;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            if (aeron_int64_to_ptr_hash_map_put(
                &conductor->resource_by_id_map, resource->registration_id, subscription) < 0)
            {
                AERON_APPEND_ERR(
                    "Unable to insert subscription into resource_by_id_map: resource_id: %" PRId64,
                    resource->registration_id);
                return -1;
            }

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);

            if (NULL != conductor->on_new_subscription)
            {
                conductor->on_new_subscription(
                    conductor->on_new_subscription_clientd,
                    resource,
                    channel,
                    stream_id,
                    response->correlation_id);
            }
            break;
        }
    }

    return 0;
}

int aeron_client_conductor_on_operation_success(
    aeron_client_conductor_t *conductor, aeron_operation_succeeded_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}

struct aeron_client_conductor_clientd_stct
{
    aeron_client_conductor_t *conductor;
    void *clientd;
};
typedef struct aeron_client_conductor_clientd_stct aeron_client_conductor_clientd_t;

void aeron_client_conductor_forward_error(void *clientd, int64_t key, void *value)
{
    aeron_client_conductor_clientd_t *conductor_clientd = (aeron_client_conductor_clientd_t *)clientd;
    aeron_client_conductor_t *conductor = conductor_clientd->conductor;
    aeron_publication_error_t *response = (aeron_publication_error_t *)conductor_clientd->clientd;
    aeron_client_command_base_t *resource = (aeron_client_command_base_t *)value;

    const bool is_publication = AERON_CLIENT_TYPE_PUBLICATION == resource->type &&
        ((aeron_publication_t *)resource)->original_registration_id == response->registration_id;
    const bool is_exclusive_publication = AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION == resource->type &&
        ((aeron_exclusive_publication_t *)resource)->original_registration_id == response->registration_id;

    if (is_publication || is_exclusive_publication)
    {
        conductor->error_frame_handler(
            conductor->error_frame_handler_clientd, (aeron_publication_error_values_t *)response);
    }
}

#ifdef _MSC_VER
#define _Static_assert static_assert
#endif

_Static_assert(
    sizeof(aeron_publication_error_t) == sizeof(aeron_publication_error_values_t),
    "sizeof(aeron_publication_error_t) must be equal to sizeof(aeron_publication_error_values_t)");
_Static_assert(
    offsetof(aeron_publication_error_t, registration_id) == offsetof(aeron_publication_error_values_t, registration_id),
    "offsetof(aeron_publication_error_t, registration_id) must match offsetof(aeron_publication_error_values_t, registration_id)");
_Static_assert(
    offsetof(aeron_publication_error_t, destination_registration_id) == offsetof(aeron_publication_error_values_t, destination_registration_id),
    "offsetof(aeron_publication_error_t, destination_registration_id) must match offsetof(aeron_publication_error_values_t, destination_registration_id)");
_Static_assert(
    offsetof(aeron_publication_error_t, session_id) == offsetof(aeron_publication_error_values_t, session_id),
    "offsetof(aeron_publication_error_t, session_id) must match offsetof(aeron_publication_error_values_t, session_id)");
_Static_assert(
    offsetof(aeron_publication_error_t, stream_id) == offsetof(aeron_publication_error_values_t, stream_id),
    "offsetof(aeron_publication_error_t, stream_id) must match offsetof(aeron_publication_error_values_t, stream_id)");
_Static_assert(
    offsetof(aeron_publication_error_t, receiver_id) == offsetof(aeron_publication_error_values_t, receiver_id),
    "offsetof(aeron_publication_error_t, receiver_id) must match offsetof(aeron_publication_error_values_t, receiver_id)");
_Static_assert(
    offsetof(aeron_publication_error_t, group_tag) == offsetof(aeron_publication_error_values_t, group_tag),
    "offsetof(aeron_publication_error_t, group_tag) must match offsetof(aeron_publication_error_values_t, group_tag)");
_Static_assert(
    offsetof(aeron_publication_error_t, address_type) == offsetof(aeron_publication_error_values_t, address_type),
    "offsetof(aeron_publication_error_t, address_type) must match offsetof(aeron_publication_error_values_t, address_type)");
_Static_assert(
    offsetof(aeron_publication_error_t, source_port) == offsetof(aeron_publication_error_values_t, source_port),
    "offsetof(aeron_publication_error_t, address_port) must match offsetof(aeron_publication_error_values_t, address_port)");
_Static_assert(
    offsetof(aeron_publication_error_t, source_address) == offsetof(aeron_publication_error_values_t, source_address),
    "offsetof(aeron_publication_error_t, source_address) must match offsetof(aeron_publication_error_values_t, source_address)");
_Static_assert(
    offsetof(aeron_publication_error_t, error_code) == offsetof(aeron_publication_error_values_t, error_code),
    "offsetof(aeron_publication_error_t, error_code) must match offsetof(aeron_publication_error_values_t, error_code)");
_Static_assert(
    offsetof(aeron_publication_error_t, error_message_length) == offsetof(aeron_publication_error_values_t, error_message_length),
    "offsetof(aeron_publication_error_t, error_message_length) must match offsetof(aeron_publication_error_values_t, error_message_length)");
_Static_assert(
    offsetof(aeron_publication_error_t, error_message) == offsetof(aeron_publication_error_values_t, error_message),
    "offsetof(aeron_publication_error_t, error_message) must match offsetof(aeron_publication_error_values_t, error_message)");

int aeron_client_conductor_on_error_frame(aeron_client_conductor_t *conductor, aeron_publication_error_t *response)
{
    aeron_client_conductor_clientd_t clientd = {
        .conductor = conductor,
        .clientd = response
    };

    aeron_int64_to_ptr_hash_map_for_each(
        &conductor->resource_by_id_map, aeron_client_conductor_forward_error, (void *)&clientd);

    return 0;
}

int aeron_publication_error_values_copy(aeron_publication_error_values_t **dst, aeron_publication_error_values_t *src)
{
    if (NULL == src)
    {
        AERON_SET_ERR(-1, "%s", "src must not be NULL");
        return -1;
    }

    if (NULL == dst)
    {
        AERON_SET_ERR(-1, "%s", "dst must not be NULL");
        return -1;
    }

    size_t error_values_size = sizeof(*src) + (size_t)src->error_message_length;
    if (aeron_alloc((void **)dst, error_values_size) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    memcpy((void *)*dst, (void *)src, error_values_size);
    return 0;
}

void aeron_publication_error_values_delete(aeron_publication_error_values_t *to_delete)
{
    aeron_free(to_delete);
}

aeron_subscription_t *aeron_client_conductor_find_subscription_by_id(
    aeron_client_conductor_t *conductor, int64_t registration_id)
{
    aeron_subscription_t *subscription = aeron_int64_to_ptr_hash_map_get(
        &conductor->resource_by_id_map, registration_id);

    if (NULL != subscription && AERON_CLIENT_TYPE_SUBSCRIPTION == subscription->command_base.type)
    {
        return subscription;
    }

    return NULL;
}

int aeron_client_conductor_on_available_image(
    aeron_client_conductor_t *conductor,
    aeron_image_buffers_ready_t *response,
    int32_t log_file_length,
    const char *log_file,
    int32_t source_identity_length,
    const char *source_identity)
{
    aeron_subscription_t *subscription = aeron_client_conductor_find_subscription_by_id(
        conductor, response->subscriber_registration_id);

    if (NULL != subscription)
    {
        char log_file_str[AERON_MAX_PATH], source_identity_str[AERON_MAX_PATH];

        memcpy(log_file_str, log_file, (size_t)log_file_length);
        log_file_str[log_file_length] = '\0';
        memcpy(source_identity_str, source_identity, (size_t)source_identity_length);
        source_identity_str[source_identity_length] = '\0';

        aeron_log_buffer_t *log_buffer;

        if (aeron_client_conductor_get_or_create_log_buffer(
            conductor, &log_buffer, log_file_str, response->correlation_id, conductor->pre_touch) < 0)
        {
            return -1;
        }

        aeron_image_t *image;
        int64_t *subscriber_position = aeron_counters_reader_addr(
            &conductor->counters_reader, response->subscriber_position_id);

        if (aeron_image_create(
            &image,
            subscription,
            conductor,
            log_buffer,
            response->subscriber_position_id,
            subscriber_position,
            response->correlation_id,
            response->session_id,
            source_identity_str,
            (size_t)source_identity_length) < 0)
        {
            return -1;
        }

        if (aeron_array_to_ptr_hash_map_put(
            &conductor->image_by_key_map,
            (const uint8_t *)&image->key,
            sizeof(aeron_image_key_t),
            image) < 0)
        {
            AERON_APPEND_ERR(
                "Unable to put into image_by_key_map, correlation_id: %" PRId64 ", subscription_registration_id: %" PRId64,
                response->correlation_id,
                response->subscriber_registration_id);
            return -1;
        }

        if (aeron_client_conductor_subscription_add_image(subscription, image) < 0)
        {
            return -1;
        }

        aeron_client_conductor_subscription_prune_image_lists(subscription);

        if (NULL != subscription->on_available_image)
        {
            subscription->on_available_image(subscription->on_available_image_clientd, subscription, image);
        }
    }

    return 0;
}

int aeron_client_conductor_on_unavailable_image(aeron_client_conductor_t *conductor, aeron_image_message_t *response)
{
    aeron_subscription_t *subscription = aeron_client_conductor_find_subscription_by_id(
        conductor, response->subscription_registration_id);

    if (NULL != subscription)
    {
        aeron_image_key_t key =
            {
                .correlation_id = response->correlation_id,
                .subscription_registration_id = response->subscription_registration_id,
            };

        aeron_image_t *image = aeron_array_to_ptr_hash_map_remove(
            &conductor->image_by_key_map,
            (const uint8_t *)&key,
            sizeof(aeron_image_key_t));

        if (NULL != image)
        {
            if (aeron_client_conductor_subscription_remove_image(subscription, image) < 0)
            {
                return -1;
            }

            aeron_client_conductor_subscription_prune_image_lists(subscription);

            if (NULL != subscription->on_unavailable_image)
            {
                subscription->on_unavailable_image(subscription->on_unavailable_image_clientd, subscription, image);
            }

            if (aeron_client_conductor_linger_image(conductor, image) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_client_conductor_on_counter_ready(aeron_client_conductor_t *conductor, aeron_counter_update_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            aeron_counter_t *counter;
            int64_t *counter_addr = aeron_counters_reader_addr(&conductor->counters_reader, response->counter_id);

            if (aeron_counter_create(
                &counter,
                conductor,
                response->correlation_id,
                response->counter_id,
                counter_addr) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            resource->resource.counter = counter;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            if (aeron_int64_to_ptr_hash_map_put(
                &conductor->resource_by_id_map, resource->registration_id, counter) < 0)
            {
                AERON_APPEND_ERR(
                    "Unable to put counter into resource_by_id_map, registration_id: %" PRId64,
                    resource->registration_id);
                return -1;
            }

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
            break;
        }
    }

    for (size_t i = 0, length = conductor->available_counter_handlers.length; i < length; i++)
    {
        aeron_on_available_counter_pair_t *pair = &conductor->available_counter_handlers.array[i];

        pair->handler(pair->clientd, &conductor->counters_reader, response->correlation_id, response->counter_id);
    }

    return 0;
}

int aeron_client_conductor_on_unavailable_counter(aeron_client_conductor_t *conductor, aeron_counter_update_t *response)
{
    for (size_t i = 0, length = conductor->unavailable_counter_handlers.length; i < length; i++)
    {
        aeron_on_unavailable_counter_pair_t *pair = &conductor->unavailable_counter_handlers.array[i];

        pair->handler(pair->clientd, &conductor->counters_reader, response->correlation_id, response->counter_id);
    }

    return 0;
}

int aeron_client_conductor_on_static_counter(aeron_client_conductor_t *conductor, aeron_static_counter_response_t *response)
{
    for (size_t i = 0, size = conductor->registering_resources.length, last_index = size - 1; i < size; i++)
    {
        aeron_client_registering_resource_t *resource = conductor->registering_resources.array[i].resource;

        if (response->correlation_id == resource->registration_id)
        {
            aeron_counter_t *counter;
            int64_t *counter_addr = aeron_counters_reader_addr(&conductor->counters_reader, response->counter_id);

            if (aeron_counter_create(
                &counter,
                conductor,
                resource->counter.registration_id,
                response->counter_id,
                counter_addr) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            resource->resource.counter = counter;

            aeron_array_fast_unordered_remove(
                (uint8_t *)conductor->registering_resources.array,
                sizeof(aeron_client_registering_resource_entry_t),
                i,
                last_index);
            conductor->registering_resources.length--;

            AERON_SET_RELEASE(resource->registration_status, AERON_CLIENT_REGISTERED_MEDIA_DRIVER);
            break;
        }
    }

    return 0;
}

int aeron_client_conductor_on_client_timeout(aeron_client_conductor_t *conductor, aeron_client_timeout_t *response)
{
    if (response->client_id == conductor->client_id)
    {
        char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

        conductor->is_terminating = true;
        aeron_client_conductor_force_close_resources(conductor);
        snprintf(err_buffer, sizeof(err_buffer) - 1, "%s", "client timeout from driver");
        conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_CLIENT_TIMEOUT, err_buffer);
    }

    return 0;
}

int aeron_client_conductor_offer_remove_counter_command(
    aeron_client_conductor_t *conductor, int64_t registration_id)
{
    int rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer, AERON_COMMAND_REMOVE_COUNTER, sizeof(aeron_remove_counter_command_t))) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "remove counter command could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }

        sched_yield();
    }

    aeron_remove_counter_command_t *command = (aeron_remove_counter_command_t *)(conductor->to_driver_buffer.buffer + offset);

    command->correlated.correlation_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    command->correlated.client_id = conductor->client_id;
    command->registration_id = registration_id;

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    return 0;
}

int aeron_client_conductor_offer_remove_publication_command(
    aeron_client_conductor_t *conductor, int64_t registration_id, bool revoke)
{
    int rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer, AERON_COMMAND_REMOVE_PUBLICATION, sizeof(aeron_remove_publication_command_t))) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "remove command could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }

        sched_yield();
    }

    aeron_remove_publication_command_t *command = (aeron_remove_publication_command_t *)(conductor->to_driver_buffer.buffer + offset);

    command->correlated.correlation_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    command->correlated.client_id = conductor->client_id;
    command->registration_id = registration_id;
    command->flags = revoke ? AERON_COMMAND_REMOVE_PUBLICATION_FLAG_REVOKE : 0;

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    return 0;
}

int aeron_client_conductor_offer_remove_subscription_command(
    aeron_client_conductor_t *conductor, int64_t registration_id)
{
    int rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(
        &conductor->to_driver_buffer, AERON_COMMAND_REMOVE_SUBSCRIPTION, sizeof(aeron_remove_subscription_command_t))) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "remove command could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }

        sched_yield();
    }

    aeron_remove_subscription_command_t *command = (aeron_remove_subscription_command_t *)(conductor->to_driver_buffer.buffer + offset);

    command->correlated.correlation_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    command->correlated.client_id = conductor->client_id;
    command->registration_id = registration_id;

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    return 0;
}

int aeron_client_conductor_offer_destination_command(
    aeron_client_conductor_t *conductor,
    int64_t registration_id,
    int32_t command_type,
    const char *uri,
    int64_t *correlation_id)
{
    size_t uri_length = strlen(uri);
    const size_t command_length = sizeof(aeron_destination_command_t) + uri_length;
    int rb_offer_fail_count = 0;

    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(&conductor->to_driver_buffer, command_type, command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            char err_buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            snprintf(err_buffer, sizeof(err_buffer) - 1, "destination command could not be sent (%s:%d)",
                __FILE__, __LINE__);
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_destination_command_t *command = (aeron_destination_command_t *)ptr;
    command->correlated.correlation_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);
    command->correlated.client_id = conductor->client_id;
    command->registration_id = registration_id;
    command->channel_length = (int32_t)uri_length;
    memcpy(ptr + sizeof(aeron_destination_command_t), uri, uri_length);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    if (NULL != correlation_id)
    {
        *correlation_id = command->correlated.correlation_id;
    }

    return 0;
}

int aeron_client_conductor_reject_image(
    aeron_client_conductor_t *conductor,
    int64_t image_correlation_id,
    int64_t position,
    const char *reason,
    int32_t command_type)
{
    size_t reason_length = strlen(reason);
    const size_t command_length = sizeof(aeron_reject_image_command_t) + reason_length;

    int rb_offer_fail_count = 0;
    int32_t offset;
    while ((offset = aeron_mpsc_rb_try_claim(&conductor->to_driver_buffer, command_type, command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            const char *err_buffer = "reject_image command could not be sent";
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }

        sched_yield();
    }

    uint8_t *ptr = (conductor->to_driver_buffer.buffer + offset);
    aeron_reject_image_command_t *command = (aeron_reject_image_command_t *)ptr;
    command->image_correlation_id = image_correlation_id;
    command->position = position;
    command->reason_length = (int32_t)reason_length;
    memcpy(ptr + offsetof(aeron_reject_image_command_t, reason_text), reason, reason_length);
    command->reason_text[reason_length] = '\0';

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);

    return 0;
}

int aeron_client_conductor_on_cmd_client_close(aeron_client_conductor_t *conductor)
{
    int32_t command_length = sizeof(aeron_correlated_command_t);
    int32_t offset;
    int rb_offer_fail_count = 0;

    while ((offset = aeron_mpsc_rb_try_claim(&conductor->to_driver_buffer, AERON_COMMAND_CLIENT_CLOSE, command_length)) < 0)
    {
        if (++rb_offer_fail_count > AERON_CLIENT_COMMAND_RB_FAIL_THRESHOLD)
        {
            const char *err_buffer = "client_close command could not be sent";
            conductor->error_handler(conductor->error_handler_clientd, AERON_CLIENT_ERROR_BUFFER_FULL, err_buffer);
            AERON_SET_ERR(AERON_CLIENT_ERROR_BUFFER_FULL, "%s", err_buffer);
            return -1;
        }
        sched_yield();
    }

    uint8_t *ptr = conductor->to_driver_buffer.buffer + offset;
    aeron_correlated_command_t *command = (aeron_correlated_command_t *)ptr;
    command->client_id = conductor->client_id;
    command->correlation_id = aeron_mpsc_rb_next_correlation_id(&conductor->to_driver_buffer);

    aeron_mpsc_rb_commit(&conductor->to_driver_buffer, offset);
    return 0;
}

extern int aeron_counter_heartbeat_timestamp_find_counter_id_by_registration_id(
    aeron_counters_reader_t *counters_reader, int32_t type_id, int64_t registration_id);
extern bool aeron_counter_heartbeat_timestamp_is_active(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t type_id, int64_t registration_id);
extern void aeron_client_conductor_notify_close_handlers(aeron_client_conductor_t *conductor);
extern bool aeron_client_conductor_is_closed(aeron_client_conductor_t *conductor);
