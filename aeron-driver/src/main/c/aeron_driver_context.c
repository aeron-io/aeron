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

#include <stddef.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <errno.h>

#ifdef HAVE_UUID_H
#include <uuid/uuid.h>
#endif

#include "aeron_windows.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_parse_util.h"
#include "aeron_driver.h"
#include "aeron_alloc.h"
#include "aeron_termination_validator.h"
#include "agent/aeron_driver_agent.h"
#include "util/aeron_dlopen.h"

aeron_threading_mode_t aeron_config_parse_threading_mode(const char *threading_mode, aeron_threading_mode_t def)
{
    aeron_threading_mode_t result = def;

    if (NULL != threading_mode)
    {
        if (strncmp(threading_mode, "SHARED", sizeof("SHARED")) == 0)
        {
            result = AERON_THREADING_MODE_SHARED;
        }
        else if (strncmp(threading_mode, "SHARED_NETWORK", sizeof("SHARED_NETWORK")) == 0)
        {
            result = AERON_THREADING_MODE_SHARED_NETWORK;
        }
        else if (strncmp(threading_mode, "DEDICATED", sizeof("DEDICATED")) == 0)
        {
            result = AERON_THREADING_MODE_DEDICATED;
        }
        else if (strncmp(threading_mode, "INVOKER", sizeof("INVOKER")) == 0)
        {
            result = AERON_THREADING_MODE_INVOKER;
        }
        else
        {
            aeron_config_prop_warning(AERON_THREADING_MODE_ENV_VAR, threading_mode);
        }
    }

    return result;
}

aeron_inferable_boolean_t aeron_config_parse_inferable_boolean(
    const char *inferable_boolean, aeron_inferable_boolean_t def)
{
    aeron_inferable_boolean_t result = def;

    if (NULL != inferable_boolean)
    {
        if (strncmp(inferable_boolean, "true", sizeof("true")) == 0)
        {
            result = AERON_FORCE_TRUE;
        }
        else if (strncmp(inferable_boolean, "infer", sizeof("infer")) == 0)
        {
            result = AERON_INFER;
        }
        else
        {
            result = AERON_FORCE_FALSE;
        }
    }

    return result;
}

#define AERON_CONFIG_GETENV_OR_DEFAULT(e, d) ((NULL == getenv(e)) ? (d) : getenv(e))
#define AERON_CONFIG_STRNDUP_GETENV_OR_NULL(e) ((NULL == getenv(e)) ? (NULL) : aeron_strndup(getenv(e), AERON_MAX_PATH))

static void aeron_driver_conductor_to_driver_interceptor_null(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
}

static void aeron_driver_conductor_to_client_interceptor_null(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length)
{
}

static void aeron_driver_conductor_name_resolver_on_neighbor_change_null(const struct sockaddr_storage *addr)
{
}

static void aeron_driver_conductor_remove_publication_cleanup_null(
    const int32_t session_id, const int32_t stream_id, const size_t channel_length, const char *channel)
{
}

static void aeron_driver_conductor_remove_subscription_cleanup_null(
    const int64_t id, const int32_t stream_id, const size_t channel_length, const char *channel)
{
}

static void aeron_driver_conductor_remove_image_cleanup_null(
    const int64_t id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel)
{
}

static void aeron_driver_conductor_on_endpoint_change_null(const void *channel)
{
}

static void aeron_driver_untethered_subscription_state_change_null(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id)
{
}

#define AERON_DIR_WARN_IF_EXISTS_DEFAULT false
#define AERON_THREADING_MODE_DEFAULT AERON_THREADING_MODE_DEDICATED
#define AERON_DIR_DELETE_ON_START_DEFAULT false
#define AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT false
#define AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * INT64_C(1000))
#define AERON_TERM_BUFFER_LENGTH_DEFAULT (16 * 1024 * 1024)
#define AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT (64 * 1024 * 1024)
#define AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT (true)
#define AERON_PERFORM_STORAGE_CHECKS_DEFAULT (true)
#define AERON_LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT (AERON_TERM_BUFFER_LENGTH_DEFAULT * INT64_C(10))
#define AERON_SPIES_SIMULATE_CONNECTION_DEFAULT (false)
#define AERON_FILE_PAGE_SIZE_DEFAULT (4 * 1024)
#define AERON_MTU_LENGTH_DEFAULT (1408)
#define AERON_IPC_MTU_LENGTH_DEFAULT (1408)
#define AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT (0)
#define AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT (0)
#define AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * INT64_C(1000))
#define AERON_SOCKET_SO_RCVBUF_DEFAULT (128 * 1024)
#define AERON_SOCKET_SO_SNDBUF_DEFAULT (0)
#define AERON_SOCKET_MULTICAST_TTL_DEFAULT (0)
#define AERON_RECEIVER_GROUP_TAG_IS_PRESENT_DEFAULT false
#define AERON_RECEIVER_GROUP_TAG_VALUE_DEFAULT (-1)
#define AERON_FLOW_CONTROL_GROUP_TAG_DEFAULT (-1)
#define AERON_FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT (0)
#define AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * INT64_C(1000))
#define AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT (6)
#define AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT (200 * 1000 * INT64_C(1000))
#define AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT ("aeron_max_multicast_flow_control_strategy_supplier")
#define AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT ("aeron_unicast_flow_control_strategy_supplier")
#define AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT ("aeron_congestion_control_default_strategy_supplier")
#define AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * INT64_C(1000))
#define AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT (128 * 1024)
#define AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT (1024 * 1024)
#define AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT (15 * 1000 * 1000 * INT64_C(1000))
#define AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * INT64_C(1000))
#define AERON_TIMER_INTERVAL_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_IDLE_STRATEGY_BACKOFF_DEFAULT "aeron_idle_strategy_backoff"
#define AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_PRINT_CONFIGURATION_DEFAULT (false)
#define AERON_RELIABLE_STREAM_DEFAULT (true)
#define AERON_TETHER_SUBSCRIPTIONS_DEFAULT (true)
#define AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT (5 * 1000 * 1000 * INT64_C(1000))
#define AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT (10 * 1000 * 1000 * INT64_C(1000))
#define AERON_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000)
#define AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT (0)
#define AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT (10 * 1000 * INT64_C(1000))
#define AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT (10)
#define AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT (10 * 1000 * INT64_C(1000))
#define AERON_NAK_UNICAST_DELAY_NS_DEFAULT (100 * INT64_C(1000))
#define AERON_NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT (100)
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT ("default")
#define AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_INTERCEPTORS_DEFAULT ("")
#define AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT (AERON_INFER)
#define AERON_REJOIN_STREAM_DEFAULT (true)
#define AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT (-1)
#define AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT (1000)
#define AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_DRIVER_SENDER_CYCLE_THRESHOLD_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_NS_DEFAULT (1 * 1000 * 1000 * INT64_C(1000))
#define AERON_DRIVER_NAME_RESOLVER_THRESHOLD_NS_DEFAULT (5 * 1000 * 1000 * INT64_C(1000))
#define AERON_RECEIVER_IO_VECTOR_CAPACITY_DEFAULT UINT32_C(2)
#define AERON_SENDER_IO_VECTOR_CAPACITY_DEFAULT UINT32_C(2)
#define AERON_SENDER_MAX_MESSAGES_PER_SEND_DEFAULT UINT32_C(2)
#define AERON_DRIVER_RESOURCE_FREE_LIMIT_DEFAULT UINT32_C(10)
#define AERON_DRIVER_ASYNC_EXECUTOR_THREADS_DEFAULT UINT32_C(1)
#define AERON_CPU_AFFINITY_DEFAULT (-1)
#define AERON_DRIVER_CONNECT_DEFAULT true
#define AERON_ENABLE_EXPERIMENTAL_FEATURES_DEFAULT false
#define AERON_DRIVER_STREAM_SESSION_LIMIT_DEFAULT (INT32_MAX)


int aeron_driver_context_init(aeron_driver_context_t **context)
{
    aeron_driver_context_t *_context = NULL;

    if (NULL == context)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_driver_context_init(NULL)");
        return -1;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_driver_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_driver_context");
        return -1;
    }

    _context->cnc_map.addr = NULL;
    _context->loss_report.addr = NULL;
    _context->conductor_proxy = NULL;
    _context->sender_proxy = NULL;
    _context->receiver_proxy = NULL;
    _context->counters_manager = NULL;
    _context->error_log = NULL;

    _context->conductor_duty_cycle_tracker = &_context->conductor_duty_cycle_stall_tracker.tracker;
    _context->conductor_duty_cycle_stall_tracker.tracker.update =
        aeron_duty_cycle_stall_tracker_update;
    _context->conductor_duty_cycle_stall_tracker.tracker.measure_and_update =
        aeron_duty_cycle_stall_tracker_measure_and_update;
    _context->conductor_duty_cycle_stall_tracker.tracker.state = &_context->conductor_duty_cycle_stall_tracker;

    _context->sender_duty_cycle_tracker = &_context->sender_duty_cycle_stall_tracker.tracker;
    _context->sender_duty_cycle_stall_tracker.tracker.update =
        aeron_duty_cycle_stall_tracker_update;
    _context->sender_duty_cycle_stall_tracker.tracker.measure_and_update =
        aeron_duty_cycle_stall_tracker_measure_and_update;
    _context->sender_duty_cycle_stall_tracker.tracker.state = &_context->sender_duty_cycle_stall_tracker;

    _context->receiver_duty_cycle_tracker = &_context->receiver_duty_cycle_stall_tracker.tracker;
    _context->receiver_duty_cycle_stall_tracker.tracker.update =
        aeron_duty_cycle_stall_tracker_update;
    _context->receiver_duty_cycle_stall_tracker.tracker.measure_and_update =
        aeron_duty_cycle_stall_tracker_measure_and_update;
    _context->receiver_duty_cycle_stall_tracker.tracker.state = &_context->receiver_duty_cycle_stall_tracker;

    _context->name_resolver_time_tracker = &_context->name_resolver_time_stall_tracker.tracker;
    _context->name_resolver_time_stall_tracker.tracker.update =
        aeron_duty_cycle_stall_tracker_update;
    _context->name_resolver_time_stall_tracker.tracker.measure_and_update =
        aeron_duty_cycle_stall_tracker_measure_and_update;
    _context->name_resolver_time_stall_tracker.tracker.state = &_context->name_resolver_time_stall_tracker;

    _context->sender_port_manager = &_context->sender_wildcard_port_manager.port_manager;
    _context->receiver_port_manager = &_context->receiver_wildcard_port_manager.port_manager;

    _context->udp_channel_outgoing_interceptor_bindings = NULL;
    _context->udp_channel_incoming_interceptor_bindings = NULL;
    _context->dynamic_libs = NULL;
    _context->bindings_clientd_entries = NULL;
    _context->num_bindings_clientd_entries = 0;

    const size_t command_rb_capacity = (AERON_COMMAND_RB_CAPACITY * 1024) + AERON_RB_TRAILER_LENGTH;

    void *sender_buffer;
    if (aeron_alloc(&sender_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_mpsc_rb_init(&_context->sender_command_queue, sender_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(sender_buffer);
        goto error;
    }

    void *receiver_buffer;
    if (aeron_alloc(&receiver_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_mpsc_rb_init(&_context->receiver_command_queue, receiver_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(receiver_buffer);
        goto error;
    }

    void *conductor_buffer;
    if (aeron_alloc(&conductor_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_mpsc_rb_init(&_context->conductor_command_queue, conductor_buffer, command_rb_capacity))
    {
        AERON_APPEND_ERR("%s", "");
        aeron_free(conductor_buffer);
        goto error;
    }

    _context->agent_on_start_func = NULL;
    _context->agent_on_start_state = NULL;

    if ((_context->unicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(
        AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        goto error;
    }

    if ((_context->multicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(
        AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        goto error;
    }

    if ((_context->congestion_control_supplier_func = aeron_congestion_control_strategy_supplier_load(
        AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT)) == NULL)
    {
        goto error;
    }

    if ((_context->name_resolver_supplier_func = aeron_name_resolver_supplier_load(
        AERON_NAME_RESOLVER_SUPPLIER_DEFAULT)) == NULL)
    {
        goto error;
    }

    if (aeron_wildcard_port_manager_init(&_context->sender_wildcard_port_manager, true) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to initialize sender wildcard port manager");
        goto error;
    }

    if (aeron_wildcard_port_manager_init(&_context->receiver_wildcard_port_manager, false) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to initialize receiver wildcard port manager");
        goto error;
    }

    if (aeron_default_path(_context->aeron_dir, sizeof(_context->aeron_dir)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to resolve default aeron directory");
        goto error;
    }

    _context->threading_mode = aeron_config_parse_threading_mode(
        getenv(AERON_THREADING_MODE_ENV_VAR), AERON_THREADING_MODE_DEFAULT);
    _context->receiver_group_consideration = aeron_config_parse_inferable_boolean(
        getenv(AERON_RECEIVER_GROUP_CONSIDERATION_ENV_VAR), AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT);
    _context->dirs_delete_on_start = AERON_DIR_DELETE_ON_START_DEFAULT;
    _context->dirs_delete_on_shutdown = AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT;
    _context->warn_if_dirs_exist = AERON_DIR_WARN_IF_EXISTS_DEFAULT;
    _context->term_buffer_sparse_file = AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT;
    _context->perform_storage_checks = AERON_PERFORM_STORAGE_CHECKS_DEFAULT;
    _context->spies_simulate_connection = AERON_SPIES_SIMULATE_CONNECTION_DEFAULT;
    _context->print_configuration_on_start = AERON_PRINT_CONFIGURATION_DEFAULT;
    _context->reliable_stream = AERON_RELIABLE_STREAM_DEFAULT;
    _context->tether_subscriptions = AERON_TETHER_SUBSCRIPTIONS_DEFAULT;
    _context->rejoin_stream = AERON_REJOIN_STREAM_DEFAULT;
    _context->ats_enabled = false;
    _context->driver_timeout_ms = AERON_DRIVER_TIMEOUT_MS_DEFAULT;
    _context->to_driver_buffer_length = AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT;
    _context->to_clients_buffer_length = AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT;
    _context->counters_values_buffer_length = AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT;
    _context->error_buffer_length = AERON_ERROR_BUFFER_LENGTH_DEFAULT;
    _context->client_liveness_timeout_ns = AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT;
    _context->timer_interval_ns = AERON_TIMER_INTERVAL_NS_DEFAULT;
    _context->term_buffer_length = AERON_TERM_BUFFER_LENGTH_DEFAULT;
    _context->ipc_term_buffer_length = AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT;
    _context->mtu_length = AERON_MTU_LENGTH_DEFAULT;
    _context->ipc_mtu_length = AERON_IPC_MTU_LENGTH_DEFAULT;
    _context->ipc_publication_window_length = AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
    _context->publication_window_length = AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
    _context->publication_linger_timeout_ns = AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT;
    _context->socket_rcvbuf = AERON_SOCKET_SO_RCVBUF_DEFAULT;
    _context->socket_sndbuf = AERON_SOCKET_SO_SNDBUF_DEFAULT;
    _context->multicast_ttl = AERON_SOCKET_MULTICAST_TTL_DEFAULT;
    _context->receiver_group_tag.is_present = AERON_RECEIVER_GROUP_TAG_IS_PRESENT_DEFAULT;
    _context->receiver_group_tag.value = AERON_RECEIVER_GROUP_TAG_VALUE_DEFAULT;
    _context->flow_control.group_tag = AERON_FLOW_CONTROL_GROUP_TAG_DEFAULT;
    _context->flow_control.group_min_size = AERON_FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT;
    _context->flow_control.receiver_timeout_ns = AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT;
    _context->send_to_sm_poll_ratio = AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT;
    _context->status_message_timeout_ns = AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT;
    _context->image_liveness_timeout_ns = AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT;
    _context->initial_window_length = AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT;
    _context->loss_report_length = AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT;
    _context->file_page_size = AERON_FILE_PAGE_SIZE_DEFAULT;
    _context->low_file_store_warning_threshold = AERON_LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT;
    _context->publication_unblock_timeout_ns = AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT;
    _context->publication_connection_timeout_ns = AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT;
    _context->counter_free_to_reuse_ns = AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT;
    _context->untethered_window_limit_timeout_ns = AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT;
    _context->untethered_resting_timeout_ns = AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT;
    _context->max_resend = AERON_RETRANSMIT_HANDLER_MAX_RESEND;
    _context->retransmit_unicast_delay_ns = AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT;
    _context->retransmit_unicast_linger_ns = AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT;
    _context->nak_multicast_group_size = AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT;
    _context->nak_multicast_max_backoff_ns = AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT;
    _context->nak_unicast_delay_ns = AERON_NAK_UNICAST_DELAY_NS_DEFAULT;
    _context->nak_unicast_retry_delay_ratio = AERON_NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT;
    _context->publication_reserved_session_id_low = AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT;
    _context->publication_reserved_session_id_high = AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT;
    _context->resolver_name = NULL;
    _context->resolver_interface = NULL;
    _context->resolver_bootstrap_neighbor = NULL;
    _context->name_resolver_init_args = NULL;
    _context->re_resolution_check_interval_ns = AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT;
    _context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns = AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_NS_DEFAULT;
    _context->sender_duty_cycle_stall_tracker.cycle_threshold_ns = AERON_DRIVER_SENDER_CYCLE_THRESHOLD_NS_DEFAULT;
    _context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns = AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_NS_DEFAULT;
    _context->name_resolver_time_stall_tracker.cycle_threshold_ns = AERON_DRIVER_NAME_RESOLVER_THRESHOLD_NS_DEFAULT;
    _context->receiver_io_vector_capacity = AERON_RECEIVER_IO_VECTOR_CAPACITY_DEFAULT;
    _context->sender_io_vector_capacity = AERON_SENDER_IO_VECTOR_CAPACITY_DEFAULT;
    _context->network_publication_max_messages_per_send = AERON_SENDER_MAX_MESSAGES_PER_SEND_DEFAULT;
    _context->resource_free_limit = AERON_DRIVER_RESOURCE_FREE_LIMIT_DEFAULT;
    _context->async_executor_threads = AERON_DRIVER_ASYNC_EXECUTOR_THREADS_DEFAULT;
    _context->connect_enabled = AERON_DRIVER_CONNECT_DEFAULT;
    _context->conductor_cpu_affinity_no = AERON_CPU_AFFINITY_DEFAULT;
    _context->sender_cpu_affinity_no = AERON_CPU_AFFINITY_DEFAULT;
    _context->receiver_cpu_affinity_no = AERON_CPU_AFFINITY_DEFAULT;
    _context->enable_experimental_features = AERON_ENABLE_EXPERIMENTAL_FEATURES_DEFAULT;
    _context->stream_session_limit = AERON_DRIVER_STREAM_SESSION_LIMIT_DEFAULT;

    char *value = NULL;

    if ((value = getenv(AERON_DRIVER_DYNAMIC_LIBRARIES_ENV_VAR)))
    {
        if (aeron_dl_load_libs(&_context->dynamic_libs, value) < 0)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        if (aeron_driver_context_set_dir(_context, value) < 0)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_AGENT_ON_START_FUNCTION_ENV_VAR)))
    {
        if ((_context->agent_on_start_func = aeron_agent_on_start_load(value)) == NULL)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_UNICAST_FLOWCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->unicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(value)) == NULL)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_MULTICAST_FLOWCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->multicast_flow_control_supplier_func = aeron_flow_control_strategy_supplier_load(value)) == NULL)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_CONGESTIONCONTROL_SUPPLIER_ENV_VAR)))
    {
        if ((_context->congestion_control_supplier_func =
            aeron_congestion_control_strategy_supplier_load(value)) == NULL)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_NAME_RESOLVER_SUPPLIER_ENV_VAR)))
    {
        if ((_context->name_resolver_supplier_func = aeron_name_resolver_supplier_load(value)) == NULL)
        {
            goto error;
        }
    }

    _context->resolver_name = getenv(AERON_DRIVER_RESOLVER_NAME_ENV_VAR);
    _context->resolver_interface = getenv(AERON_DRIVER_RESOLVER_INTERFACE_ENV_VAR);
    _context->resolver_bootstrap_neighbor = getenv(AERON_DRIVER_RESOLVER_BOOTSTRAP_NEIGHBOR_ENV_VAR);
    _context->name_resolver_init_args = getenv(AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);

    if (NULL != _context->resolver_interface)
    {
        if (NULL == _context->resolver_name || 0 == strlen(_context->resolver_name))
        {
            AERON_SET_ERR(EINVAL, "%s", "`resolverName` is required when `resolverInterface` is set");
            goto error;
        }
    }

    _context->dirs_delete_on_start = aeron_parse_bool(
        getenv(AERON_DIR_DELETE_ON_START_ENV_VAR), _context->dirs_delete_on_start);

    _context->dirs_delete_on_shutdown = aeron_parse_bool(
        getenv(AERON_DIR_DELETE_ON_SHUTDOWN_ENV_VAR), _context->dirs_delete_on_shutdown);

    _context->warn_if_dirs_exist = aeron_parse_bool(
        getenv(AERON_DIR_WARN_IF_EXISTS_ENV_VAR), _context->warn_if_dirs_exist);

    _context->term_buffer_sparse_file = aeron_parse_bool(
        getenv(AERON_TERM_BUFFER_SPARSE_FILE_ENV_VAR), _context->term_buffer_sparse_file);

    _context->perform_storage_checks = aeron_parse_bool(
        getenv(AERON_PERFORM_STORAGE_CHECKS_ENV_VAR), _context->perform_storage_checks);

    _context->spies_simulate_connection = aeron_parse_bool(
        getenv(AERON_SPIES_SIMULATE_CONNECTION_ENV_VAR), _context->spies_simulate_connection);

    _context->print_configuration_on_start = aeron_parse_bool(
        getenv(AERON_PRINT_CONFIGURATION_ON_START_ENV_VAR), _context->print_configuration_on_start);

    _context->reliable_stream = aeron_parse_bool(
        getenv(AERON_RELIABLE_STREAM_ENV_VAR), _context->reliable_stream);

    _context->tether_subscriptions = aeron_parse_bool(
        getenv(AERON_TETHER_SUBSCRIPTIONS_ENV_VAR), _context->tether_subscriptions);

    _context->rejoin_stream = aeron_parse_bool(
        getenv(AERON_REJOIN_STREAM_ENV_VAR), _context->rejoin_stream);

    _context->connect_enabled = aeron_parse_bool(getenv(AERON_DRIVER_CONNECT_ENV_VAR), _context->connect_enabled);

    _context->to_driver_buffer_length = aeron_config_parse_size64(
        AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TO_CONDUCTOR_BUFFER_LENGTH_ENV_VAR),
        _context->to_driver_buffer_length,
        AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT,
        INT32_MAX);

    _context->to_clients_buffer_length = aeron_config_parse_size64(
        AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TO_CLIENTS_BUFFER_LENGTH_ENV_VAR),
        _context->to_clients_buffer_length,
        AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT,
        INT32_MAX);

    _context->counters_values_buffer_length = aeron_config_parse_size64(
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_COUNTERS_VALUES_BUFFER_LENGTH_ENV_VAR),
        _context->counters_values_buffer_length,
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT,
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_MAX);

    _context->error_buffer_length = aeron_config_parse_size64(
        AERON_ERROR_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_ERROR_BUFFER_LENGTH_ENV_VAR),
        _context->error_buffer_length,
        AERON_ERROR_BUFFER_LENGTH_DEFAULT,
        INT32_MAX);

    _context->client_liveness_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR,
        getenv(AERON_CLIENT_LIVENESS_TIMEOUT_ENV_VAR),
        _context->client_liveness_timeout_ns,
        1000,
        INT64_MAX);

    _context->publication_linger_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_LINGER_TIMEOUT_ENV_VAR),
        _context->publication_linger_timeout_ns,
        1000,
        INT64_MAX);

    _context->term_buffer_length = aeron_config_parse_size64(
        AERON_TERM_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_TERM_BUFFER_LENGTH_ENV_VAR),
        _context->term_buffer_length,
        1024,
        INT32_MAX);

    _context->ipc_term_buffer_length = aeron_config_parse_size64(
        AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_IPC_TERM_BUFFER_LENGTH_ENV_VAR),
        _context->ipc_term_buffer_length,
        1024,
        INT32_MAX);

    _context->mtu_length = aeron_config_parse_size64(
        AERON_MTU_LENGTH_ENV_VAR,
        getenv(AERON_MTU_LENGTH_ENV_VAR),
        _context->mtu_length,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _context->ipc_mtu_length = aeron_config_parse_size64(
        AERON_IPC_MTU_LENGTH_ENV_VAR,
        getenv(AERON_IPC_MTU_LENGTH_ENV_VAR),
        _context->ipc_mtu_length,
        AERON_DATA_HEADER_LENGTH,
        AERON_MAX_UDP_PAYLOAD_LENGTH);

    _context->ipc_publication_window_length = aeron_config_parse_size64(
        AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR),
        _context->ipc_publication_window_length,
        0,
        AERON_LOGBUFFER_TERM_MAX_LENGTH);

    _context->publication_window_length = aeron_config_parse_size64(
        AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_PUBLICATION_TERM_WINDOW_LENGTH_ENV_VAR),
        _context->publication_window_length,
        0,
        AERON_LOGBUFFER_TERM_MAX_LENGTH);

    _context->socket_rcvbuf = aeron_config_parse_size64(
        AERON_SOCKET_SO_RCVBUF_ENV_VAR,
        getenv(AERON_SOCKET_SO_RCVBUF_ENV_VAR),
        _context->socket_rcvbuf,
        0,
        INT32_MAX);

    _context->socket_sndbuf = aeron_config_parse_size64(
        AERON_SOCKET_SO_SNDBUF_ENV_VAR,
        getenv(AERON_SOCKET_SO_SNDBUF_ENV_VAR),
        _context->socket_sndbuf,
        0,
        INT32_MAX);

    _context->multicast_ttl = (uint8_t)aeron_config_parse_uint64(
        AERON_SOCKET_MULTICAST_TTL_ENV_VAR,
        getenv(AERON_SOCKET_MULTICAST_TTL_ENV_VAR),
        _context->multicast_ttl,
        0,
        255);

    _context->conductor_cpu_affinity_no = aeron_config_parse_int32(
        AERON_CONDUCTOR_CPU_AFFINITY_ENV_VAR,
        getenv(AERON_CONDUCTOR_CPU_AFFINITY_ENV_VAR),
        _context->conductor_cpu_affinity_no,
        -1,
        255);
    _context->receiver_cpu_affinity_no = aeron_config_parse_int32(
        AERON_RECEIVER_CPU_AFFINITY_ENV_VAR,
        getenv(AERON_RECEIVER_CPU_AFFINITY_ENV_VAR),
        _context->receiver_cpu_affinity_no,
        -1,
        255);
    _context->sender_cpu_affinity_no = aeron_config_parse_int32(
        AERON_SENDER_CPU_AFFINITY_ENV_VAR,
        getenv(AERON_SENDER_CPU_AFFINITY_ENV_VAR),
        _context->sender_cpu_affinity_no,
        -1,
        255);

    _context->send_to_sm_poll_ratio = (uint8_t)aeron_config_parse_uint64(
        AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR,
        getenv(AERON_SEND_TO_STATUS_POLL_RATIO_ENV_VAR),
        _context->send_to_sm_poll_ratio,
        1,
        INT32_MAX);

    _context->driver_timeout_ms = aeron_config_parse_uint64(
        AERON_DRIVER_TIMEOUT_ENV_VAR,
        getenv(AERON_DRIVER_TIMEOUT_ENV_VAR),
        _context->driver_timeout_ms,
        0,
        INT64_MAX);

    _context->status_message_timeout_ns = aeron_config_parse_duration_ns(
        AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR,
        getenv(AERON_RCV_STATUS_MESSAGE_TIMEOUT_ENV_VAR),
        _context->status_message_timeout_ns,
        1000,
        INT64_MAX);

    _context->image_liveness_timeout_ns = aeron_config_parse_duration_ns(
        AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR,
        getenv(AERON_IMAGE_LIVENESS_TIMEOUT_ENV_VAR),
        _context->image_liveness_timeout_ns,
        1000,
        INT64_MAX);

    _context->initial_window_length = aeron_config_parse_size64(
        AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR,
        getenv(AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR),
        _context->initial_window_length,
        256,
        INT32_MAX);

    _context->loss_report_length = aeron_config_parse_size64(
        AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR,
        getenv(AERON_LOSS_REPORT_BUFFER_LENGTH_ENV_VAR),
        _context->loss_report_length,
        1024,
        INT32_MAX);

    _context->file_page_size = aeron_config_parse_size64(
        AERON_FILE_PAGE_SIZE_ENV_VAR,
        getenv(AERON_FILE_PAGE_SIZE_ENV_VAR),
        _context->file_page_size,
        4 * 1024,
        INT32_MAX);

    _context->low_file_store_warning_threshold = aeron_config_parse_size64(
        AERON_LOW_FILE_STORE_WARNING_THRESHOLD_ENV_VAR,
        getenv(AERON_LOW_FILE_STORE_WARNING_THRESHOLD_ENV_VAR),
        _context->low_file_store_warning_threshold,
        0,
        INT64_MAX);

    _context->publication_unblock_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_UNBLOCK_TIMEOUT_ENV_VAR),
        _context->publication_unblock_timeout_ns,
        1000,
        INT64_MAX);

    _context->publication_connection_timeout_ns = aeron_config_parse_duration_ns(
        AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR,
        getenv(AERON_PUBLICATION_CONNECTION_TIMEOUT_ENV_VAR),
        _context->publication_connection_timeout_ns,
        1000,
        INT64_MAX);

    _context->timer_interval_ns = aeron_config_parse_duration_ns(
        AERON_TIMER_INTERVAL_ENV_VAR,
        getenv(AERON_TIMER_INTERVAL_ENV_VAR),
        _context->timer_interval_ns,
        1000,
        INT64_MAX);

    _context->counter_free_to_reuse_ns = aeron_config_parse_duration_ns(
        AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR,
        getenv(AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_ENV_VAR),
        _context->counter_free_to_reuse_ns,
        0,
        INT64_MAX);

    _context->untethered_window_limit_timeout_ns = aeron_config_parse_duration_ns(
        AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_ENV_VAR,
        getenv(AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_ENV_VAR),
        _context->untethered_window_limit_timeout_ns,
        1000,
        INT64_MAX);

    _context->untethered_resting_timeout_ns = aeron_config_parse_duration_ns(
        AERON_UNTETHERED_RESTING_TIMEOUT_ENV_VAR,
        getenv(AERON_UNTETHERED_RESTING_TIMEOUT_ENV_VAR),
        _context->untethered_resting_timeout_ns,
        1000,
        INT64_MAX);

    _context->max_resend = aeron_config_parse_uint32(
        AERON_MAX_RESEND_ENV_VAR,
        getenv(AERON_MAX_RESEND_ENV_VAR),
        _context->max_resend,
        1,
        AERON_RETRANSMIT_HANDLER_MAX_RESEND_MAX);

    _context->retransmit_unicast_delay_ns = aeron_config_parse_duration_ns(
        AERON_RETRANSMIT_UNICAST_DELAY_ENV_VAR,
        getenv(AERON_RETRANSMIT_UNICAST_DELAY_ENV_VAR),
        _context->retransmit_unicast_delay_ns,
        0,
        INT64_MAX);

    _context->retransmit_unicast_linger_ns = aeron_config_parse_duration_ns(
        AERON_RETRANSMIT_UNICAST_LINGER_ENV_VAR,
        getenv(AERON_RETRANSMIT_UNICAST_LINGER_ENV_VAR),
        _context->retransmit_unicast_linger_ns,
        1000,
        INT64_MAX);

    _context->nak_multicast_group_size = (size_t)aeron_config_parse_uint64(
        AERON_NAK_MULTICAST_GROUP_SIZE_ENV_VAR,
        getenv(AERON_NAK_MULTICAST_GROUP_SIZE_ENV_VAR),
        _context->nak_multicast_group_size,
        1,
        INT32_MAX);

    _context->nak_multicast_max_backoff_ns = aeron_config_parse_duration_ns(
        AERON_NAK_MULTICAST_MAX_BACKOFF_ENV_VAR,
        getenv(AERON_NAK_MULTICAST_MAX_BACKOFF_ENV_VAR),
        _context->nak_multicast_max_backoff_ns,
        1000,
        INT64_MAX);

    _context->nak_unicast_delay_ns = aeron_config_parse_duration_ns(
        AERON_NAK_UNICAST_DELAY_ENV_VAR,
        getenv(AERON_NAK_UNICAST_DELAY_ENV_VAR),
        _context->nak_unicast_delay_ns,
        1000,
        INT64_MAX);

    _context->receiver_group_tag.value = aeron_config_parse_int64(
        AERON_NAK_UNICAST_RETRY_DELAY_RATIO_ENV_VAR,
        getenv(AERON_NAK_UNICAST_RETRY_DELAY_RATIO_ENV_VAR),
        (int64_t)_context->nak_unicast_retry_delay_ratio,
        1,
        INT64_MAX);

    _context->publication_reserved_session_id_low = aeron_config_parse_int32(
        AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_ENV_VAR,
        getenv(AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_ENV_VAR),
        _context->publication_reserved_session_id_low,
        INT32_MIN,
        INT32_MAX);

    _context->publication_reserved_session_id_high = aeron_config_parse_int32(
        AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_ENV_VAR,
        getenv(AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_ENV_VAR),
        _context->publication_reserved_session_id_high,
        INT32_MIN,
        INT32_MAX);

    const char *group_tag_str = getenv(AERON_RECEIVER_GROUP_TAG_ENV_VAR);
    if (NULL != group_tag_str)
    {
        _context->receiver_group_tag.is_present = true;
        _context->receiver_group_tag.value = aeron_config_parse_int64(
            AERON_RECEIVER_GROUP_TAG_ENV_VAR,
            getenv(AERON_RECEIVER_GROUP_TAG_ENV_VAR),
            _context->receiver_group_tag.value,
            INT64_MIN,
            INT64_MAX);
    }

    _context->flow_control.group_tag = aeron_config_parse_int64(
        AERON_FLOW_CONTROL_GROUP_TAG_ENV_VAR,
        getenv(AERON_FLOW_CONTROL_GROUP_TAG_ENV_VAR),
        _context->flow_control.group_tag,
        INT64_MIN,
        INT64_MAX);

    _context->flow_control.group_min_size = aeron_config_parse_int32(
        AERON_FLOW_CONTROL_GROUP_MIN_SIZE_ENV_VAR,
        getenv(AERON_FLOW_CONTROL_GROUP_MIN_SIZE_ENV_VAR),
        _context->flow_control.group_min_size,
        0,
        INT32_MAX);

    _context->flow_control.receiver_timeout_ns = aeron_config_parse_duration_ns(
        AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_ENV_VAR,
        getenv(AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_ENV_VAR),
        _context->flow_control.receiver_timeout_ns,
        0,
        INT64_MAX);

    _context->re_resolution_check_interval_ns = aeron_config_parse_duration_ns(
        AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_ENV_VAR,
        getenv(AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_ENV_VAR),
        _context->re_resolution_check_interval_ns,
        0,
        INT64_MAX);

    _context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns = aeron_config_parse_duration_ns(
        AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_ENV_VAR,
        getenv(AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_ENV_VAR),
        _context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns,
        0,
        UINT64_C(60) * 60 * 1000 * 1000 * 1000);

    _context->sender_duty_cycle_stall_tracker.cycle_threshold_ns = aeron_config_parse_duration_ns(
        AERON_DRIVER_SENDER_CYCLE_THRESHOLD_ENV_VAR,
        getenv(AERON_DRIVER_SENDER_CYCLE_THRESHOLD_ENV_VAR),
        _context->sender_duty_cycle_stall_tracker.cycle_threshold_ns,
        0,
        UINT64_C(60) * 60 * 1000 * 1000 * 1000);

    _context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns = aeron_config_parse_duration_ns(
        AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_ENV_VAR,
        getenv(AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_ENV_VAR),
        _context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns,
        0,
        UINT64_C(60) * 60 * 1000 * 1000 * 1000);

    _context->name_resolver_time_stall_tracker.cycle_threshold_ns = aeron_config_parse_duration_ns(
        AERON_DRIVER_NAME_RESOLVER_THRESHOLD_ENV_VAR,
        getenv(AERON_DRIVER_NAME_RESOLVER_THRESHOLD_ENV_VAR),
        _context->name_resolver_time_stall_tracker.cycle_threshold_ns,
        0,
        UINT64_C(60) * 60 * 1000 * 1000 * 1000);

    if ((value = getenv(AERON_DRIVER_SENDER_WILDCARD_PORT_RANGE_ENV_VAR)))
    {
        uint16_t low_port = 0, high_port = 0;

        if (aeron_parse_port_range(value, &low_port, &high_port) < 0)
        {
            AERON_APPEND_ERR("sender wildcard port range \"%s\" is invalid", value);
            goto error;
        }

        aeron_wildcard_port_manager_set_range(&_context->sender_wildcard_port_manager, low_port, high_port);
    }

    if ((value = getenv(AERON_DRIVER_RECEIVER_WILDCARD_PORT_RANGE_ENV_VAR)))
    {
        uint16_t low_port = 0, high_port = 0;

        if (aeron_parse_port_range(value, &low_port, &high_port) < 0)
        {
            AERON_APPEND_ERR("receiver wildcard port range \"%s\" is invalid", value);
            goto error;
        }

        aeron_wildcard_port_manager_set_range(&_context->receiver_wildcard_port_manager, low_port, high_port);
    }

    _context->receiver_io_vector_capacity = aeron_config_parse_uint32(
        AERON_RECEIVER_IO_VECTOR_CAPACITY_ENV_VAR,
        getenv(AERON_RECEIVER_IO_VECTOR_CAPACITY_ENV_VAR),
        (int32_t)_context->receiver_io_vector_capacity,
        1,
        AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX);

    _context->sender_io_vector_capacity = aeron_config_parse_uint32(
        AERON_SENDER_IO_VECTOR_CAPACITY_ENV_VAR,
        getenv(AERON_SENDER_IO_VECTOR_CAPACITY_ENV_VAR),
        _context->sender_io_vector_capacity,
        1,
        AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX);

    _context->network_publication_max_messages_per_send = aeron_config_parse_uint32(
        AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR,
        getenv(AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND_ENV_VAR),
        _context->network_publication_max_messages_per_send,
        1,
        AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND);

    _context->resource_free_limit = aeron_config_parse_uint32(
        AERON_DRIVER_RESOURCE_FREE_LIMIT_ENV_VAR,
        getenv(AERON_DRIVER_RESOURCE_FREE_LIMIT_ENV_VAR),
        _context->resource_free_limit,
        1,
        INT32_MAX);

    _context->async_executor_threads = aeron_config_parse_uint32(
        AERON_DRIVER_ASYNC_EXECUTOR_THREADS_ENV_VAR,
        getenv(AERON_DRIVER_ASYNC_EXECUTOR_THREADS_ENV_VAR),
        _context->async_executor_threads,
        0,
        1);

    _context->enable_experimental_features = aeron_parse_bool(
        getenv(AERON_ENABLE_EXPERIMENTAL_FEATURES_ENV_VAR), _context->enable_experimental_features);

    _context->stream_session_limit = aeron_config_parse_int32(
        AERON_DRIVER_STREAM_SESSION_LIMIT_ENV_VAR,
        getenv(AERON_DRIVER_STREAM_SESSION_LIMIT_ENV_VAR),
        _context->stream_session_limit,
        1,
        INT32_MAX);

    _context->to_driver_buffer = NULL;
    _context->to_clients_buffer = NULL;
    _context->counters_values_buffer = NULL;
    _context->counters_metadata_buffer = NULL;
    _context->error_buffer = NULL;

    _context->nano_clock = aeron_nano_clock;
    _context->epoch_clock = aeron_epoch_clock;
    if (aeron_clock_cache_alloc(&_context->cached_clock) < 0)
    {
        goto error;
    }
    if (aeron_clock_cache_alloc(&_context->sender_cached_clock) < 0)
    {
        goto error;
    }
    if (aeron_clock_cache_alloc(&_context->receiver_cached_clock) < 0)
    {
        goto error;
    }

    _context->conductor_idle_strategy_name = aeron_strndup("backoff", 10);
    _context->shared_idle_strategy_name = aeron_strndup("backoff", 10);
    _context->shared_network_idle_strategy_name = aeron_strndup("backoff", 10);
    _context->sender_idle_strategy_name = aeron_strndup("backoff", 10);
    _context->receiver_idle_strategy_name = aeron_strndup("backoff", 10);

    _context->conductor_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_CONDUCTOR_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->conductor_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->conductor_idle_strategy_state,
        AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR,
        _context->conductor_idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    _context->shared_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SHARED_IDLE_STRATEGY_ENV_INIT_ARGS_VAR);
    if ((_context->shared_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHARED_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        _context->shared_idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    _context->shared_network_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SHAREDNETWORK_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->shared_network_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->shared_network_idle_strategy_state,
        AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR,
        _context->shared_network_idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    _context->sender_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_SENDER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->sender_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_SENDER_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->sender_idle_strategy_state,
        AERON_SENDER_IDLE_STRATEGY_ENV_VAR,
        _context->sender_idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    _context->receiver_idle_strategy_init_args =
        AERON_CONFIG_STRNDUP_GETENV_OR_NULL(AERON_RECEIVER_IDLE_STRATEGY_INIT_ARGS_ENV_VAR);
    if ((_context->receiver_idle_strategy_func = aeron_idle_strategy_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR, "backoff"),
        &_context->receiver_idle_strategy_state,
        AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR,
        _context->receiver_idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    _context->usable_fs_space_func = _context->perform_storage_checks ?
        aeron_usable_fs_space : aeron_usable_fs_space_disabled;
    _context->raw_log_map_func = aeron_raw_log_map;
    _context->raw_log_close_func = aeron_raw_log_close;
    _context->raw_log_free_func = aeron_raw_log_free;

    _context->log.to_driver_interceptor = aeron_driver_conductor_to_driver_interceptor_null;
    _context->log.to_client_interceptor = aeron_driver_conductor_to_client_interceptor_null;

    _context->log.remove_publication_cleanup = aeron_driver_conductor_remove_publication_cleanup_null;
    _context->log.remove_subscription_cleanup = aeron_driver_conductor_remove_subscription_cleanup_null;
    _context->log.remove_image_cleanup = aeron_driver_conductor_remove_image_cleanup_null;

    _context->log.sender_proxy_on_add_endpoint = aeron_driver_conductor_on_endpoint_change_null;
    _context->log.sender_proxy_on_remove_endpoint = aeron_driver_conductor_on_endpoint_change_null;
    _context->log.receiver_proxy_on_add_endpoint = aeron_driver_conductor_on_endpoint_change_null;
    _context->log.receiver_proxy_on_remove_endpoint = aeron_driver_conductor_on_endpoint_change_null;

    _context->log.untethered_subscription_on_state_change = aeron_driver_untethered_subscription_state_change_null;

    _context->log.name_resolution_on_neighbor_added = aeron_driver_conductor_name_resolver_on_neighbor_change_null;
    _context->log.name_resolution_on_neighbor_removed = aeron_driver_conductor_name_resolver_on_neighbor_change_null;

    _context->log.flow_control_on_receiver_added = NULL;
    _context->log.flow_control_on_receiver_removed = NULL;
    _context->log.on_name_resolve = NULL;

    _context->log.send_nak_message = NULL;
    _context->log.on_nak_message = NULL;
    _context->log.resend = NULL;

    if ((_context->termination_validator_func = aeron_driver_termination_validator_load(
        AERON_CONFIG_GETENV_OR_DEFAULT(AERON_DRIVER_TERMINATION_VALIDATOR_ENV_VAR, "deny"))) == NULL)
    {
        goto error;
    }

    _context->termination_validator_state = NULL;

    _context->termination_hook_func = NULL;
    _context->termination_hook_state = NULL;

    if ((_context->udp_channel_transport_bindings = aeron_udp_channel_transport_bindings_load_media(
        AERON_CONFIG_GETENV_OR_DEFAULT(
            AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_ENV_VAR,
            AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT))) == NULL)
    {
        goto error;
    }

    if ((_context->conductor_udp_channel_transport_bindings = aeron_udp_channel_transport_bindings_load_media(
        AERON_CONFIG_GETENV_OR_DEFAULT(
            AERON_CONDUCTOR_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_ENV_VAR,
            AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT))) == NULL)
    {
        goto error;
    }

    if ((value = getenv(AERON_UDP_CHANNEL_OUTGOING_INTERCEPTORS_ENV_VAR)))
    {
        if ((_context->udp_channel_outgoing_interceptor_bindings = aeron_udp_channel_interceptor_bindings_load(
            NULL, value)) == NULL)
        {
            goto error;
        }
    }

    if ((value = getenv(AERON_UDP_CHANNEL_INCOMING_INTERCEPTORS_ENV_VAR)))
    {
        if ((_context->udp_channel_incoming_interceptor_bindings = aeron_udp_channel_interceptor_bindings_load(
            NULL, value)) == NULL)
        {
            goto error;
        }
    }

#ifdef HAVE_UUID_GENERATE
    uuid_t id;
    uuid_generate(id);

    struct uuid_as_uint64
    {
        uint64_t high;
        uint64_t low;
    }

    *id_as_uint64 = (struct uuid_as_uint64 *)&id;
    _context->next_receiver_id = id_as_uint64->high ^ id_as_uint64->low;
#else
    /* pure random id */
    _context->next_receiver_id = aeron_randomised_int32();
#endif

    if (aeron_netutil_get_so_buf_lengths(
        &_context->os_buffer_lengths.default_so_rcvbuf, &_context->os_buffer_lengths.default_so_sndbuf) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to initial context with buffer lengths");
        return -1;
    }

    if (aeron_driver_context_bindings_clientd_create_entries(_context) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate bindings_clientd entries");
        goto error;
    }

    // Should be last in the initialization chain
    if (getenv(AERON_EVENT_LOG_ENV_VAR))
    {
        if (aeron_driver_agent_context_init(_context) < 0)
        {
            goto error;
        }
    }

    *context = _context;
    return 0;

error:
    aeron_driver_context_close(_context);
    return -1;
}

static void aeron_driver_context_free_bindings(const aeron_udp_channel_interceptor_bindings_t *bindings)
{
    if (NULL != bindings)
    {
        aeron_driver_context_free_bindings(bindings->meta_info.next_interceptor_bindings);
        aeron_free((void *)bindings);
    }
}

int aeron_driver_context_run_storage_checks(aeron_driver_context_t *context, uint64_t log_length)
{
    if (context->perform_storage_checks)
    {
        const uint64_t usable_space = context->usable_fs_space_func(context->aeron_dir);
        if (usable_space < log_length)
        {
            AERON_SET_ERR(
                -AERON_ERROR_CODE_STORAGE_SPACE,
                "insufficient usable storage for new log of length=%" PRId64 " usable=%" PRId64 " in %s",
            log_length, usable_space, context->aeron_dir);
            return -1;
        }

        if (usable_space <= context->low_file_store_warning_threshold)
        {
            AERON_SET_ERR(
                -AERON_ERROR_CODE_STORAGE_SPACE,
                "WARNING: space is running low: threshold=%" PRId64 " usable=%" PRId64 " in %s",
            context->low_file_store_warning_threshold, usable_space, context->aeron_dir);
            aeron_distinct_error_log_record(context->error_log, aeron_errcode(), aeron_errmsg());
            aeron_err_clear();
        }
    }
    return 0;
}

int aeron_driver_context_bindings_clientd_create_entries(aeron_driver_context_t *context)
{
    const aeron_udp_channel_interceptor_bindings_t *interceptor_bindings;
    aeron_driver_context_bindings_clientd_entry_t *_entries;
    size_t num_entries = 1;

    interceptor_bindings = context->udp_channel_outgoing_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        num_entries++;
        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

    interceptor_bindings = context->udp_channel_incoming_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        num_entries++;
        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

    if (0 == context->num_bindings_clientd_entries)
    {
        if (aeron_alloc((void **)&_entries, sizeof(aeron_driver_context_bindings_clientd_entry_t) * num_entries) < 0)
        {
            AERON_APPEND_ERR("%s", "could not allocate context_bindings_clientd_entries");
            return -1;
        }

        for (size_t i = 0; i < num_entries; i++)
        {
            _entries[i].name = NULL;
            _entries[i].clientd = NULL;
        }

        context->bindings_clientd_entries = _entries;
    }
    else if (num_entries > context->num_bindings_clientd_entries)
    {
        if (aeron_reallocf(
            (void **)&context->bindings_clientd_entries,
            sizeof(aeron_driver_context_bindings_clientd_entry_t) * num_entries) < 0)
        {
            AERON_APPEND_ERR("%s", "could not reallocate context_bindings_clientd_entries");
            return -1;
        }

        for (size_t i = context->num_bindings_clientd_entries; i < num_entries; i++)
        {
            context->bindings_clientd_entries[i].name = NULL;
            context->bindings_clientd_entries[i].clientd = NULL;
        }
    }

    context->num_bindings_clientd_entries = num_entries;

    return 0;
}

void aeron_driver_context_drain_all_free(void *cliend, void *item)
{
    aeron_free(item);
}

int aeron_driver_context_close(aeron_driver_context_t *context)
{
    if (NULL == context)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_driver_context_close(NULL)");
        return -1;
    }

    aeron_wildcard_port_manager_delete(&context->sender_wildcard_port_manager);
    aeron_wildcard_port_manager_delete(&context->receiver_wildcard_port_manager);

    aeron_free(context->conductor_command_queue.buffer);
    aeron_free(context->sender_command_queue.buffer);
    aeron_free(context->receiver_command_queue.buffer);

    aeron_driver_context_free_bindings(context->udp_channel_outgoing_interceptor_bindings);
    aeron_driver_context_free_bindings(context->udp_channel_incoming_interceptor_bindings);

    aeron_unmap(&context->loss_report);
    aeron_unmap(&context->cnc_map);

    int result = 0;
    if (context->dirs_delete_on_shutdown)
    {
        int delete_result = aeron_delete_directory(context->aeron_dir);
        if (0 != delete_result)
        {
            if (-1 == delete_result)
            {
                delete_result = EINVAL;
            }

            AERON_SET_ERR(delete_result, "aeron_driver_context_close failed to delete dir: %s", context->aeron_dir);

            result = -1;
        }
    }

    aeron_free(context->conductor_idle_strategy_state);
    aeron_free(context->receiver_idle_strategy_state);
    aeron_free(context->sender_idle_strategy_state);
    aeron_free(context->shared_idle_strategy_state);
    aeron_free(context->shared_network_idle_strategy_state);
    aeron_free((void *)context->conductor_idle_strategy_name);
    aeron_free((void *)context->shared_network_idle_strategy_name);
    aeron_free((void *)context->shared_idle_strategy_name);
    aeron_free((void *)context->sender_idle_strategy_name);
    aeron_free((void *)context->receiver_idle_strategy_name);
    aeron_free(context->conductor_idle_strategy_init_args);
    aeron_free(context->sender_idle_strategy_init_args);
    aeron_free(context->receiver_idle_strategy_init_args);
    aeron_free(context->shared_idle_strategy_init_args);
    aeron_free(context->shared_network_idle_strategy_init_args);
    aeron_free(context->bindings_clientd_entries);
    aeron_free(context->cached_clock);
    aeron_free(context->sender_cached_clock);
    aeron_free(context->receiver_cached_clock);
    aeron_dl_load_libs_delete(context->dynamic_libs);

    aeron_free(context);

    return result;
}

int aeron_driver_validate_unblock_timeout(aeron_driver_context_t *context)
{
    if (context->publication_unblock_timeout_ns <= context->client_liveness_timeout_ns)
    {
        errno = EINVAL;
        AERON_SET_ERR(
            EINVAL,
            "publication_unblock_timeout_ns=%" PRIu64 " <= client_liveness_timeout_ns=%" PRIu64,
            context->publication_unblock_timeout_ns, context->client_liveness_timeout_ns);
        return -1;
    }

    if (context->client_liveness_timeout_ns <= context->timer_interval_ns)
    {
        errno = EINVAL;
        AERON_SET_ERR(
            EINVAL,
            "client_liveness_timeout_ns=%" PRIu64 " <= timer_interval_ns=%" PRIu64,
            context->client_liveness_timeout_ns, context->timer_interval_ns);
        return -1;
    }

    return 0;
}

int aeron_driver_validate_untethered_timeouts(aeron_driver_context_t *context)
{
    if (context->untethered_window_limit_timeout_ns <= context->timer_interval_ns)
    {
        errno = EINVAL;
        AERON_SET_ERR(
            EINVAL,
            "untethered_window_limit_timeout_ns=%" PRIu64 " <= timer_interval_ns=%" PRIu64,
            context->untethered_window_limit_timeout_ns, context->timer_interval_ns);
        return -1;
    }

    if (context->untethered_resting_timeout_ns <= context->timer_interval_ns)
    {
        errno = EINVAL;
        AERON_SET_ERR(
            EINVAL,
            "untethered_resting_timeout_ns=%" PRIu64 " <= timer_interval_ns=%" PRIu64,
            context->untethered_resting_timeout_ns, context->timer_interval_ns);
        return -1;
    }

    return 0;
}

int aeron_driver_context_validate_mtu_length(uint64_t mtu_length)
{
    if (mtu_length <= AERON_DATA_HEADER_LENGTH || mtu_length > AERON_MAX_UDP_PAYLOAD_LENGTH)
    {
        AERON_SET_ERR(
            EINVAL,
            "mtuLength must be a > HEADER_LENGTH and <= MAX_UDP_PAYLOAD_LENGTH: mtuLength=%" PRIu64,
            mtu_length);
        return -1;
    }

    if ((mtu_length & (AERON_LOGBUFFER_FRAME_ALIGNMENT - 1)) != 0)
    {
        AERON_SET_ERR(EINVAL, "mtuLength must be a multiple of FRAME_ALIGNMENT: mtuLength=%" PRIu64, mtu_length);
        return -1;
    }

    return 0;
}

bool aeron_is_driver_active_with_cnc(
    aeron_mapped_file_t *cnc_mmap, int64_t timeout_ms, int64_t now_ms, aeron_log_func_t log_func)
{
    char buffer[AERON_MAX_PATH];
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap->addr;
    int32_t cnc_version;

    while (0 == (cnc_version = aeron_cnc_version_volatile(metadata)))
    {
        if (aeron_epoch_clock() > (now_ms + timeout_ms))
        {
            snprintf(buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file version was 0 for timeout");
            return false;
        }

        aeron_micro_sleep(1000);
    }

    if (aeron_semantic_version_major(AERON_CNC_VERSION) != aeron_semantic_version_major(cnc_version))
    {
        snprintf(
            buffer, sizeof(buffer) - 1,
            "ERROR: aeron cnc version not compatible: app version=%d.%d.%d file=%d.%d.%d",
            (int)aeron_semantic_version_major(AERON_CNC_VERSION),
            (int)aeron_semantic_version_minor(AERON_CNC_VERSION),
            (int)aeron_semantic_version_patch(AERON_CNC_VERSION),
            (int)aeron_semantic_version_major(cnc_version),
            (int)aeron_semantic_version_minor(cnc_version),
            (int)aeron_semantic_version_patch(cnc_version));

        log_func(buffer);
    }
    else
    {
        aeron_mpsc_rb_t rb;

        if (aeron_mpsc_rb_init(
            &rb, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) != 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "ERROR: aeron cnc file could not init to-driver buffer");
            log_func(buffer);
        }
        else
        {
            int64_t timestamp_ms = aeron_mpsc_rb_consumer_heartbeat_time_value(&rb);
            int64_t age = now_ms - timestamp_ms;

            snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron driver heartbeat is %" PRId64 " ms old", age);
            log_func(buffer);

            if (age <= timeout_ms)
            {
                return true;
            }
        }
    }

    return false;
}

bool aeron_is_driver_active(const char *dirname, int64_t timeout_ms, aeron_log_func_t log_func)
{
    char filename[AERON_MAX_PATH];
    char buffer[2 * AERON_MAX_PATH];
    bool result = false;

    if (aeron_is_directory(dirname))
    {
        aeron_mapped_file_t cnc_map = { .addr = NULL, .length = 0 };

        snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron directory %s exists", dirname);
        log_func(buffer);

        if (aeron_cnc_resolve_filename(dirname, filename, sizeof(filename)) < 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "INFO: Unable to resolve cnc filename: %s", aeron_errmsg());
            log_func(buffer);
            return false;
        }

        if (aeron_map_existing_file(&cnc_map, filename) < 0)
        {
            snprintf(buffer, sizeof(buffer) - 1, "INFO: failed to mmap CnC file");
            log_func(buffer);
            return false;
        }

        snprintf(buffer, sizeof(buffer) - 1, "INFO: Aeron CnC file %s exists", filename);
        log_func(buffer);

        result = aeron_is_driver_active_with_cnc(&cnc_map, timeout_ms, aeron_epoch_clock(), log_func);

        aeron_unmap(&cnc_map);
    }

    return result;
}

size_t aeron_cnc_length(aeron_driver_context_t *context)
{
    return aeron_cnc_computed_length(
        context->to_driver_buffer_length +
        context->to_clients_buffer_length +
        AERON_COUNTERS_METADATA_BUFFER_LENGTH(context->counters_values_buffer_length) +
        context->counters_values_buffer_length +
        context->error_buffer_length,
        context->file_page_size);
}

extern void aeron_cnc_version_signal_cnc_ready(aeron_cnc_metadata_t *metadata, int32_t cnc_version);

extern size_t aeron_producer_window_length(size_t producer_window_length, size_t term_length);

extern size_t aeron_receiver_window_length(size_t initial_receiver_window_length, size_t term_length);

#define AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(r, a) \
do \
{ \
    if (NULL == (a)) \
    { \
        AERON_SET_ERR(EINVAL, "%s is null", #a); \
        return (r); \
    } \
} \
while (false) \

int aeron_driver_context_set_dir(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    snprintf(context->aeron_dir, sizeof(context->aeron_dir), "%s", value);
    return 0;
}

const char *aeron_driver_context_get_dir(aeron_driver_context_t *context)
{
    return NULL != context ? context->aeron_dir : NULL;
}

int aeron_driver_context_set_dir_warn_if_exists(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->warn_if_dirs_exist = value;
    return 0;
}

bool aeron_driver_context_get_dir_warn_if_exists(aeron_driver_context_t *context)
{
    return NULL != context ? context->warn_if_dirs_exist : AERON_DIR_WARN_IF_EXISTS_DEFAULT;
}

int aeron_driver_context_set_threading_mode(aeron_driver_context_t *context, aeron_threading_mode_t mode)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->threading_mode = mode;
    return 0;
}

aeron_threading_mode_t aeron_driver_context_get_threading_mode(aeron_driver_context_t *context)
{
    return NULL != context ? context->threading_mode : AERON_THREADING_MODE_DEFAULT;
}

int aeron_driver_context_set_dir_delete_on_start(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->dirs_delete_on_start = value;
    return 0;
}

bool aeron_driver_context_get_dir_delete_on_start(aeron_driver_context_t *context)
{
    return NULL != context ? context->dirs_delete_on_start : AERON_DIR_DELETE_ON_START_DEFAULT;
}

int aeron_driver_context_set_dir_delete_on_shutdown(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->dirs_delete_on_shutdown = value;
    return 0;
}

bool aeron_driver_context_get_dir_delete_on_shutdown(aeron_driver_context_t *context)
{
    return NULL != context ? context->dirs_delete_on_shutdown : AERON_DIR_DELETE_ON_SHUTDOWN_DEFAULT;
}

int aeron_driver_context_set_to_conductor_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->to_driver_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_to_conductor_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->to_driver_buffer_length : AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_to_clients_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->to_clients_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_to_clients_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->to_clients_buffer_length : AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_counters_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->counters_values_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_counters_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->counters_values_buffer_length : AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_error_buffer_length(aeron_driver_context_t *context, size_t length)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_buffer_length = length;
    return 0;
}

size_t aeron_driver_context_get_error_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->error_buffer_length : AERON_ERROR_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_client_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->client_liveness_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_client_liveness_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->client_liveness_timeout_ns : AERON_CLIENT_LIVENESS_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_term_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->term_buffer_length = value;
    return 0;
}

size_t aeron_driver_context_get_term_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->term_buffer_length : AERON_TERM_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_term_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_term_buffer_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_term_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_term_buffer_length : AERON_IPC_TERM_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_term_buffer_sparse_file(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->term_buffer_sparse_file = value;
    return 0;
}

bool aeron_driver_context_get_term_buffer_sparse_file(aeron_driver_context_t *context)
{
    return NULL != context ? context->term_buffer_sparse_file : AERON_TERM_BUFFER_SPARSE_FILE_DEFAULT;
}

int aeron_driver_context_set_perform_storage_checks(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->perform_storage_checks = value;
    return 0;
}

bool aeron_driver_context_get_perform_storage_checks(aeron_driver_context_t *context)
{
    return NULL != context ? context->perform_storage_checks : AERON_PERFORM_STORAGE_CHECKS_DEFAULT;
}

int aeron_driver_context_set_low_file_store_warning_threshold(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->low_file_store_warning_threshold = value;
    return 0;
}

uint64_t aeron_driver_context_get_low_file_store_warning_threshold(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->low_file_store_warning_threshold : AERON_LOW_FILE_STORE_WARNING_THRESHOLD_DEFAULT;
}

int aeron_driver_context_set_spies_simulate_connection(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->spies_simulate_connection = value;
    return 0;
}

bool aeron_driver_context_get_spies_simulate_connection(aeron_driver_context_t *context)
{
    return NULL != context ? context->spies_simulate_connection : AERON_SPIES_SIMULATE_CONNECTION_DEFAULT;
}

int aeron_driver_context_set_file_page_size(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->file_page_size = value;
    return 0;
}

size_t aeron_driver_context_get_file_page_size(aeron_driver_context_t *context)
{
    return NULL != context ? context->file_page_size : AERON_FILE_PAGE_SIZE_DEFAULT;
}

int aeron_driver_context_set_mtu_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->mtu_length = value;
    return 0;
}

size_t aeron_driver_context_get_mtu_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->mtu_length : AERON_MTU_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_mtu_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_mtu_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_mtu_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_mtu_length : AERON_IPC_MTU_LENGTH_DEFAULT;
}

int aeron_driver_context_set_ipc_publication_term_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->ipc_publication_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_ipc_publication_term_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->ipc_publication_window_length : AERON_IPC_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_term_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_publication_term_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_window_length : AERON_PUBLICATION_TERM_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_linger_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_linger_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_linger_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_linger_timeout_ns : AERON_PUBLICATION_LINGER_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_socket_so_rcvbuf(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->socket_rcvbuf = value;
    return 0;
}

size_t aeron_driver_context_get_socket_so_rcvbuf(aeron_driver_context_t *context)
{
    return NULL != context ? context->socket_rcvbuf : AERON_SOCKET_SO_RCVBUF_DEFAULT;
}

int aeron_driver_context_set_socket_so_sndbuf(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->socket_sndbuf = value;
    return 0;
}

size_t aeron_driver_context_get_socket_so_sndbuf(aeron_driver_context_t *context)
{
    return NULL != context ? context->socket_sndbuf : AERON_SOCKET_SO_SNDBUF_DEFAULT;
}

int aeron_driver_context_set_socket_multicast_ttl(aeron_driver_context_t *context, uint8_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->multicast_ttl = value;
    return 0;
}

uint8_t aeron_driver_context_get_socket_multicast_ttl(aeron_driver_context_t *context)
{
    return NULL != context ? context->multicast_ttl : AERON_SOCKET_MULTICAST_TTL_DEFAULT;
}

int aeron_driver_context_set_send_to_status_poll_ratio(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->send_to_sm_poll_ratio = value;
    return 0;
}

size_t aeron_driver_context_get_send_to_status_poll_ratio(aeron_driver_context_t *context)
{
    return NULL != context ? context->send_to_sm_poll_ratio : AERON_SEND_TO_STATUS_POLL_RATIO_DEFAULT;
}

int aeron_driver_context_set_rcv_status_message_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->status_message_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_rcv_status_message_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->status_message_timeout_ns : AERON_RCV_STATUS_MESSAGE_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->multicast_flow_control_supplier_func = value;
    return 0;
}

aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_multicast_flowcontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->multicast_flow_control_supplier_func :
        aeron_flow_control_strategy_supplier_load(AERON_MULTICAST_FLOWCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context, aeron_flow_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->unicast_flow_control_supplier_func = value;
    return 0;
}

aeron_flow_control_strategy_supplier_func_t aeron_driver_context_get_unicast_flowcontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->unicast_flow_control_supplier_func :
        aeron_flow_control_strategy_supplier_load(AERON_UNICAST_FLOWCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_image_liveness_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->image_liveness_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_image_liveness_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->image_liveness_timeout_ns : AERON_IMAGE_LIVENESS_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_rcv_initial_window_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->initial_window_length = value;
    return 0;
}

size_t aeron_driver_context_get_rcv_initial_window_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->initial_window_length : AERON_RCV_INITIAL_WINDOW_LENGTH_DEFAULT;
}

int aeron_driver_context_set_congestioncontrol_supplier(
    aeron_driver_context_t *context, aeron_congestion_control_strategy_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    context->congestion_control_supplier_func = value;
    return 0;
}

aeron_congestion_control_strategy_supplier_func_t aeron_driver_context_get_congestioncontrol_supplier(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->congestion_control_supplier_func :
        aeron_congestion_control_strategy_supplier_load(AERON_CONGESTIONCONTROL_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_loss_report_buffer_length(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->loss_report_length = value;
    return 0;
}

size_t aeron_driver_context_get_loss_report_buffer_length(aeron_driver_context_t *context)
{
    return NULL != context ? context->loss_report_length : AERON_LOSS_REPORT_BUFFER_LENGTH_DEFAULT;
}

int aeron_driver_context_set_publication_unblock_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_unblock_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_unblock_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->publication_unblock_timeout_ns : AERON_PUBLICATION_UNBLOCK_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_publication_connection_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_connection_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_publication_connection_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_connection_timeout_ns : AERON_PUBLICATION_CONNECTION_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_timer_interval_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->timer_interval_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_timer_interval_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->timer_interval_ns : AERON_TIMER_INTERVAL_NS_DEFAULT;
}

int aeron_driver_context_set_sender_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->sender_idle_strategy_state);
    aeron_free((void *)context->sender_idle_strategy_name);

    if ((context->sender_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->sender_idle_strategy_state,
        AERON_SENDER_IDLE_STRATEGY_ENV_VAR,
        context->sender_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->sender_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sender_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_conductor_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->conductor_idle_strategy_state);
    aeron_free((void *)context->conductor_idle_strategy_name);

    if ((context->conductor_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->conductor_idle_strategy_state,
        AERON_CONDUCTOR_IDLE_STRATEGY_ENV_VAR,
        context->conductor_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->conductor_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_conductor_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_receiver_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->receiver_idle_strategy_state);
    aeron_free((void *)context->receiver_idle_strategy_name);

    if ((context->receiver_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->receiver_idle_strategy_state,
        AERON_RECEIVER_IDLE_STRATEGY_ENV_VAR,
        context->receiver_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->receiver_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_receiver_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_sharednetwork_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->shared_network_idle_strategy_state);
    aeron_free((void *)context->shared_network_idle_strategy_name);

    if ((context->shared_network_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->shared_network_idle_strategy_state,
        AERON_SHAREDNETWORK_IDLE_STRATEGY_ENV_VAR,
        context->shared_network_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->shared_network_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sharednetwork_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_network_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_shared_idle_strategy(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->shared_idle_strategy_state);
    aeron_free((void *)context->shared_idle_strategy_name);

    if ((context->shared_idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->shared_idle_strategy_state,
        AERON_SHARED_IDLE_STRATEGY_ENV_VAR,
        context->shared_idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->shared_idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_shared_idle_strategy(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_idle_strategy_name : AERON_IDLE_STRATEGY_BACKOFF_DEFAULT;
}

int aeron_driver_context_set_sender_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->sender_idle_strategy_init_args);
    context->sender_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sender_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_conductor_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->conductor_idle_strategy_init_args);
    context->conductor_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_conductor_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_receiver_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->receiver_idle_strategy_init_args);
    context->receiver_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_receiver_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->shared_network_idle_strategy_init_args);
    context->shared_network_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_sharednetwork_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_network_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_shared_idle_strategy_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->shared_idle_strategy_init_args);
    context->shared_idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_driver_context_get_shared_idle_strategy_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->shared_idle_strategy_init_args : NULL;
}

int aeron_driver_context_set_agent_on_start_function(
    aeron_driver_context_t *context, aeron_agent_on_start_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->agent_on_start_func = value;
    context->agent_on_start_state = state;

    return 0;
}

aeron_agent_on_start_func_t aeron_driver_context_get_agent_on_start_function(aeron_driver_context_t *context)
{
    return NULL != context ? context->agent_on_start_func : NULL;
}

void *aeron_driver_context_get_agent_on_start_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->agent_on_start_state : NULL;
}

int aeron_driver_context_set_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->counter_free_to_reuse_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_counters_free_to_reuse_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->counter_free_to_reuse_ns : AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_flow_control_receiver_timeout_ns(aeron_driver_context_t *context,  uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->flow_control.receiver_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_flow_control_receiver_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->flow_control.receiver_timeout_ns : AERON_FLOW_CONTROL_RECEIVER_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_flow_control_group_tag(aeron_driver_context_t *context, int64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->flow_control.group_tag = value;
    return 0;
}

int64_t aeron_driver_context_get_flow_control_group_tag(aeron_driver_context_t *context)
{
    return NULL != context ? context->flow_control.group_tag : AERON_FLOW_CONTROL_GROUP_TAG_DEFAULT;
}

int aeron_driver_context_set_flow_control_group_min_size(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->flow_control.group_min_size = value;
    return 0;
}

int32_t aeron_driver_context_get_flow_control_group_min_size(aeron_driver_context_t *context)
{
    return NULL != context ? context->flow_control.group_min_size :
        AERON_FLOW_CONTROL_GROUP_MIN_SIZE_DEFAULT;
}

int aeron_driver_context_set_receiver_group_tag(aeron_driver_context_t *context, bool is_present, int64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_group_tag.is_present = is_present;
    context->receiver_group_tag.value = value;
    return 0;
}

bool aeron_driver_context_get_receiver_group_tag_is_present(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_group_tag.is_present : AERON_RECEIVER_GROUP_TAG_IS_PRESENT_DEFAULT;
}

int64_t aeron_driver_context_get_receiver_group_tag_value(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_group_tag.value : AERON_RECEIVER_GROUP_TAG_VALUE_DEFAULT;
}

int aeron_driver_context_set_driver_termination_validator(
    aeron_driver_context_t *context, aeron_driver_termination_validator_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->termination_validator_func = value;
    context->termination_validator_state = state;
    return 0;
}

aeron_driver_termination_validator_func_t aeron_driver_context_get_driver_termination_validator(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_validator_func : aeron_driver_termination_validator_default_deny;
}

void *aeron_driver_context_get_driver_termination_validator_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_validator_state : NULL;
}

int aeron_driver_context_set_driver_termination_hook(
    aeron_driver_context_t *context, aeron_driver_termination_hook_func_t value, void *state)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->termination_hook_func = value;
    context->termination_hook_state = state;
    return 0;
}

aeron_driver_termination_hook_func_t aeron_driver_context_get_driver_termination_hook(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_hook_func : NULL;
}

void *aeron_driver_context_get_driver_termination_hook_state(aeron_driver_context_t *context)
{
    return NULL != context ? context->termination_hook_state : NULL;
}

int aeron_driver_context_set_print_configuration(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->print_configuration_on_start = value;
    return 0;
}

bool aeron_driver_context_get_print_configuration(aeron_driver_context_t *context)
{
    return NULL != context ? context->print_configuration_on_start : AERON_PRINT_CONFIGURATION_DEFAULT;
}

int aeron_driver_context_set_reliable_stream(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->reliable_stream = value;
    return 0;
}

bool aeron_driver_context_get_reliable_stream(aeron_driver_context_t *context)
{
    return NULL != context ? context->reliable_stream : AERON_RELIABLE_STREAM_DEFAULT;
}

int aeron_driver_context_set_tether_subscriptions(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->tether_subscriptions = value;
    return 0;
}

bool aeron_driver_context_get_tether_subscriptions(aeron_driver_context_t *context)
{
    return NULL != context ? context->tether_subscriptions : AERON_TETHER_SUBSCRIPTIONS_DEFAULT;
}

int aeron_driver_context_set_untethered_window_limit_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->untethered_window_limit_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_untethered_window_limit_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->untethered_window_limit_timeout_ns : AERON_UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_untethered_resting_timeout_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->untethered_resting_timeout_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_untethered_resting_timeout_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->untethered_resting_timeout_ns : AERON_UNTETHERED_RESTING_TIMEOUT_NS_DEFAULT;
}

int aeron_driver_context_set_driver_timeout_ms(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->driver_timeout_ms = value;
    return 0;
}

uint64_t aeron_driver_context_get_driver_timeout_ms(aeron_driver_context_t *context)
{
    return NULL != context ? context->driver_timeout_ms : AERON_DRIVER_TIMEOUT_MS_DEFAULT;
}

int aeron_driver_context_set_max_resend(aeron_driver_context_t *context, uint32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->max_resend = value;
    return 0;
}

uint32_t aeron_driver_context_get_max_resend(aeron_driver_context_t *context)
{
    return NULL != context ? context->max_resend : AERON_RETRANSMIT_HANDLER_MAX_RESEND;
}

int aeron_driver_context_set_retransmit_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->retransmit_unicast_delay_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_retransmit_unicast_delay_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->retransmit_unicast_delay_ns : AERON_RETRANSMIT_UNICAST_DELAY_NS_DEFAULT;
}

int aeron_driver_context_set_retransmit_unicast_linger_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->retransmit_unicast_linger_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_retransmit_unicast_linger_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->retransmit_unicast_linger_ns : AERON_RETRANSMIT_UNICAST_LINGER_NS_DEFAULT;
}

int aeron_driver_context_set_nak_multicast_group_size(aeron_driver_context_t *context, size_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_multicast_group_size = value;
    return 0;
}

size_t aeron_driver_context_get_nak_multicast_group_size(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_multicast_group_size : AERON_NAK_MULTICAST_GROUP_SIZE_DEFAULT;
}

int aeron_driver_context_set_nak_multicast_max_backoff_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_multicast_max_backoff_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_nak_multicast_max_backoff_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_multicast_max_backoff_ns : AERON_NAK_MULTICAST_MAX_BACKOFF_NS_DEFAULT;
}

int aeron_driver_context_set_nak_unicast_delay_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_unicast_delay_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_nak_unicast_delay_ns(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_unicast_delay_ns : AERON_NAK_UNICAST_DELAY_NS_DEFAULT;
}

int aeron_driver_context_set_nak_unicast_retry_delay_ratio(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->nak_unicast_retry_delay_ratio = value;
    return 0;
}

uint64_t aeron_driver_context_get_nak_unicast_retry_delay_ratio(aeron_driver_context_t *context)
{
    return NULL != context ? context->nak_unicast_retry_delay_ratio : AERON_NAK_UNICAST_RETRY_DELAY_RATIO_DEFAULT;
}

int aeron_driver_context_set_udp_channel_transport_bindings(
    aeron_driver_context_t *context, aeron_udp_channel_transport_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_transport_bindings = value;
    return 0;
}

aeron_udp_channel_transport_bindings_t *aeron_driver_context_get_udp_channel_transport_bindings(
    aeron_driver_context_t *context)
{
    return NULL != context ?
        context->udp_channel_transport_bindings :
        aeron_udp_channel_transport_bindings_load_media(AERON_UDP_CHANNEL_TRANSPORT_BINDINGS_MEDIA_DEFAULT);
}

int aeron_driver_context_set_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_outgoing_interceptor_bindings = value;
    return 0;
}

aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_outgoing_interceptors(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->udp_channel_outgoing_interceptor_bindings : NULL;
}

int aeron_driver_context_set_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context, aeron_udp_channel_interceptor_bindings_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->udp_channel_incoming_interceptor_bindings = value;
    return 0;
}

aeron_udp_channel_interceptor_bindings_t *aeron_driver_context_get_udp_channel_incoming_interceptors(
    aeron_driver_context_t *context)
{
    return NULL != context ? context->udp_channel_incoming_interceptor_bindings : NULL;
}

int aeron_driver_context_set_receiver_group_consideration(
    aeron_driver_context_t *context, aeron_inferable_boolean_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_group_consideration = value;
    return 0;
}

aeron_inferable_boolean_t aeron_driver_context_get_receiver_group_consideration(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_group_consideration : AERON_RECEIVER_GROUP_CONSIDERATION_DEFAULT;
}

int aeron_driver_context_set_rejoin_stream(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->rejoin_stream = value;
    return 0;
}

int aeron_driver_context_set_publication_reserved_session_id_low(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_reserved_session_id_low = value;
    return 0;
}

int32_t aeron_driver_context_get_publication_reserved_session_id_low(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_reserved_session_id_low : AERON_PUBLICATION_RESERVED_SESSION_ID_LOW_DEFAULT;
}

int aeron_driver_context_set_publication_reserved_session_id_high(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->publication_reserved_session_id_high = value;
    return 0;
}

int32_t aeron_driver_context_get_publication_reserved_session_id_high(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->publication_reserved_session_id_high : AERON_PUBLICATION_RESERVED_SESSION_ID_HIGH_DEFAULT;
}

bool aeron_driver_context_get_rejoin_stream(aeron_driver_context_t *context)
{
    return NULL != context ? context->rejoin_stream : AERON_REJOIN_STREAM_DEFAULT;
}

int aeron_driver_context_set_connect_enabled(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->connect_enabled = value;
    return 0;
}

int aeron_driver_context_get_connect_enabled(aeron_driver_context_t *context)
{
    return NULL != context ? context->connect_enabled : AERON_DRIVER_CONNECT_DEFAULT;
}

int aeron_driver_context_set_resolver_name(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->resolver_name = value;
    return 0;
}

const char *aeron_driver_context_get_resolver_name(aeron_driver_context_t *context)
{
    return NULL != context ? context->resolver_name : NULL;
}

int aeron_driver_context_set_resolver_interface(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->resolver_interface = value;
    return 0;
}

const char *aeron_driver_context_get_resolver_interface(aeron_driver_context_t *context)
{
    return NULL != context ? context->resolver_interface : NULL;
}

int aeron_driver_context_set_resolver_bootstrap_neighbor(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->resolver_bootstrap_neighbor = value;
    return 0;
}

const char *aeron_driver_context_get_resolver_bootstrap_neighbor(aeron_driver_context_t *context)
{
    return NULL != context ? context->resolver_bootstrap_neighbor : NULL;
}

int aeron_driver_context_set_name_resolver_supplier(
    aeron_driver_context_t *context, aeron_name_resolver_supplier_func_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->name_resolver_supplier_func = value;
    return 0;
}

aeron_name_resolver_supplier_func_t aeron_driver_context_get_name_resolver_supplier(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->name_resolver_supplier_func : aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_SUPPLIER_DEFAULT);
}

int aeron_driver_context_set_name_resolver_init_args(aeron_driver_context_t *context, const char *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->name_resolver_init_args = value;
    return 0;
}

const char *aeron_driver_context_get_name_resolver_init_args(aeron_driver_context_t *context)
{
    return NULL != context ? context->name_resolver_init_args : NULL;
}

int aeron_driver_context_set_re_resolution_check_interval_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->re_resolution_check_interval_ns = value;
    return 0;
}

uint64_t aeron_driver_context_get_re_resolution_check_interval_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->re_resolution_check_interval_ns : AERON_DRIVER_RERESOLUTION_CHECK_INTERVAL_NS_DEFAULT;
}

int aeron_driver_context_set_conductor_duty_cycle_tracker(
    aeron_driver_context_t *context, aeron_duty_cycle_tracker_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->conductor_duty_cycle_tracker = value;
    return 0;
}

aeron_duty_cycle_tracker_t *aeron_driver_context_get_conductor_duty_cycle_tracker(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_duty_cycle_tracker : NULL;
}

int aeron_driver_context_set_sender_duty_cycle_tracker(
    aeron_driver_context_t *context, aeron_duty_cycle_tracker_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->sender_duty_cycle_tracker = value;
    return 0;
}

aeron_duty_cycle_tracker_t *aeron_driver_context_get_sender_duty_cycle_tracker(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_duty_cycle_tracker : NULL;
}

int aeron_driver_context_set_receiver_duty_cycle_tracker(
    aeron_driver_context_t *context, aeron_duty_cycle_tracker_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_duty_cycle_tracker = value;
    return 0;
}

aeron_duty_cycle_tracker_t *aeron_driver_context_get_receiver_duty_cycle_tracker(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_duty_cycle_tracker : NULL;
}

int aeron_driver_context_set_name_resolver_time_tracker(
    aeron_driver_context_t *context, aeron_duty_cycle_tracker_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->name_resolver_time_tracker = value;
    return 0;
}

aeron_duty_cycle_tracker_t *aeron_driver_context_get_name_resolver_time_tracker(aeron_driver_context_t *context)
{
    return NULL != context ? context->name_resolver_time_tracker : NULL;
}

int64_t aeron_driver_context_set_conductor_cycle_threshold_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns = value;
    return 0;
}

int64_t aeron_driver_context_get_conductor_cycle_threshold_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns :
        AERON_DRIVER_CONDUCTOR_CYCLE_THRESHOLD_NS_DEFAULT;
}

int64_t aeron_driver_context_set_sender_cycle_threshold_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->sender_duty_cycle_stall_tracker.cycle_threshold_ns = value;
    return 0;
}

int64_t aeron_driver_context_get_sender_cycle_threshold_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->sender_duty_cycle_stall_tracker.cycle_threshold_ns :
        AERON_DRIVER_SENDER_CYCLE_THRESHOLD_NS_DEFAULT;
}

int64_t aeron_driver_context_set_receiver_cycle_threshold_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns = value;
    return 0;
}

int64_t aeron_driver_context_get_receiver_cycle_threshold_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns :
        AERON_DRIVER_RECEIVER_CYCLE_THRESHOLD_NS_DEFAULT;
}

int64_t aeron_driver_context_set_name_resolver_threshold_ns(aeron_driver_context_t *context, uint64_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->name_resolver_time_stall_tracker.cycle_threshold_ns = value;
    return 0;
}

int64_t aeron_driver_context_get_name_resolver_threshold_ns(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->name_resolver_time_stall_tracker.cycle_threshold_ns :
        AERON_DRIVER_NAME_RESOLVER_THRESHOLD_NS_DEFAULT;
}

int aeron_driver_context_set_sender_wildcard_port_range(
    aeron_driver_context_t *context, uint16_t low_port, uint16_t high_port)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_wildcard_port_manager_set_range(&context->sender_wildcard_port_manager, low_port, high_port);
    return 0;
}

int aeron_driver_context_get_sender_wildcard_port_range(
    aeron_driver_context_t *context, uint16_t *low_port, uint16_t *high_port)
{
    if (NULL == context)
    {
        return -1;
    }

    *low_port = context->sender_wildcard_port_manager.low_port;
    *high_port = context->sender_wildcard_port_manager.high_port;
    return 0;
}

int aeron_driver_context_set_receiver_wildcard_port_range(
    aeron_driver_context_t *context, uint16_t low_port, uint16_t high_port)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_wildcard_port_manager_set_range(&context->receiver_wildcard_port_manager, low_port, high_port);
    return 0;
}

int aeron_driver_context_get_receiver_wildcard_port_range(
    aeron_driver_context_t *context, uint16_t *low_port, uint16_t *high_port)
{
    if (NULL == context)
    {
        return -1;
    }

    *low_port = context->receiver_wildcard_port_manager.low_port;
    *high_port = context->receiver_wildcard_port_manager.high_port;
    return 0;
}

int aeron_driver_context_set_sender_port_manager(
    aeron_driver_context_t *context, aeron_port_manager_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->sender_port_manager = value;
    return 0;
}

aeron_port_manager_t *aeron_driver_context_get_sender_port_manager(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_port_manager : NULL;
}

int aeron_driver_context_set_receiver_port_manager(
    aeron_driver_context_t *context, aeron_port_manager_t *value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->receiver_port_manager = value;
    return 0;
}

aeron_port_manager_t *aeron_driver_context_get_receiver_port_manager(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_port_manager : NULL;
}

int aeron_driver_context_set_enable_experimental_features(aeron_driver_context_t *context, bool value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->enable_experimental_features = value;
    return 0;
}

int aeron_driver_context_get_enable_experimental_features(aeron_driver_context_t *context)
{
    return NULL != context ? context->enable_experimental_features : false;
}

int aeron_driver_context_set_stream_session_limit(aeron_driver_context_t *context, int32_t value)
{
    AERON_DRIVER_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->stream_session_limit = value;
    return 0;
}

int32_t aeron_driver_context_get_stream_session_limit(aeron_driver_context_t *context)
{
    return NULL != context ? context->stream_session_limit : AERON_DRIVER_STREAM_SESSION_LIMIT_DEFAULT;
}


int aeron_driver_context_bindings_clientd_find_first_free_index(aeron_driver_context_t *context)
{
    for (size_t i = 0; i < context->num_bindings_clientd_entries; i++)
    {
        if (NULL == context->bindings_clientd_entries[i].clientd)
        {
            return (int)i;
        }
    }

    return -1;
}

int aeron_driver_context_bindings_clientd_find(aeron_driver_context_t *context, const char *name)
{
    for (size_t i = 0; i < context->num_bindings_clientd_entries; i++)
    {
        aeron_driver_context_bindings_clientd_entry_t *entry = &context->bindings_clientd_entries[i];

        if (NULL != entry->name && 0 == strncmp(entry->name, name, strlen(name)))
        {
            return (int)i;
        }
    }

    return -1;
}

aeron_driver_context_bindings_clientd_entry_t *aeron_driver_context_bindings_clientd_get_or_find_first_free_entry(
    aeron_driver_context_t *context, const char *name)
{
    int index = aeron_driver_context_bindings_clientd_find(context, name);
    if (-1 == index)
    {
        index = aeron_driver_context_bindings_clientd_find_first_free_index(context);
        if (-1 == index)
        {
            return NULL;
        }

        context->bindings_clientd_entries[index].name = name;
    }

    return &context->bindings_clientd_entries[index];
}


static uint32_t aeron_driver_context_clamp_value(uint32_t value, uint32_t min, uint32_t max)
{
    uint32_t clamped_value;
    clamped_value = value > max ? max : value;
    clamped_value = clamped_value < min ? min : clamped_value;

    return clamped_value;
}

int aeron_driver_context_set_receiver_io_vector_capacity(aeron_driver_context_t *context, uint32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->receiver_io_vector_capacity = aeron_driver_context_clamp_value(
        value, 1, AERON_DRIVER_RECEIVER_IO_VECTOR_LENGTH_MAX);

    return 0;
}

uint32_t aeron_driver_context_get_receiver_io_vector_capacity(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_io_vector_capacity : AERON_RECEIVER_IO_VECTOR_CAPACITY_DEFAULT;
}

int aeron_driver_context_set_sender_io_vector_capacity(aeron_driver_context_t *context, uint32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->sender_io_vector_capacity = aeron_driver_context_clamp_value(
        value, 1, AERON_DRIVER_SENDER_IO_VECTOR_LENGTH_MAX);

    return 0;
}

uint32_t aeron_driver_context_get_sender_io_vector_capacity(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_io_vector_capacity : AERON_SENDER_IO_VECTOR_CAPACITY_DEFAULT;
}

int aeron_driver_context_set_network_publication_max_messages_per_send(aeron_driver_context_t *context, uint32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->network_publication_max_messages_per_send = aeron_driver_context_clamp_value(
        value, 1, AERON_NETWORK_PUBLICATION_MAX_MESSAGES_PER_SEND);
    return 0;
}

uint32_t aeron_driver_context_get_network_publication_max_messages_per_send(aeron_driver_context_t *context)
{
    return NULL != context ?
        context->network_publication_max_messages_per_send : AERON_SENDER_MAX_MESSAGES_PER_SEND_DEFAULT;
}

int aeron_driver_context_set_resource_free_limit(aeron_driver_context_t *context, uint32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->resource_free_limit = value;
    return 0;
}

uint32_t aeron_driver_context_get_resource_free_limit(aeron_driver_context_t *context)
{
    return NULL != context ? context->resource_free_limit : AERON_DRIVER_RESOURCE_FREE_LIMIT_DEFAULT;
}

int aeron_driver_context_set_async_executor_threads(aeron_driver_context_t *context, uint32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->async_executor_threads = value;
    return 0;
}

uint32_t aeron_driver_context_get_async_executor_threads(aeron_driver_context_t *context)
{
    return NULL != context ? context->async_executor_threads : AERON_DRIVER_ASYNC_EXECUTOR_THREADS_DEFAULT;
}

void aeron_set_thread_affinity_on_start(void *state, const char *role_name)
{
    aeron_driver_context_t *context = (aeron_driver_context_t *)state;
    int result = 0;
    if (0 <= context->conductor_cpu_affinity_no &&
       (0 == strcmp("conductor", role_name) ||
        0 == strcmp("[conductor, sender, receiver]", role_name)))
    {
        result = aeron_thread_set_affinity(role_name, (uint8_t)context->conductor_cpu_affinity_no);
    }
    else if (0 <= context->sender_cpu_affinity_no &&
            (0 == strcmp("sender", role_name) ||
             0 == strcmp("[sender, receiver]", role_name)))
    {
        result = aeron_thread_set_affinity(role_name, (uint8_t)context->sender_cpu_affinity_no);
    }
    else if (0 <= context->receiver_cpu_affinity_no && 0 == strcmp("receiver", role_name))
    {
        result = aeron_thread_set_affinity(role_name, (uint8_t)context->receiver_cpu_affinity_no);
    }

    if (result < 0)
    {
        AERON_APPEND_ERR("%s", "WARNING: unable to apply affinity");
        // Just in case the error log is not initialised, but it should be by this point.
        if (NULL != context->error_log)
        {
            aeron_distinct_error_log_record(context->error_log, aeron_errcode(), aeron_errmsg());
        }
        else
        {
            fprintf(stderr, "%s", aeron_errmsg());
        }
        aeron_err_clear();
    }
}


int aeron_driver_context_set_conductor_cpu_affinity(aeron_driver_context_t *context, int32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->conductor_cpu_affinity_no = value;
    return 0;
}

int32_t aeron_driver_context_get_conductor_cpu_affinity(aeron_driver_context_t *context)
{
    return NULL != context ? context->conductor_cpu_affinity_no : AERON_CPU_AFFINITY_DEFAULT;
}

int aeron_driver_context_set_sender_cpu_affinity(aeron_driver_context_t *context, int32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->sender_cpu_affinity_no = value;
    return 0;
}

int32_t aeron_driver_context_get_sender_cpu_affinity(aeron_driver_context_t *context)
{
    return NULL != context ? context->sender_cpu_affinity_no : AERON_CPU_AFFINITY_DEFAULT;
}

int aeron_driver_context_set_receiver_cpu_affinity(aeron_driver_context_t *context, int32_t value)
{
    if (NULL == context)
    {
        return -1;
    }

    context->receiver_cpu_affinity_no = value;
    return 0;
}

int32_t aeron_driver_context_get_receiver_cpu_affinity(aeron_driver_context_t *context)
{
    return NULL != context ? context->receiver_cpu_affinity_no :AERON_CPU_AFFINITY_DEFAULT;
}
