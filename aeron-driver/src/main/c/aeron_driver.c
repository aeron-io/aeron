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
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include "util/aeron_platform.h"
#if defined(AERON_COMPILER_MSVC)
#define _CRT_RAND_S

#define S_IRWXU 0
#define S_IRWXG 0
#define S_IRWXO 0
#endif
#include <stdlib.h>
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <inttypes.h>

#include "util/aeron_error.h"
#include "aeronmd.h"
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"
#include "aeron_socket.h"
#include "util/aeron_dlopen.h"

void aeron_log_func_stderr(const char *str)
{
    fprintf(stderr, "%s\n", str);
}

void aeron_log_func_none(const char *str)
{
}

static void error_log_reader_save_to_file(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd)
{
    FILE *saved_errors_file = (FILE *)clientd;
    char first_datestamp[AERON_FORMAT_DATE_MAX_LENGTH];
    char last_datestamp[AERON_FORMAT_DATE_MAX_LENGTH];

    aeron_format_date(first_datestamp, sizeof(first_datestamp) - 1, first_observation_timestamp);
    aeron_format_date(last_datestamp, sizeof(last_datestamp) - 1, last_observation_timestamp);
    fprintf(
        saved_errors_file,
        "***\n%d observations from %s to %s for:\n %.*s\n",
        observation_count,
        first_datestamp,
        last_datestamp,
        (int)error_length,
        error);
}

int aeron_report_existing_errors(aeron_mapped_file_t *cnc_map, const char *aeron_dir)
{
    char buffer[AERON_MAX_PATH];
    int result = 0;

    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_map->addr;

    if (aeron_semantic_version_major(AERON_CNC_VERSION) == aeron_semantic_version_major(metadata->cnc_version) &&
        aeron_error_log_exists(aeron_cnc_error_log_buffer(cnc_map->addr), (size_t)metadata->error_log_buffer_length))
    {
        char datestamp[AERON_FORMAT_DATE_MAX_LENGTH];
        FILE *saved_errors_file = NULL;

        aeron_format_date(datestamp, sizeof(datestamp) - 1, aeron_epoch_clock());
        while (true)
        {
            char *invalid_win_symbol = strstr(datestamp, ":");
            if (invalid_win_symbol == NULL)
            {
                break;
            }

            *invalid_win_symbol = '-';
        }

        snprintf(buffer, sizeof(buffer), "%s-%s-error.log", aeron_dir, datestamp);

        if ((saved_errors_file = fopen(buffer, "w")) != NULL)
        {
            uint64_t observations = aeron_error_log_read(
                aeron_cnc_error_log_buffer(metadata),
                (size_t)metadata->error_log_buffer_length,
                error_log_reader_save_to_file,
                saved_errors_file,
                0);

            fprintf(saved_errors_file, "\n%" PRIu64 " distinct errors observed.\n", observations);
            fprintf(stderr, "WARNING: Existing errors saved to: %s\n", buffer);

            fclose(saved_errors_file);
        }
        else
        {
            AERON_SET_ERR(errno, "Failed to open saved_error_file: %s", buffer);
            result = -1;
        }
    }

    return result;
}

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context)
{
    char filename[AERON_MAX_PATH];
    char buffer[2 * AERON_MAX_PATH];
    const char *dirname = context->aeron_dir;
    aeron_log_func_t log_func = aeron_log_func_none;

    if (aeron_is_directory(dirname))
    {
        if (context->warn_if_dirs_exist)
        {
            log_func = aeron_log_func_stderr;
            snprintf(buffer, sizeof(buffer), "WARNING: %s exists", dirname);
            log_func(buffer);
        }

        if (context->dirs_delete_on_start)
        {
            if (0 != aeron_delete_directory(dirname))
            {
                snprintf(buffer, sizeof(buffer), "INFO: failed to delete: %s", dirname);
                log_func(buffer);
                return -1;
            }
        }
        else
        {
            aeron_mapped_file_t cnc_mmap = { .addr = NULL, .length = 0 };

            if (aeron_cnc_resolve_filename(dirname, filename, sizeof(filename)) < 0)
            {
                snprintf(buffer, sizeof(buffer), "INFO: failed to resole CnC file: path=%s", dirname);
                log_func(buffer);
                return -1;
            }

            if (aeron_map_existing_file(&cnc_mmap, filename) < 0)
            {
                if (ENOENT == aeron_errcode())
                {
                    aeron_err_clear();
                }
                else
                {
                    snprintf(buffer, sizeof(buffer), "INFO: failed to mmap CnC file: %s", filename);
                    log_func(buffer);
                    return -1;
                }
            }
            else
            {
                snprintf(buffer, sizeof(buffer), "INFO: Aeron CnC file %s exists", filename);
                log_func(buffer);

                if (aeron_is_driver_active_with_cnc(
                    &cnc_mmap, (int64_t)context->driver_timeout_ms, aeron_epoch_clock(), log_func))
                {
                    aeron_unmap(&cnc_mmap);
                    return -1;
                }

                if (aeron_report_existing_errors(&cnc_mmap, dirname) < 0)
                {
                    aeron_unmap(&cnc_mmap);
                    return -1;
                }

                aeron_unmap(&cnc_mmap);
            }

            if (aeron_delete_directory(context->aeron_dir) != 0)
            {
                snprintf(buffer, sizeof(buffer) - 1, "INFO: failed to delete %s", context->aeron_dir);
                log_func(buffer);
            }
        }
    }

    if (aeron_mkdir_recursive(dirname, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        AERON_APPEND_ERR("Failed to mkdir aeron directory: %s", dirname);
        return -1;
    }

    if (aeron_file_resolve(dirname, AERON_PUBLICATIONS_DIR, filename, sizeof(filename)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to get publications directory filename");
        return -1;
    }

    if (aeron_mkdir_recursive(filename, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        AERON_APPEND_ERR("Failed to mkdir publications directory: %s", filename);
        return -1;
    }

    if (aeron_file_resolve(dirname, AERON_IMAGES_DIR, filename, sizeof(filename)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to get images directory filename");
        return -1;
    }

    if (aeron_mkdir_recursive(filename, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
    {
        AERON_SET_ERR(errno, "Failed to mkdir images directory: %s", filename);
        return -1;
    }

    return 0;
}

void aeron_driver_fill_cnc_metadata(aeron_driver_context_t *context)
{
    aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)context->cnc_map.addr;
    metadata->to_driver_buffer_length = (int32_t)context->to_driver_buffer_length;
    metadata->to_clients_buffer_length = (int32_t)context->to_clients_buffer_length;
    metadata->counter_metadata_buffer_length =
        (int32_t)(AERON_COUNTERS_METADATA_BUFFER_LENGTH(context->counters_values_buffer_length));
    metadata->counter_values_buffer_length = (int32_t)context->counters_values_buffer_length;
    metadata->error_log_buffer_length = (int32_t)context->error_buffer_length;
    metadata->client_liveness_timeout = (int64_t)context->client_liveness_timeout_ns;
    metadata->start_timestamp = context->epoch_clock();
    metadata->pid = getpid();

    context->to_driver_buffer = aeron_cnc_to_driver_buffer(metadata);
    context->to_clients_buffer = aeron_cnc_to_clients_buffer(metadata);
    context->counters_values_buffer = aeron_cnc_counters_values_buffer(metadata);
    context->counters_metadata_buffer = aeron_cnc_counters_metadata_buffer(metadata);
    context->error_buffer = aeron_cnc_error_log_buffer(metadata);
}

int aeron_driver_validate_value_range(uint64_t value, uint64_t min_value, uint64_t max_value, const char *name)
{
    if (value < min_value)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s less than min size of %" PRIu64 ": page size=%" PRIu64,
            name, min_value, value);
        return -1;
    }

    if (value > max_value)
    {
        AERON_SET_ERR(
            EINVAL,
            "%s greater than max size of %" PRIu64 ": page size=%" PRIu64,
            name, max_value, value);
        return -1;
    }

    return 0;
}

int aeron_driver_create_cnc_file(aeron_driver_t *driver)
{
    char buffer[AERON_MAX_PATH];
    size_t cnc_file_length = aeron_cnc_length(driver->context);
    if (aeron_driver_validate_value_range(cnc_file_length, 0, INT32_MAX, "CnC file length") < 0)
    {
        return -1;
    }

    driver->context->cnc_map.addr = NULL;
    driver->context->cnc_map.length = cnc_file_length;

    if(aeron_file_resolve(driver->context->aeron_dir, AERON_CNC_FILE, buffer, sizeof(buffer)) < 0)
    {
        AERON_APPEND_ERR("Failed to resolve CnC file path: dir=%s, filename=%s", driver->context->aeron_dir, AERON_CNC_FILE);
        return -1;
    }

    if (aeron_map_new_file(&driver->context->cnc_map, buffer, true) < 0)
    {
        AERON_APPEND_ERR("CnC file: %s", buffer);
        return -1;
    }

    aeron_driver_fill_cnc_metadata(driver->context);

    return 0;
}

int aeron_driver_create_loss_report_file(aeron_driver_t *driver)
{
    char buffer[AERON_MAX_PATH];

    driver->context->loss_report.addr = NULL;
    driver->context->loss_report.length =
        AERON_ALIGN(driver->context->loss_report_length, driver->context->file_page_size);

    if (aeron_loss_reporter_resolve_filename(driver->context->aeron_dir, buffer, sizeof(buffer)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to get loss report filename");
        return -1;
    }

    if (aeron_map_new_file(&driver->context->loss_report, buffer, true) < 0)
    {
        AERON_APPEND_ERR("could not map loss report file: %s", buffer);
        return -1;
    }

    return 0;
}

int aeron_driver_validate_sufficient_socket_buffer_lengths(aeron_driver_t *driver)
{
    int result = -1;
    aeron_socket_t probe_fd;

    if ((probe_fd = aeron_socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to probe socket for buffer lengths");
        goto cleanup;
    }

    size_t default_sndbuf = 0;
    socklen_t len = sizeof(default_sndbuf);
    if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &default_sndbuf, &len) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_SNDBUF option");
        goto cleanup;
    }

    size_t default_rcvbuf = 0;
    len = sizeof(default_rcvbuf);
    if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &default_rcvbuf, &len) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_RCVBUF option");
        goto cleanup;
    }

    size_t max_rcvbuf = default_rcvbuf;
    size_t max_sndbuf = default_sndbuf;

    if (driver->context->socket_sndbuf > 0)
    {
        size_t socket_sndbuf = driver->context->socket_sndbuf;

        if (aeron_setsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, sizeof(socket_sndbuf)) < 0)
        {
            AERON_APPEND_ERR("failed to set SOL_SOCKET/SO_SNDBUF option to: %" PRIu64, (uint64_t)socket_sndbuf);
            goto cleanup;
        }

        len = sizeof(socket_sndbuf);
        if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_SNDBUF, &socket_sndbuf, &len) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_SNDBUF option");
            goto cleanup;
        }

        max_sndbuf = socket_sndbuf;

        if (driver->context->socket_sndbuf > socket_sndbuf)
        {
            fprintf(
                stderr,
                "WARNING: Could not get desired SO_SNDBUF, adjust OS buffer to match %s: attempted=%" PRIu64 ", actual=%" PRIu64 "\n",
                AERON_SOCKET_SO_SNDBUF_ENV_VAR,
                (uint64_t)driver->context->socket_sndbuf,
                (uint64_t)socket_sndbuf);
        }
    }

    if (driver->context->socket_rcvbuf > 0)
    {
        size_t socket_rcvbuf = driver->context->socket_rcvbuf;

        if (aeron_setsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, sizeof(socket_rcvbuf)) < 0)
        {
            AERON_APPEND_ERR("failed to set SOL_SOCKET/SO_RCVBUF option to: %" PRIu64, (uint64_t)socket_rcvbuf);
            goto cleanup;
        }

        len = sizeof(socket_rcvbuf);
        if (aeron_getsockopt(probe_fd, SOL_SOCKET, SO_RCVBUF, &socket_rcvbuf, &len) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_RCVBUF option");
            goto cleanup;
        }

        max_rcvbuf = socket_rcvbuf;

        if (driver->context->socket_rcvbuf > socket_rcvbuf)
        {
            fprintf(
                stderr,
                "WARNING: Could not get desired SO_RCVBUF, adjust OS buffer to match %s: attempted=%" PRIu64 ", actual=%" PRIu64 "\n",
                AERON_SOCKET_SO_RCVBUF_ENV_VAR,
                (uint64_t)driver->context->socket_rcvbuf,
                (uint64_t)socket_rcvbuf);
        }
    }

    if (driver->context->mtu_length > max_sndbuf)
    {
        AERON_SET_ERR(
            EINVAL,
            "MTU greater than socket SO_SNDBUF, adjust %s to match MTU: mtuLength=%" PRIu64 ", SO_SNDBUF=%" PRIu64 "\n",
            AERON_SOCKET_SO_SNDBUF_ENV_VAR,
            (uint64_t)driver->context->mtu_length,
            max_sndbuf);
        goto cleanup;
    }

    if (driver->context->initial_window_length > max_rcvbuf)
    {
        AERON_SET_ERR(
            EINVAL,
            "Window length greater than socket SO_RCVBUF, increase %s to match window: windowLength=%" PRIu64 ", SO_RCVBUF=%" PRIu64 "\n",
            AERON_RCV_INITIAL_WINDOW_LENGTH_ENV_VAR,
            (uint64_t)driver->context->initial_window_length,
            max_rcvbuf);
        goto cleanup;
    }

    result = 0;

cleanup:
    aeron_close_socket(probe_fd);

    return result;
}

int aeron_driver_validate_page_size(aeron_driver_t *driver)
{
    if (aeron_driver_validate_value_range(
        driver->context->file_page_size, AERON_PAGE_MIN_SIZE, AERON_PAGE_MAX_SIZE, "file_page_size") < 0)
    {
        return -1;
    }

    if (!AERON_IS_POWER_OF_TWO(driver->context->file_page_size))
    {
        AERON_SET_ERR(
            EINVAL,
            "Page size not a power of 2: page size=%" PRIu64,
            driver->context->file_page_size);
        return -1;
    }

    return 0;
}

const char *aeron_driver_threading_mode_to_string(aeron_threading_mode_t mode)
{
    switch (mode)
    {
        case AERON_THREADING_MODE_DEDICATED:
            return "DEDICATED";
        case AERON_THREADING_MODE_SHARED:
            return "SHARED";
        case AERON_THREADING_MODE_SHARED_NETWORK:
            return "SHARED_NETWORK";
        case AERON_THREADING_MODE_INVOKER:
            return "INVOKER";
        default:
            return "unknown";
    }
}

void aeron_driver_context_print_configuration(aeron_driver_context_t *context)
{
    FILE *fpout = stdout;
    char buffer[1024];

    fprintf(fpout, "aeron_driver_context_t {");
    fprintf(fpout, "\n    cnc_version=%d.%d.%d",
        (int)aeron_semantic_version_major(AERON_CNC_VERSION),
        (int)aeron_semantic_version_minor(AERON_CNC_VERSION),
        (int)aeron_semantic_version_patch(AERON_CNC_VERSION));
    fprintf(fpout, "\n    aeron_dir=%s", context->aeron_dir);
    fprintf(fpout, "\n    driver_timeout_ms=%" PRIu64, context->driver_timeout_ms);
    fprintf(fpout, "\n    print_configuration_on_start=%d", context->print_configuration_on_start);
    fprintf(fpout, "\n    dirs_delete_on_start=%d", context->dirs_delete_on_start);
    fprintf(fpout, "\n    dirs_delete_on_shutdown=%d", context->dirs_delete_on_shutdown);
    fprintf(fpout, "\n    warn_if_dirs_exists=%d", context->warn_if_dirs_exist);
    fprintf(fpout, "\n    term_buffer_sparse_file=%d", context->term_buffer_sparse_file);
    fprintf(fpout, "\n    perform_storage_checks=%d", context->perform_storage_checks);
    fprintf(fpout, "\n    spies_simulate_connection=%d", context->spies_simulate_connection);
    fprintf(fpout, "\n    reliable_stream=%d", context->reliable_stream);
    fprintf(fpout, "\n    tether_subscriptions=%d", context->tether_subscriptions);
    fprintf(fpout, "\n    rejoin_stream=%d", context->rejoin_stream);
    fprintf(fpout, "\n    receiver_group_consideration=%d", context->receiver_group_consideration);
    fprintf(fpout, "\n    to_driver_buffer_length=%" PRIu64, (uint64_t)context->to_driver_buffer_length);
    fprintf(fpout, "\n    to_clients_buffer_length=%" PRIu64, (uint64_t)context->to_clients_buffer_length);
    fprintf(fpout, "\n    counters_values_buffer_length=%" PRIu64, (uint64_t)context->counters_values_buffer_length);
    fprintf(fpout, "\n    error_buffer_length=%" PRIu64, (uint64_t)context->error_buffer_length);
    fprintf(fpout, "\n    timer_interval_ns=%" PRIu64, context->timer_interval_ns);
    fprintf(fpout, "\n    client_liveness_timeout_ns=%" PRIu64, context->client_liveness_timeout_ns);
    fprintf(fpout, "\n    image_liveness_timeout_ns=%" PRIu64, context->image_liveness_timeout_ns);
    fprintf(fpout, "\n    publication_unblock_timeout_ns=%" PRIu64, context->publication_unblock_timeout_ns);
    fprintf(fpout, "\n    publication_connection_timeout_ns=%" PRIu64, context->publication_connection_timeout_ns);
    fprintf(fpout, "\n    publication_linger_timeout_ns=%" PRIu64, context->publication_linger_timeout_ns);
    fprintf(fpout, "\n    untethered_window_limit_timeout_ns=%" PRIu64, context->untethered_window_limit_timeout_ns);
    fprintf(fpout, "\n    untethered_resting_timeout_ns=%" PRIu64, context->untethered_resting_timeout_ns);
    fprintf(fpout, "\n    max_resend=%" PRIu32, context->max_resend);
    fprintf(fpout, "\n    retransmit_unicast_delay_ns=%" PRIu64, context->retransmit_unicast_delay_ns);
    fprintf(fpout, "\n    retransmit_unicast_linger_ns=%" PRIu64, context->retransmit_unicast_linger_ns);
    fprintf(fpout, "\n    nak_unicast_delay_ns=%" PRIu64, context->nak_unicast_delay_ns);
    fprintf(fpout, "\n    nak_unicast_retry_delay_ratio=%" PRIu64, context->nak_unicast_retry_delay_ratio);
    fprintf(fpout, "\n    nak_multicast_max_backoff_ns=%" PRIu64, context->nak_multicast_max_backoff_ns);
    fprintf(fpout, "\n    nak_multicast_group_size=%" PRIu64, (uint64_t)context->nak_multicast_group_size);
    fprintf(fpout, "\n    status_message_timeout_ns=%" PRIu64, context->status_message_timeout_ns);
    fprintf(fpout, "\n    counter_free_to_reuse_ns=%" PRIu64, context->counter_free_to_reuse_ns);
    fprintf(fpout, "\n    conductor_cycle_threshold_ns=%" PRIu64,
        context->conductor_duty_cycle_stall_tracker.cycle_threshold_ns);
    fprintf(fpout, "\n    sender_cycle_threshold_ns=%" PRIu64,
        context->sender_duty_cycle_stall_tracker.cycle_threshold_ns);
    fprintf(fpout, "\n    receiver_cycle_threshold_ns=%" PRIu64,
        context->receiver_duty_cycle_stall_tracker.cycle_threshold_ns);
    fprintf(fpout, "\n    name_resolver_threshold_ns=%" PRIu64,
        context->name_resolver_time_stall_tracker.cycle_threshold_ns);
    fprintf(fpout, "\n    term_buffer_length=%" PRIu64, (uint64_t)context->term_buffer_length);
    fprintf(fpout, "\n    ipc_term_buffer_length=%" PRIu64, (uint64_t)context->ipc_term_buffer_length);
    fprintf(fpout, "\n    publication_window_length=%" PRIu64, (uint64_t)context->publication_window_length);
    fprintf(fpout, "\n    ipc_publication_window_length=%" PRIu64, (uint64_t)context->ipc_publication_window_length);
    fprintf(fpout, "\n    initial_window_length=%" PRIu64, (uint64_t)context->initial_window_length);
    fprintf(fpout, "\n    socket_sndbuf_length=%" PRIu64, (uint64_t)context->socket_sndbuf);
    fprintf(fpout, "\n    socket_rcvbuf_length=%" PRIu64, (uint64_t)context->socket_rcvbuf);
    fprintf(fpout, "\n    multicast_ttl=%" PRIu8, context->multicast_ttl);
    fprintf(fpout, "\n    mtu_length=%" PRIu64, (uint64_t)context->mtu_length);
    fprintf(fpout, "\n    ipc_mtu_length=%" PRIu64, (uint64_t)context->ipc_mtu_length);
    fprintf(fpout, "\n    file_page_size=%" PRIu64, (uint64_t)context->file_page_size);
    fprintf(fpout, "\n    low_file_store_warning_threshold=%" PRIu64, (uint64_t)context->low_file_store_warning_threshold);
    fprintf(fpout, "\n    publication_reserved_session_id_low=%" PRId32, context->publication_reserved_session_id_low);
    fprintf(fpout, "\n    publication_reserved_session_id_high=%" PRId32, context->publication_reserved_session_id_high);
    fprintf(fpout, "\n    loss_report_length=%" PRIu64, (uint64_t)context->loss_report_length);
    fprintf(fpout, "\n    send_to_sm_poll_ratio=%" PRIu64, (uint64_t)context->send_to_sm_poll_ratio);
    fprintf(fpout, "\n    receiver_io_vector_capacity=%" PRIu64, (uint64_t)context->receiver_io_vector_capacity);
    fprintf(fpout, "\n    sender_io_vector_capacity=%" PRIu64, (uint64_t)context->sender_io_vector_capacity);
    fprintf(
        fpout, "\n    network_publication_max_messages_per_send=%" PRIu64,
        (uint64_t)context->network_publication_max_messages_per_send);
    fprintf(fpout, "\n    resource_free_limit=%" PRIu32, context->resource_free_limit);
    fprintf(fpout, "\n    async_executor_threads=%" PRIu32, context->async_executor_threads);
    fprintf(fpout, "\n    conductor_cpu_affinity_no=%" PRId32, context->conductor_cpu_affinity_no);
    fprintf(fpout, "\n    receiver_cpu_affinity_no=%" PRId32, context->receiver_cpu_affinity_no);
    fprintf(fpout, "\n    sender_cpu_affinity_no=%" PRId32, context->sender_cpu_affinity_no);

    fprintf(fpout, "\n    epoch_clock=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->epoch_clock, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    nano_clock=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->nano_clock, buffer, sizeof(buffer)));
    /* cachedEpochClock */
    /* cachedNanoClock */
    fprintf(fpout, "\n    threading_mode=%s", aeron_driver_threading_mode_to_string(context->threading_mode));
    fprintf(fpout, "\n    agent_on_start_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->agent_on_start_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    agent_on_start_state=%p", context->agent_on_start_state);
    fprintf(fpout, "\n    conductor_idle_strategy_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->conductor_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    conductor_idle_strategy_init_args=%p%s",
        (void *)context->conductor_idle_strategy_init_args,
        context->conductor_idle_strategy_init_args ? context->conductor_idle_strategy_init_args : "");
    fprintf(fpout, "\n    sender_idle_strategy_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->sender_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    sender_idle_strategy_init_args=%p%s",
        (void *)context->sender_idle_strategy_init_args,
        context->sender_idle_strategy_init_args ? context->sender_idle_strategy_init_args : "");
    fprintf(fpout, "\n    receiver_idle_strategy_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->receiver_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    receiver_idle_strategy_init_args=%p%s",
        (void *)context->receiver_idle_strategy_init_args,
        context->receiver_idle_strategy_init_args ? context->receiver_idle_strategy_init_args : "");
    fprintf(fpout, "\n    shared_network_idle_strategy_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->shared_network_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    shared_network_idle_strategy_init_args=%p%s",
        (void *)context->shared_network_idle_strategy_init_args,
        context->shared_network_idle_strategy_init_args ? context->shared_network_idle_strategy_init_args : "");
    fprintf(fpout, "\n    shared_idle_strategy_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->shared_idle_strategy_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    shared_idle_strategy_init_args=%p%s",
        (void *)context->shared_idle_strategy_init_args,
        context->shared_idle_strategy_init_args ? context->shared_idle_strategy_init_args : "");
    fprintf(fpout, "\n    unicast_flow_control_supplier_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->unicast_flow_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    multicast_flow_control_supplier_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->multicast_flow_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    receiver_group_tag.is_present=%d",
        context->receiver_group_tag.is_present);
    fprintf(fpout, "\n    receiver_group_tag.value=%" PRId64, context->receiver_group_tag.value);
    fprintf(fpout, "\n    flow_control.group_tag=%" PRId64, context->flow_control.group_tag);
    fprintf(fpout, "\n    flow_control.group_min_size=%" PRId32, context->flow_control.group_min_size);
    fprintf(fpout, "\n    flow_control_receiver_timeout_ns=%" PRIu64, context->flow_control.receiver_timeout_ns);
    fprintf(fpout, "\n    congestion_control_supplier_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->congestion_control_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    usable_fs_space_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->usable_fs_space_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_validator_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->termination_validator_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_validator_state=%p", context->termination_validator_state);
    fprintf(fpout, "\n    termination_hook_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->termination_hook_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    termination_hook_state=%p", context->termination_hook_state);
    fprintf(fpout, "\n    name_resolver_supplier_func=%s",
        aeron_dlinfo_func((aeron_fptr_t)context->name_resolver_supplier_func, buffer, sizeof(buffer)));
    fprintf(fpout, "\n    name_resolver_init_args=%s",
        (void *)context->name_resolver_init_args ? context->name_resolver_init_args : "");
    fprintf(fpout, "\n    resolver_name=%s",
        (void *)context->resolver_name ? context->resolver_name : "");
    fprintf(fpout, "\n    resolver_interface=%s",
        (void *)context->resolver_interface ? context->resolver_interface : "");
    fprintf(fpout, "\n    resolver_bootstrap_neighbor=%s",
        (void *)context->resolver_bootstrap_neighbor ? context->resolver_bootstrap_neighbor : "");
    fprintf(fpout, "\n    re_resolution_check_interval_ns=%" PRIu64, context->re_resolution_check_interval_ns);
    fprintf(fpout, "\n    sender_wildcard_port_range=\"%" PRIu16 " %" PRIu16 "\"", 
        context->sender_wildcard_port_manager.low_port, context->sender_wildcard_port_manager.high_port);
    fprintf(fpout, "\n    receiver_wildcard_port_range=\"%" PRIu16 " %" PRIu16 "\"",
        context->receiver_wildcard_port_manager.low_port, context->receiver_wildcard_port_manager.high_port);
    fprintf(fpout, "\n    enable_experimental_features=%s", context->enable_experimental_features ? "true" : "false");
    fprintf(fpout, "\n    stream_session_limit=%" PRId32, context->stream_session_limit);

    const aeron_udp_channel_transport_bindings_t *bindings = context->udp_channel_transport_bindings;
    if (NULL != bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_transport_bindings.%s=%s,%p%s",
            bindings->meta_info.type,
            bindings->meta_info.name,
            bindings->meta_info.source_symbol,
            aeron_dlinfo(bindings->meta_info.source_symbol, buffer, sizeof(buffer)));
    }

    const aeron_udp_channel_transport_bindings_t *conductor_bindings = context->conductor_udp_channel_transport_bindings;
    if (NULL != conductor_bindings)
    {
        fprintf(
            fpout, "\n    conductor_udp_channel_transport_bindings.%s=%s,%p%s",
            conductor_bindings->meta_info.type,
            conductor_bindings->meta_info.name,
            conductor_bindings->meta_info.source_symbol,
            aeron_dlinfo(conductor_bindings->meta_info.source_symbol, buffer, sizeof(buffer)));
    }

    const aeron_udp_channel_interceptor_bindings_t *interceptor_bindings;

    interceptor_bindings = context->udp_channel_outgoing_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_outgoing_interceptor_bindings.%s=%s,%s",
            interceptor_bindings->meta_info.type,
            interceptor_bindings->meta_info.name,
            aeron_dlinfo_func(interceptor_bindings->meta_info.source_symbol, buffer, sizeof(buffer)));

        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

    interceptor_bindings = context->udp_channel_incoming_interceptor_bindings;
    while (NULL != interceptor_bindings)
    {
        fprintf(
            fpout, "\n    udp_channel_incoming_interceptor_bindings.%s=%s,%s",
            interceptor_bindings->meta_info.type,
            interceptor_bindings->meta_info.name,
            aeron_dlinfo_func(interceptor_bindings->meta_info.source_symbol, buffer, sizeof(buffer)));

        interceptor_bindings = interceptor_bindings->meta_info.next_interceptor_bindings;
    }

    fprintf(fpout, "\n}\n");
    fflush(fpout);
}

int aeron_driver_shared_do_work(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;
    int sum = 0;

    sum += aeron_driver_conductor_do_work(&driver->conductor);
    sum += aeron_driver_sender_do_work(&driver->sender);
    sum += aeron_driver_receiver_do_work(&driver->receiver);

    return sum;
}

void aeron_driver_shared_on_close(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;

    aeron_driver_conductor_on_close(&driver->conductor);
    aeron_driver_sender_on_close(&driver->sender);
    aeron_driver_receiver_on_close(&driver->receiver);
}

int aeron_driver_shared_network_do_work(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;
    int sum = 0;

    sum += aeron_driver_sender_do_work(&driver->sender);
    sum += aeron_driver_receiver_do_work(&driver->receiver);

    return sum;
}

void aeron_driver_shared_network_on_close(void *clientd)
{
    aeron_driver_t *driver = (aeron_driver_t *)clientd;

    aeron_driver_sender_on_close(&driver->sender);
    aeron_driver_receiver_on_close(&driver->receiver);
}

int aeron_driver_init(aeron_driver_t **driver, aeron_driver_context_t *context)
{
    aeron_driver_t *_driver = NULL;

    if (NULL == driver || NULL == context)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver or context are null");
        goto error;
    }

    if (aeron_alloc((void **)&_driver, sizeof(aeron_driver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate driver");
        goto error;
    }

    if (aeron_driver_context_bindings_clientd_create_entries(context) < 0)
    {
        goto error;
    }

    _driver->context = context;

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        _driver->runners[i].state = AERON_AGENT_STATE_UNUSED;
        _driver->runners[i].role_name = NULL;
        _driver->runners[i].on_close = NULL;
    }

    if (aeron_logbuffer_check_term_length(_driver->context->term_buffer_length) < 0 ||
        aeron_logbuffer_check_term_length(_driver->context->ipc_term_buffer_length) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_page_size(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_context_validate_mtu_length(_driver->context->mtu_length) < 0 ||
        aeron_driver_context_validate_mtu_length(_driver->context->ipc_mtu_length) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->to_driver_buffer_length,
        AERON_TO_CONDUCTOR_BUFFER_LENGTH_DEFAULT,
        INT32_MAX,
        "to_driver_buffer_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->to_clients_buffer_length,
        AERON_TO_CLIENTS_BUFFER_LENGTH_DEFAULT,
        INT32_MAX,
        "to_clients_buffer_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->counters_values_buffer_length,
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_DEFAULT,
        AERON_COUNTERS_VALUES_BUFFER_LENGTH_MAX,
        "counters_values_buffer_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->error_buffer_length,
        AERON_ERROR_BUFFER_LENGTH_DEFAULT,
        INT32_MAX,
        "error_buffer_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->publication_window_length,
        0,
        AERON_LOGBUFFER_TERM_MAX_LENGTH,
        "publication_window_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_value_range(
        _driver->context->ipc_publication_window_length,
        0,
        AERON_LOGBUFFER_TERM_MAX_LENGTH,
        "ipc_publication_window_length") < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_sufficient_socket_buffer_lengths(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_ensure_dir_is_recreated(_driver->context) < 0)
    {
        AERON_APPEND_ERR("could not recreate aeron dir: %s", _driver->context->aeron_dir);
        goto error;
    }

    if (aeron_driver_create_cnc_file(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_create_loss_report_file(_driver) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_unblock_timeout(_driver->context) < 0)
    {
        goto error;
    }

    if (aeron_driver_validate_untethered_timeouts(_driver->context) < 0)
    {
        goto error;
    }

    context->counters_manager = &_driver->conductor.counters_manager;
    context->system_counters = &_driver->conductor.system_counters;
    context->error_log = &_driver->conductor.error_log;
    _driver->context->conductor_proxy = &_driver->conductor.conductor_proxy;

    if (aeron_driver_conductor_init(&_driver->conductor, context) < 0)
    {
        goto error;
    }

    if (aeron_driver_sender_init(
        &_driver->sender, context, &_driver->conductor.system_counters, &_driver->conductor.error_log) < 0)
    {
        goto error;
    }

    _driver->context->sender_proxy = &_driver->sender.sender_proxy;

    if (aeron_driver_receiver_init(
        &_driver->receiver, context, &_driver->conductor.system_counters, &_driver->conductor.error_log) < 0)
    {
        goto error;
    }

    _driver->context->receiver_proxy = &_driver->receiver.receiver_proxy;

    aeron_counter_set_release(
        aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_AERON_VERSION),
        aeron_semantic_version_compose(aeron_version_major(), aeron_version_minor(), aeron_version_patch()));

    aeron_counter_set_release(
        aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED),
        (int64_t)(_driver->context->cnc_map.length + _driver->context->loss_report_length));

    if (aeron_feedback_delay_state_init(
        &_driver->context->unicast_delay_feedback_generator,
        aeron_loss_detector_nak_unicast_delay_generator,
        (int64_t)_driver->context->nak_unicast_delay_ns,
        (int64_t)_driver->context->nak_unicast_delay_ns * (int64_t)_driver->context->nak_unicast_retry_delay_ratio,
        1) < 0)
    {
        goto error;
    }

    if (aeron_feedback_delay_state_init(
        &_driver->context->multicast_delay_feedback_generator,
        aeron_loss_detector_nak_multicast_delay_generator,
        (int64_t)_driver->context->nak_multicast_max_backoff_ns,
        (int64_t)_driver->context->nak_multicast_max_backoff_ns,
        _driver->context->nak_multicast_group_size) < 0)
    {
        goto error;
    }

    aeron_mpsc_rb_next_correlation_id(&_driver->conductor.to_driver_commands);
    aeron_mpsc_rb_consumer_heartbeat_time(&_driver->conductor.to_driver_commands, aeron_epoch_clock());
    aeron_cnc_version_signal_cnc_ready((aeron_cnc_metadata_t *)context->cnc_map.addr, AERON_CNC_VERSION);
    aeron_msync(context->cnc_map.addr, context->cnc_map.length);

    if (_driver->context->print_configuration_on_start)
    {
        aeron_driver_context_print_configuration(_driver->context);
    }

    switch (_driver->context->threading_mode)
    {
        case AERON_THREADING_MODE_INVOKER:
        case AERON_THREADING_MODE_SHARED:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SHARED],
                "[conductor, sender, receiver]",
                _driver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_shared_do_work,
                aeron_driver_shared_on_close,
                _driver->context->shared_idle_strategy_func,
                _driver->context->shared_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;

        case AERON_THREADING_MODE_SHARED_NETWORK:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_CONDUCTOR],
                "conductor",
                &_driver->conductor,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_conductor_do_work,
                aeron_driver_conductor_on_close,
                _driver->context->conductor_idle_strategy_func,
                _driver->context->conductor_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SHARED_NETWORK],
                "[sender, receiver]",
                _driver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_shared_network_do_work,
                aeron_driver_shared_network_on_close,
                _driver->context->shared_network_idle_strategy_func,
                _driver->context->shared_network_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;

        case AERON_THREADING_MODE_DEDICATED:
        default:
            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_CONDUCTOR],
                "conductor",
                &_driver->conductor,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_conductor_do_work,
                aeron_driver_conductor_on_close,
                _driver->context->conductor_idle_strategy_func,
                _driver->context->conductor_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_SENDER],
                "sender",
                &_driver->sender,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_sender_do_work,
                aeron_driver_sender_on_close,
                _driver->context->sender_idle_strategy_func,
                _driver->context->sender_idle_strategy_state) < 0)
            {
                goto error;
            }

            if (aeron_agent_init(
                &_driver->runners[AERON_AGENT_RUNNER_RECEIVER],
                "receiver",
                &_driver->receiver,
                _driver->context->agent_on_start_func,
                _driver->context->agent_on_start_state,
                aeron_driver_receiver_do_work,
                aeron_driver_receiver_on_close,
                _driver->context->receiver_idle_strategy_func,
                _driver->context->receiver_idle_strategy_state) < 0)
            {
                goto error;
            }
            break;
    }

    *driver = _driver;
    return 0;

error:
    if (NULL != _driver)
    {
        aeron_free(_driver);
    }

    return -1;
}

int aeron_driver_start(aeron_driver_t *driver, bool manual_main_loop)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return -1;
    }

    if (!manual_main_loop)
    {
        if (AERON_THREADING_MODE_INVOKER == driver->context->threading_mode)
        {
            AERON_SET_ERR(EINVAL, "%s", "INVOKER threading mode requires manual_main_loop");
            return -1;
        }

        if (aeron_agent_start(&driver->runners[0]) < 0)
        {
            return -1;
        }
    }
    else
    {
        aeron_agent_runner_t first_runner = driver->runners[0];
        if (NULL != first_runner.on_start)
        {
            first_runner.on_start(first_runner.on_start_state, first_runner.role_name);
        }

        first_runner.state = AERON_AGENT_STATE_MANUAL;
    }

    for (int i = 1; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (AERON_AGENT_STATE_INITED == driver->runners[i].state)
        {
            if (aeron_agent_start(&driver->runners[i]) < 0)
            {
                return -1;
            }
        }
    }

    return 0;
}

int aeron_driver_main_do_work(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return -1;
    }

    return aeron_agent_do_work(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR]);
}

void aeron_driver_main_idle_strategy(aeron_driver_t *driver, int work_count)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return;
    }

    aeron_agent_idle(&driver->runners[AERON_AGENT_RUNNER_CONDUCTOR], work_count);
}

int aeron_driver_close(aeron_driver_t *driver)
{
    if (NULL == driver)
    {
        AERON_SET_ERR(EINVAL, "%s", "driver is null");
        return -1;
    }

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (aeron_agent_stop(&driver->runners[i]) < 0)
        {
            return -1;
        }
    }

    for (int i = 0; i < AERON_AGENT_RUNNER_MAX; i++)
    {
        if (aeron_agent_close(&driver->runners[i]) < 0)
        {
            return -1;
        }
    }

    aeron_free(driver);

    return 0;
}
