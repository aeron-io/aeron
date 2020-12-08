/*
 * Copyright 2014-2020 Real Logic Limited.
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
#include <stdio.h>
#include <inttypes.h>

#ifndef _MSC_VER
#include <unistd.h>
#include <getopt.h>
#endif

#include "aeronc.h"
#include "aeron_common.h"
#include "aeron_cnc_file_descriptor.h"
#include "concurrent/aeron_thread.h"
#include "concurrent/aeron_mpsc_rb.h"


#include "util/aeron_strutil.h"
#include "util/aeron_error.h"


typedef struct aeron_driver_tool_settings_stct
{
    const char *directory;
    bool pid_only;
    bool terminate_driver;
    long long timeout_ms;
}
aeron_driver_tool_settings_t;

const char *usage()
{
    return
        "    -P            Print PID only without anything else.\n"
        "    -T            Request driver to terminate.\n"
        "    -d basePath   Base Path to shared memory. Default: /dev/shm/aeron-mike\n"
        "    -h            Displays help information.\n"
        "    -t timeout    Number of milliseconds to wait to see if the driver metadata is available.  Default 10,000\n";
}

void print_error_and_usage(const char *message)
{
    fprintf(stderr, "%s\n%s", message, usage());
}

int main(int argc, char **argv)
{
    char default_directory[AERON_MAX_PATH];
    aeron_default_path(default_directory, AERON_MAX_PATH);
    aeron_driver_tool_settings_t settings = {
        .directory = default_directory,
        .pid_only = false,
        .terminate_driver = false,
        .timeout_ms = 10 * 1000
    };

    int opt;

    while ((opt = getopt(argc, argv, "d:t:PTh")) != -1)
    {
        switch (opt)
        {
            case 'd':
                settings.directory = argv[optind];
                break;

            case 't':
            {
                aeron_set_errno(0);
                char *endptr;
                settings.timeout_ms = strtoll(optarg, &endptr, 10);
                if (0 != errno || '\0' != endptr[0])
                {
                    print_error_and_usage("Invalid timeout");
                    return EXIT_FAILURE;
                }
                break;
            }

            case 'P':
                settings.pid_only = true;
                break;

            case 'T':
                settings.terminate_driver = true;
                break;

            case 'h':
                print_error_and_usage(argv[0]);
                return EXIT_SUCCESS;

            default:
                print_error_and_usage("Unknown option");
                return EXIT_FAILURE;
        }
    }

    aeron_cnc_metadata_t *cnc_metadata;
    aeron_mapped_file_t cnc_file = { 0 };
    const int64_t deadline_ms = aeron_epoch_clock() + settings.timeout_ms;

    do
    {
        aeron_cnc_load_result_t result = aeron_cnc_map_file_and_load_metadata(
            settings.directory, &cnc_file, &cnc_metadata);

        if (AERON_CNC_LOAD_SUCCESS == result)
        {
            break;
        }
        else if (AERON_CNC_LOAD_FAILED == result)
        {
            print_error_and_usage(aeron_errmsg());
            return EXIT_FAILURE;
        }
        else
        {
            aeron_micro_sleep(16 * 1000);
        }

        if (deadline_ms <= aeron_epoch_clock())
        {
            print_error_and_usage("Timed out trying to get driver's CnC metadata");
            return EXIT_FAILURE;
        }
    }
    while (true);

    if (settings.pid_only)
    {
        printf("%" PRId64 "\n", cnc_metadata->pid);
    }
    else if (settings.terminate_driver)
    {
        aeron_context_request_driver_termination(settings.directory, NULL, 0);
    }
    else
    {
        char cnc_filename[AERON_MAX_PATH];
        char now_timestamp_buffer[AERON_MAX_PATH];
        char start_timestamp_buffer[AERON_MAX_PATH];
        char heartbeat_timestamp_buffer[AERON_MAX_PATH];

        aeron_cnc_filename(settings.directory, cnc_filename, AERON_MAX_PATH);

        int64_t now_ms = aeron_epoch_clock();

        uint8_t *to_driver_buffer = aeron_cnc_to_driver_buffer(cnc_metadata);
        aeron_mpsc_rb_t to_driver_rb = { 0 };
        aeron_mpsc_rb_init(&to_driver_rb, to_driver_buffer, cnc_metadata->to_driver_buffer_length);
        const int64_t heartbeat_ms = aeron_mpsc_rb_consumer_heartbeat_time_value(&to_driver_rb);

        aeron_format_date(now_timestamp_buffer, sizeof(now_timestamp_buffer), now_ms);
        aeron_format_date(start_timestamp_buffer, sizeof(start_timestamp_buffer), cnc_metadata->start_timestamp);
        aeron_format_date(heartbeat_timestamp_buffer, sizeof(heartbeat_timestamp_buffer), heartbeat_ms);

        fprintf(stdout, "Command 'n Control cnc_file: %s\n", cnc_filename);
        fprintf(stdout, "Version: %" PRId32 ", PID: %" PRId64 "\n", cnc_metadata->cnc_version, cnc_metadata->pid);
        fprintf(
            stdout,
            "%s (start: %s, activity: %s)\n",
            now_timestamp_buffer,
            start_timestamp_buffer,
            heartbeat_timestamp_buffer);
    }

    aeron_unmap(&cnc_file);

    return EXIT_SUCCESS;
}
