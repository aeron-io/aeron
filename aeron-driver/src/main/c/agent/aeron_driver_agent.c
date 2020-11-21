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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#if !defined(_MSC_VER)

#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#endif

#include <stdio.h>
#include <time.h>
#include <inttypes.h>

#include "agent/aeron_driver_agent.h"
#include "aeron_driver_context.h"
#include "util/aeron_dlopen.h"
#include "aeron_alloc.h"
#include "util/aeron_arrayutil.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

static AERON_INIT_ONCE agent_is_initialized = AERON_INIT_ONCE_VALUE;
static aeron_mpsc_rb_t logging_mpsc_rb;
static uint8_t *rb_buffer = NULL;
static FILE *logfp = NULL;
static aeron_thread_t log_reader_thread;
static bool enabled_events[AERON_DRIVER_EVENT_NUM_ELEMENTS];

static const char EVENT_NAMES[AERON_DRIVER_EVENT_NUM_ELEMENTS][64] =
{
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    "FRAME_IN",
    "FRAME_OUT",
    "CMD_IN_ADD_PUBLICATION",
    "CMD_IN_REMOVE_PUBLICATION",
    "CMD_IN_ADD_SUBSCRIPTION",
    "CMD_IN_REMOVE_SUBSCRIPTION",
    "CMD_OUT_PUBLICATION_READY",
    "CMD_OUT_AVAILABLE_IMAGE",
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    "CMD_OUT_ON_OPERATION_SUCCESS",
    "CMD_IN_KEEPALIVE_CLIENT",
    "REMOVE_PUBLICATION_CLEANUP",
    "REMOVE_SUBSCRIPTION_CLEANUP",
    "REMOVE_IMAGE_CLEANUP",
    "CMD_OUT_ON_UNAVAILABLE_IMAGE",
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    "SEND_CHANNEL_CREATION",
    "RECEIVE_CHANNEL_CREATION",
    "SEND_CHANNEL_CLOSE",
    "RECEIVE_CHANNEL_CLOSE",
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,
    "CMD_IN_ADD_DESTINATION",
    "CMD_IN_REMOVE_DESTINATION",
    "CMD_IN_ADD_EXCLUSIVE_PUBLICATION",
    "CMD_OUT_EXCLUSIVE_PUBLICATION_READY",
    "CMD_OUT_ERROR",
    "CMD_IN_ADD_COUNTER",
    "CMD_IN_REMOVE_COUNTER",
    "CMD_OUT_SUBSCRIPTION_READY",
    "CMD_OUT_COUNTER_READY",
    "CMD_OUT_ON_UNAVAILABLE_COUNTER",
    "CMD_IN_CLIENT_CLOSE",
    "CMD_IN_ADD_RCV_DESTINATION",
    "CMD_IN_REMOVE_RCV_DESTINATION",
    "CMD_OUT_ON_CLIENT_TIMEOUT",
    "CMD_IN_TERMINATE_DRIVER",
    "UNTETHERED_SUBSCRIPTION_STATE_CHANGE",
    "NAME_RESOLUTION_NEIGHBOR_ADDED",
    "NAME_RESOLUTION_NEIGHBOR_REMOVED"
};

static const bool CMD_IN_EVENTS[AERON_DRIVER_EVENT_NUM_ELEMENTS] =
{
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION
    true, // AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION
    true, // AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION
    false,
    false,
    false,
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_DESTINATION
    true, // AERON_DRIVER_EVENT_CMD_IN_REMOVE_DESTINATION
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_EXCLUSIVE_PUBLICATION
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER
    true, // AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE
    true, // AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION
    true, // AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION
    false,
    true, // AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER
    false,
    false,
    false
};

static const bool CMD_OUT_EVENTS[AERON_DRIVER_EVENT_NUM_ELEMENTS] =
{
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY
    true, // AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS
    false,
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY
    true, // AERON_DRIVER_EVENT_CMD_OUT_ERROR
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY
    true, // AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY
    true, // AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER
    false,
    false,
    false,
    true, // AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT
    false,
    false,
    false,
    false
};

aeron_mpsc_rb_t *aeron_driver_agent_mpsc_rb()
{
    return &logging_mpsc_rb;
}

void aeron_driver_agent_format_date(char *str, size_t count, int64_t timestamp)
{
    char time_buffer[80];
    char msec_buffer[8];
    char tz_buffer[8];
    struct tm time;
    time_t just_seconds = timestamp / 1000;
    int64_t msec_after_sec = timestamp % 1000;

    localtime_r(&just_seconds, &time);

    strftime(time_buffer, sizeof(time_buffer) - 1, "%Y-%m-%d %H:%M:%S.", &time);
    snprintf(msec_buffer, sizeof(msec_buffer) - 1, "%03" PRId64, msec_after_sec);
    strftime(tz_buffer, sizeof(tz_buffer) - 1, "%z", &time);

    snprintf(str, count, "%s%s%s", time_buffer, msec_buffer, tz_buffer);
}

static void *aeron_driver_agent_log_reader(void *arg)
{
    while (true)
    {
        size_t messages_read = aeron_mpsc_rb_read(&logging_mpsc_rb, aeron_driver_agent_log_dissector, NULL, 10);
        if (0 == messages_read)
        {
            aeron_nano_sleep(1000 * 1000);
        }
    }

    return NULL;
}

void aeron_driver_agent_logging_ring_buffer_init()
{
    size_t rb_length = RING_BUFFER_LENGTH + AERON_RB_TRAILER_LENGTH;

    if ((rb_buffer = (uint8_t *)malloc(rb_length)) == NULL)
    {
        fprintf(stderr, "could not allocate ring buffer buffer. exiting.\n");
        exit(EXIT_FAILURE);
    }
    memset(rb_buffer, 0, rb_length);

    if (aeron_mpsc_rb_init(&logging_mpsc_rb, rb_buffer, rb_length) < 0)
    {
        fprintf(stderr, "could not init logging mpwc_rb. exiting.\n");
        exit(EXIT_FAILURE);
    }
}

void aeron_driver_agent_logging_ring_buffer_free()
{
    if (NULL != rb_buffer)
    {
        aeron_free(rb_buffer);
        rb_buffer = NULL;
    }
}

static bool aeron_driver_agent_is_unknown_event(const char *event_name)
{
    return 0 == strncmp(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME, event_name, strlen(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME) + 1);
}

static aeron_driver_agent_event_t aeron_driver_agent_event_name_to_id(const char *event_name)
{
    if (aeron_driver_agent_is_unknown_event(event_name))
    {
        return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
    }

    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        const char *name = EVENT_NAMES[i];
        if (0 == strncmp(name, event_name, strlen(name) + 1))
        {
            return i;
        }
    }

    return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
}

static inline bool is_valid_event_id(const int id)
{
    return id >= 0 && id < AERON_DRIVER_EVENT_NUM_ELEMENTS;
}

const char *aeron_driver_agent_event_name(const aeron_driver_agent_event_t id)
{
    if (is_valid_event_id(id))
    {
        return EVENT_NAMES[id];
    }
    return AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME;
}

bool aeron_driver_agent_is_event_enabled(const aeron_driver_agent_event_t id)
{
    return is_valid_event_id(id) && enabled_events[id];
}

static void enable_all_events()
{
    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        const char *event_name = EVENT_NAMES[i];
        if (!aeron_driver_agent_is_unknown_event(event_name))
        {
            enabled_events[i] = true;
        }
    }
}

static void enable_admin_events()
{
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION] = true;
    enabled_events[AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP] = true;
    enabled_events[AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP] = true;
    enabled_events[AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_ERROR] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT] = true;
    enabled_events[AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER] = true;
    enabled_events[AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION] = true;
    enabled_events[AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION] = true;
    enabled_events[AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE] = true;
    enabled_events[AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE] = true;
}

static void enable_specific_events(const bool *events)
{
    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        if (events[i])
        {
            enabled_events[i] = true;
        }
    }
}

static bool any_event_enabled(const bool *events)
{
    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        if (events[i] && enabled_events[i])
        {
            return true;
        }
    }

    return false;
}

static aeron_driver_agent_event_t parse_event_name(const char *event_name) {
    aeron_driver_agent_event_t event_id = aeron_driver_agent_event_name_to_id(event_name);
    if (AERON_DRIVER_EVENT_UNKNOWN_EVENT == event_id)
    {
        const long id = strtol(event_name, NULL, 0);
        if (is_valid_event_id((int)id) &&
            !aeron_driver_agent_is_unknown_event(aeron_driver_agent_event_name((aeron_driver_agent_event_t)id)))
        {
            event_id = (aeron_driver_agent_event_t)id;
        }
    }

    return event_id;
}

bool aeron_driver_agent_logging_events_init(const char *event_log)
{
    aeron_driver_agent_logging_events_free();
    if (event_log)
    {
        const size_t event_log_length = strlen(event_log);
        char *event_log_dup = NULL;
        if (aeron_alloc((void **)&event_log_dup, event_log_length + 1) < 0)
        {
            fprintf(stderr, "failed to copy logging events\n");
            return false;
        }
        strncpy(event_log_dup, event_log, event_log_length);
        event_log_dup[event_log_length] = '\0';

        char *events[AERON_DRIVER_EVENT_NUM_ELEMENTS];
        const int num_events =
            aeron_tokenise(event_log_dup, ',', AERON_DRIVER_EVENT_NUM_ELEMENTS, events);

        aeron_free(event_log_dup);

        if (num_events < 0)
        {
            fprintf(stderr, "failed to parse logging events: '%s'\n", event_log);
            return false;
        }

        bool result = false;
        for (int i = num_events - 1; i >= 0; i--)
        {
            const char *event_name = events[i];
            if (0 == strncmp(AERON_DRIVER_AGENT_ALL_EVENTS, event_name, strlen(AERON_DRIVER_AGENT_ALL_EVENTS) + 1))
            {
                enable_all_events();
                result = true;
                break;
            }
            else if (0 == strncmp(AERON_DRIVER_AGENT_ADMIN_EVENTS, event_name, strlen(AERON_DRIVER_AGENT_ADMIN_EVENTS) + 1))
            {
                enable_admin_events();
                result = true;
            }
            else if (0 == strncmp("0x", event_name, 2))
            {
                const uint64_t mask = strtoull(event_name, NULL, 0);
                if (0 != mask)
                {
                    if (mask >= 0xFFFF)
                    {
                        enable_all_events();
                        result = true;
                    }
                    else
                    {
                        if (mask & 0x1u)
                        {
                            enable_specific_events(CMD_IN_EVENTS);
                            result = true;
                        }

                        if (mask & 0x2u)
                        {
                            enable_specific_events(CMD_OUT_EVENTS);
                            result = true;
                        }

                        if (mask & 0x4u)
                        {
                            enabled_events[AERON_DRIVER_EVENT_FRAME_IN] = true;
                            result = true;
                        }

                        if (mask & 0x8u || mask & 0x10u)
                        {
                            enabled_events[AERON_DRIVER_EVENT_FRAME_OUT] = true;
                            result = true;
                        }

                        if (mask & 0x80u)
                        {
                            enabled_events[AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE] = true;
                            result = true;
                        }
                    }
                }
                break;
            }
            else
            {
                const aeron_driver_agent_event_t event_id = parse_event_name(event_name);
                if (AERON_DRIVER_EVENT_UNKNOWN_EVENT != event_id)
                {
                    enabled_events[event_id] = true;
                    result = true;
                }
                else
                {
                    fprintf(stderr, "unknown event code: '%s'\n", event_name);
                }
            }
        }
        return result;
    }

    return false;
}

void aeron_driver_agent_logging_events_free()
{
    for (int i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        enabled_events[i] = false;
    }
}

static void initialize_agent_logging()
{
    const char *event_log_str = getenv(AERON_EVENT_LOG_ENV_VAR);
    if (aeron_driver_agent_logging_events_init(event_log_str))
    {
        logfp = stdout;
        const char *log_filename = getenv(AERON_EVENT_LOG_FILENAME_ENV_VAR);
        if (log_filename)
        {
            if ((logfp = fopen(log_filename, "a")) == NULL)
            {
                int errcode = errno;

                fprintf(stderr, "could not fopen log file %s (%d, %s). exiting.\n",
                    log_filename, errcode, strerror(errcode));
                exit(EXIT_FAILURE);
            }
        }

        aeron_driver_agent_logging_ring_buffer_init();

        if (aeron_thread_create(&log_reader_thread, NULL, aeron_driver_agent_log_reader, NULL) != 0)
        {
            fprintf(stderr, "could not start log reader thread. exiting.\n");
            exit(EXIT_FAILURE);
        }

        fprintf(logfp, "%s\n", aeron_driver_agent_dissect_log_start(aeron_nano_clock(), aeron_epoch_clock()));
    }
}

static aeron_driver_agent_event_t command_id_to_driver_event_id(const int32_t msg_type_id)
{
    switch (msg_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_PUBLICATION;

        case AERON_COMMAND_REMOVE_PUBLICATION:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_PUBLICATION;

        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_EXCLUSIVE_PUBLICATION;

        case AERON_COMMAND_ADD_SUBSCRIPTION:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_SUBSCRIPTION;

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_SUBSCRIPTION;

        case AERON_COMMAND_CLIENT_KEEPALIVE:
            return AERON_DRIVER_EVENT_CMD_IN_KEEPALIVE_CLIENT;

        case AERON_COMMAND_ADD_DESTINATION:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_DESTINATION;

        case AERON_COMMAND_REMOVE_DESTINATION:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_DESTINATION;

        case AERON_COMMAND_ADD_COUNTER:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_COUNTER;

        case AERON_COMMAND_REMOVE_COUNTER:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_COUNTER;

        case AERON_COMMAND_CLIENT_CLOSE:
            return AERON_DRIVER_EVENT_CMD_IN_CLIENT_CLOSE;

        case AERON_COMMAND_ADD_RCV_DESTINATION:
            return AERON_DRIVER_EVENT_CMD_IN_ADD_RCV_DESTINATION;

        case AERON_COMMAND_REMOVE_RCV_DESTINATION:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_RCV_DESTINATION;

        case AERON_COMMAND_TERMINATE_DRIVER:
            return AERON_DRIVER_EVENT_CMD_IN_TERMINATE_DRIVER;

        case AERON_RESPONSE_ON_ERROR:
            return AERON_DRIVER_EVENT_CMD_OUT_ERROR;

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
            return AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE;

        case AERON_RESPONSE_ON_PUBLICATION_READY:
            return AERON_DRIVER_EVENT_CMD_OUT_PUBLICATION_READY;

        case AERON_RESPONSE_ON_OPERATION_SUCCESS:
            return AERON_DRIVER_EVENT_CMD_OUT_ON_OPERATION_SUCCESS;

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
            return AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_IMAGE;

        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
            return AERON_DRIVER_EVENT_CMD_OUT_EXCLUSIVE_PUBLICATION_READY;

        case AERON_RESPONSE_ON_SUBSCRIPTION_READY:
            return AERON_DRIVER_EVENT_CMD_OUT_SUBSCRIPTION_READY;

        case AERON_RESPONSE_ON_COUNTER_READY:
            return AERON_DRIVER_EVENT_CMD_OUT_COUNTER_READY;

        case AERON_RESPONSE_ON_UNAVAILABLE_COUNTER:
            return AERON_DRIVER_EVENT_CMD_OUT_ON_UNAVAILABLE_COUNTER;

        case AERON_RESPONSE_ON_CLIENT_TIMEOUT:
            return AERON_DRIVER_EVENT_CMD_OUT_ON_CLIENT_TIMEOUT;

        default:
            return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
    }
}

void log_conductor_to_driver_command(
    const aeron_driver_agent_event_t event_id,
    const int32_t msg_type_id,
    const void *message,
    const size_t length,
    const size_t command_length,
    char *buffer)
{
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ns = aeron_nano_clock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, event_id, buffer, command_length);
}

void aeron_driver_agent_conductor_to_driver_interceptor(
    int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    const aeron_driver_agent_event_t event_id = command_id_to_driver_event_id(msg_type_id);
    if (!aeron_driver_agent_is_event_enabled(event_id))
    {
        return;
    }

    const size_t command_length = sizeof(aeron_driver_agent_cmd_log_header_t) + length;

    if (command_length > sizeof(aeron_driver_agent_cmd_log_header_t) + MAX_CMD_LENGTH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, command_length) < 0)
        {
            return;
        }
        log_conductor_to_driver_command(event_id, msg_type_id, message, length, command_length, buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_driver_agent_cmd_log_header_t) + MAX_CMD_LENGTH];
        log_conductor_to_driver_command(event_id, msg_type_id, message, length, command_length, buffer);
    }
}

void log_conductor_to_client_command(
    const aeron_driver_agent_event_t event_id,
    const int32_t msg_type_id,
    const void *message,
    const size_t length,
    const size_t command_length,
    char *buffer)
{
    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)buffer;
    hdr->time_ns = aeron_nano_clock();
    hdr->cmd_id = msg_type_id;
    memcpy(buffer + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, event_id, buffer, command_length);
}

void aeron_driver_agent_conductor_to_client_interceptor(
    aeron_driver_conductor_t *conductor, int32_t msg_type_id, const void *message, size_t length)
{
    const aeron_driver_agent_event_t event_id = command_id_to_driver_event_id(msg_type_id);
    if (!aeron_driver_agent_is_event_enabled(event_id))
    {
        return;
    }

    const size_t command_length = sizeof(aeron_driver_agent_cmd_log_header_t) + length;

    if (command_length > sizeof(aeron_driver_agent_cmd_log_header_t) + MAX_CMD_LENGTH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, command_length) < 0)
        {
            return;
        }
        log_conductor_to_client_command(event_id, msg_type_id, message, length, command_length, buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_driver_agent_cmd_log_header_t) + MAX_CMD_LENGTH];
        log_conductor_to_client_command(event_id, msg_type_id, message, length, command_length, buffer);
    }
}

void aeron_driver_agent_log_frame(
    int32_t msg_type_id, const struct msghdr *msghdr, int result, int32_t message_len)
{
    uint8_t buffer[MAX_FRAME_LENGTH + sizeof(aeron_driver_agent_frame_log_header_t) + sizeof(struct sockaddr_storage)];
    aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)buffer;
    size_t length = sizeof(aeron_driver_agent_frame_log_header_t);

    hdr->time_ns = aeron_nano_clock();
    hdr->result = (int32_t)result;
    hdr->sockaddr_len = msghdr->msg_namelen;
    hdr->message_len = message_len;

    if (msghdr->msg_iovlen > 1)
    {
        fprintf(stderr, "only aware of 1 iov. %d iovs detected.\n", (int)msghdr->msg_iovlen);
    }

    uint8_t *ptr = buffer + sizeof(aeron_driver_agent_frame_log_header_t);
    memcpy(ptr, msghdr->msg_name, msghdr->msg_namelen);
    length += msghdr->msg_namelen;

    ptr += msghdr->msg_namelen;
    int32_t copy_length = message_len < MAX_FRAME_LENGTH ? message_len : MAX_FRAME_LENGTH;
    memcpy(ptr, msghdr->msg_iov[0].iov_base, (size_t)copy_length);
    length += copy_length;

    aeron_mpsc_rb_write(&logging_mpsc_rb, msg_type_id, buffer, (size_t)length);
}

int aeron_driver_agent_outgoing_mmsg(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct mmsghdr *msgvec,
    size_t vlen)
{
    int result = delegate->outgoing_mmsg_func(
        delegate->interceptor_state, delegate->next_interceptor, transport, msgvec, vlen);

    for (int i = 0; i < result; i++)
    {
        aeron_driver_agent_log_frame(
            AERON_DRIVER_EVENT_FRAME_OUT,
            &msgvec[i].msg_hdr,
            (int32_t)msgvec[i].msg_len,
            (int32_t)msgvec[i].msg_hdr.msg_iov[0].iov_len);
    }

    return result;
}

int aeron_driver_agent_outgoing_msg(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct msghdr *message)
{
    int result = delegate->outgoing_msg_func(
        delegate->interceptor_state, delegate->next_interceptor, transport, message);

    aeron_driver_agent_log_frame(AERON_DRIVER_EVENT_FRAME_OUT, message, result, (int32_t)message->msg_iov[0].iov_len);

    return result;
}

void aeron_driver_agent_incoming_msg(
    void *interceptor_state,
    aeron_udp_channel_incoming_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    void *receiver_clientd,
    void *endpoint_clientd,
    void *destination_clientd,
    uint8_t *buffer,
    size_t length,
    struct sockaddr_storage *addr)
{
    struct msghdr message;
    struct iovec iov;

    iov.iov_base = buffer;
    iov.iov_len = (uint32_t)length;
    message.msg_iovlen = 1;
    message.msg_iov = &iov;
    message.msg_name = addr;
    message.msg_control = NULL;
    message.msg_controllen = 0;
    message.msg_namelen = sizeof(struct sockaddr_storage);

    aeron_driver_agent_log_frame(AERON_DRIVER_EVENT_FRAME_IN, &message, (int32_t)length, (int32_t)length);

    delegate->incoming_func(
        delegate->interceptor_state,
        delegate->next_interceptor,
        transport,
        receiver_clientd,
        endpoint_clientd,
        destination_clientd,
        buffer,
        length,
        addr);
}

void aeron_untethered_subscription_state_change(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id)
{
    tetherable_position->state = new_state;
    tetherable_position->time_of_last_update_ns = now_ns;
}

void aeron_driver_agent_untethered_subscription_state_change_interceptor(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id)
{
    uint8_t buffer[sizeof(aeron_driver_agent_untethered_subscription_state_change_log_header_t)];
    aeron_driver_agent_untethered_subscription_state_change_log_header_t *hdr =
        (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)buffer;

    hdr->time_ns = aeron_nano_clock();
    hdr->subscription_id = tetherable_position->subscription_registration_id;
    hdr->stream_id = stream_id;
    hdr->session_id = session_id;
    hdr->old_state = tetherable_position->state;
    hdr->new_state = new_state;

    aeron_untethered_subscription_state_change(tetherable_position, now_ns, new_state, stream_id, session_id);

    aeron_mpsc_rb_write(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE,
        buffer,
        sizeof(aeron_driver_agent_untethered_subscription_state_change_log_header_t));
}

void log_name_resolution_neighbor_change(const aeron_driver_agent_event_t id, const struct sockaddr_storage *addr)
{
    uint8_t buffer[sizeof(aeron_driver_agent_log_header_t) + sizeof(struct sockaddr_storage)];
    aeron_driver_agent_log_header_t *hdr = (aeron_driver_agent_log_header_t *)buffer;

    hdr->time_ns = aeron_nano_clock();

    uint8_t *ptr = buffer + sizeof(aeron_driver_agent_log_header_t);
    memcpy(ptr, addr, sizeof(struct sockaddr_storage));

    aeron_mpsc_rb_write(
        &logging_mpsc_rb, id, buffer, sizeof(aeron_driver_agent_log_header_t) + sizeof(struct sockaddr_storage));
}

void aeron_driver_agent_name_resolution_on_neighbor_added(const struct sockaddr_storage *addr)
{
    log_name_resolution_neighbor_change(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED, addr);
}

void aeron_driver_agent_name_resolution_on_neighbor_removed(const struct sockaddr_storage *addr)
{
    log_name_resolution_neighbor_change(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED, addr);
}

int aeron_driver_agent_interceptor_init(
    void **interceptor_state, aeron_driver_context_t *context, aeron_udp_channel_transport_affinity_t affinity)
{
    return 0;
}

int aeron_driver_agent_init_logging_events_interceptors(aeron_driver_context_t *context)
{
    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN))
    {
        aeron_udp_channel_interceptor_bindings_t *incoming_bindings = NULL;

        if (aeron_alloc((void **)&incoming_bindings, sizeof(aeron_udp_channel_interceptor_bindings_t)) < 0)
        {
            aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
            return -1;
        }

        incoming_bindings->outgoing_init_func = NULL;
        incoming_bindings->outgoing_close_func = NULL;
        incoming_bindings->outgoing_mmsg_func = NULL;
        incoming_bindings->outgoing_msg_func = NULL;
        incoming_bindings->incoming_init_func = aeron_driver_agent_interceptor_init;
        incoming_bindings->incoming_close_func = NULL;
        incoming_bindings->incoming_func = aeron_driver_agent_incoming_msg;
        incoming_bindings->outgoing_transport_notification_func = NULL;
        incoming_bindings->outgoing_publication_notification_func = NULL;
        incoming_bindings->outgoing_image_notification_func = NULL;
        incoming_bindings->incoming_transport_notification_func = NULL;
        incoming_bindings->incoming_publication_notification_func = NULL;
        incoming_bindings->incoming_image_notification_func = NULL;

        incoming_bindings->meta_info.name = "logging";
        incoming_bindings->meta_info.type = "interceptor";
        incoming_bindings->meta_info.source_symbol = "aeron_driver_agent_context_init";
        incoming_bindings->meta_info.next_interceptor_bindings = NULL;

        if (NULL == context->udp_channel_incoming_interceptor_bindings)
        {
            context->udp_channel_incoming_interceptor_bindings = incoming_bindings;
        }
        else
        {
            aeron_udp_channel_interceptor_bindings_t *iter = context->udp_channel_incoming_interceptor_bindings;
            while (NULL != iter->meta_info.next_interceptor_bindings)
            {
                iter = (aeron_udp_channel_interceptor_bindings_t *)iter->meta_info.next_interceptor_bindings;
            }

            iter->meta_info.next_interceptor_bindings = incoming_bindings;
        }
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT))
    {
        aeron_udp_channel_interceptor_bindings_t *outgoing_bindings = NULL;

        if (aeron_alloc((void **)&outgoing_bindings, sizeof(aeron_udp_channel_interceptor_bindings_t)) < 0)
        {
            aeron_set_err(ENOMEM, "could not allocate %s:%d", __FILE__, __LINE__);
            return -1;
        }

        outgoing_bindings->outgoing_init_func = aeron_driver_agent_interceptor_init;
        outgoing_bindings->outgoing_close_func = NULL;
        outgoing_bindings->outgoing_mmsg_func = aeron_driver_agent_outgoing_mmsg;
        outgoing_bindings->outgoing_msg_func = aeron_driver_agent_outgoing_msg;
        outgoing_bindings->incoming_init_func = NULL;
        outgoing_bindings->incoming_close_func = NULL;
        outgoing_bindings->incoming_func = NULL;
        outgoing_bindings->outgoing_transport_notification_func = NULL;
        outgoing_bindings->outgoing_publication_notification_func = NULL;
        outgoing_bindings->outgoing_image_notification_func = NULL;
        outgoing_bindings->incoming_transport_notification_func = NULL;
        outgoing_bindings->incoming_publication_notification_func = NULL;
        outgoing_bindings->incoming_image_notification_func = NULL;

        outgoing_bindings->meta_info.name = "logging";
        outgoing_bindings->meta_info.type = "interceptor";
        outgoing_bindings->meta_info.source_symbol = "aeron_driver_agent_context_init";
        outgoing_bindings->meta_info.next_interceptor_bindings = context->udp_channel_outgoing_interceptor_bindings;

        context->udp_channel_outgoing_interceptor_bindings = outgoing_bindings;
    }

    if (any_event_enabled(CMD_IN_EVENTS))
    {
        context->to_driver_interceptor_func = aeron_driver_agent_conductor_to_driver_interceptor;
    }

    if (any_event_enabled(CMD_OUT_EVENTS))
    {
        context->to_client_interceptor_func = aeron_driver_agent_conductor_to_client_interceptor;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP))
    {
        context->remove_publication_cleanup_func = aeron_driver_agent_remove_publication_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP))
    {
        context->remove_subscription_cleanup_func = aeron_driver_agent_remove_subscription_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP))
    {
        context->remove_image_cleanup_func = aeron_driver_agent_remove_image_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION))
    {
        context->sender_proxy_on_add_endpoint_func = aeron_driver_agent_sender_proxy_on_add_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE))
    {
        context->sender_proxy_on_remove_endpoint_func = aeron_driver_agent_sender_proxy_on_remove_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION))
    {
        context->receiver_proxy_on_add_endpoint_func = aeron_driver_agent_receiver_proxy_on_add_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE))
    {
        context->receiver_proxy_on_remove_endpoint_func = aeron_driver_agent_receiver_proxy_on_remove_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE))
    {
        context->untethered_subscription_state_change_func =
            aeron_driver_agent_untethered_subscription_state_change_interceptor;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED))
    {
        context->name_resolution_on_neighbor_added_func = aeron_driver_agent_name_resolution_on_neighbor_added;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED))
    {
        context->name_resolution_on_neighbor_removed_func = aeron_driver_agent_name_resolution_on_neighbor_removed;
    }

    return 0;
}

int aeron_driver_agent_context_init(aeron_driver_context_t *context)
{
    (void)aeron_thread_once(&agent_is_initialized, initialize_agent_logging);

    return aeron_driver_agent_init_logging_events_interceptors(context);
}

#define NANOS_PER_SECOND (1000000000)

const char *aeron_driver_agent_dissect_log_header(
    const int64_t time_ns,
    const aeron_driver_agent_event_t event_id,
    const size_t capture_length,
    const size_t message_length)
{
    static char buffer[150];

    snprintf(
        buffer,
        sizeof(buffer) - 1,
        "[%f] %s: %s [%zu/%zu]",
        (double)time_ns / NANOS_PER_SECOND,
        AERON_DRIVER_AGENT_LOG_CONTEXT,
        aeron_driver_agent_event_name(event_id),
        capture_length,
        message_length);
    return buffer;
}

const char *aeron_driver_agent_dissect_log_start(const int64_t time_ns, const int64_t time_ms)
{
    static char buffer[384];
    char datestamp[256];

    aeron_driver_agent_format_date(datestamp, sizeof(datestamp) - 1, time_ms);
    snprintf(
        buffer, sizeof(buffer) - 1, "[%f] log started %s", (double)time_ns / NANOS_PER_SECOND, datestamp);
    return buffer;
}

static const char *dissect_command_type_id(int64_t cmd_type_id)
{
    switch (cmd_type_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
            return "ADD_PUBLICATION";

        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
            return "ADD_EXCLUSIVE_PUBLICATION";

        case AERON_COMMAND_REMOVE_PUBLICATION:
            return "REMOVE_PUBLICATION";

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
            return "REMOVE_SUBSCRIPTION";

        case AERON_COMMAND_REMOVE_COUNTER:
            return "REMOVE_COUNTER";

        case AERON_COMMAND_ADD_DESTINATION:
            return "ADD_DESTINATION";

        case AERON_COMMAND_REMOVE_DESTINATION:
            return "REMOVE_DESTINATION";

        case AERON_COMMAND_TERMINATE_DRIVER:
            return "TERMINATE_DRIVER";

        default:
            return "unknown command";
    }
}

static const char *dissect_cmd_in(int64_t cmd_id, const void *message, size_t length)
{
    static char buffer[4096];

    buffer[0] = '\0';
    switch (cmd_id)
    {
        case AERON_COMMAND_ADD_PUBLICATION:
        case AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION:
        {
            aeron_publication_command_t *command = (aeron_publication_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_publication_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %d %*s [%" PRId64 ":%" PRId64 "]",
                dissect_command_type_id(cmd_id),
                command->stream_id,
                command->channel_length,
                channel,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        case AERON_COMMAND_REMOVE_COUNTER:
        {
            aeron_remove_command_t *command = (aeron_remove_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s %" PRId64 " [%" PRId64 ":%" PRId64 "]",
                dissect_command_type_id(cmd_id),
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);

            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_subscription_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "ADD_SUBSCRIPTION %d %*s [%" PRId64 "][%" PRId64 ":%" PRId64 "]",
                command->stream_id,
                command->channel_length,
                channel,
                command->registration_correlation_id,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "CLIENT_KEEPALIVE [%" PRId64 ":%" PRId64 "]",
                command->client_id,
                command->correlation_id);
            break;
        }

        case AERON_COMMAND_ADD_DESTINATION:
        case AERON_COMMAND_REMOVE_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_destination_command_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %*s %" PRId64 " [%" PRId64 ":%" PRId64 "]",
                dissect_command_type_id(cmd_id),
                command->channel_length,
                channel,
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_ADD_COUNTER:
        {
            aeron_counter_command_t *command = (aeron_counter_command_t *)message;

            const uint8_t *cursor = (const uint8_t *)message + sizeof(aeron_counter_command_t);

            int32_t key_length;
            memcpy(&key_length, cursor, sizeof(key_length));

            const uint8_t *key = cursor + sizeof(int32_t);
            cursor = key + AERON_ALIGN(key_length, sizeof(int32_t));

            int32_t label_length;
            memcpy(&label_length, cursor, sizeof(label_length));

            snprintf(buffer, sizeof(buffer) - 1, "ADD_COUNTER %d [%d %d][%d %d][%" PRId64 ":%" PRId64 "]",
                command->type_id,
                (int)(sizeof(aeron_counter_command_t) + sizeof(int32_t)),
                key_length,
                (int)(sizeof(aeron_counter_command_t) + (2 * sizeof(int32_t)) + AERON_ALIGN(key_length, sizeof(int32_t))),
                label_length,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_CLIENT_CLOSE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "CLIENT_CLOSE [%" PRId64 ":%" PRId64 "]",
                command->client_id,
                command->correlation_id);
            break;
        }

        case AERON_COMMAND_TERMINATE_DRIVER:
        {
            aeron_terminate_driver_command_t *command = (aeron_terminate_driver_command_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s %" PRId64 " %d",
                dissect_command_type_id(cmd_id),
                command->correlated.client_id,
                command->token_length);
            break;
        }

        default:
            break;
    }

    return buffer;
}

static const char *dissect_cmd_out(int64_t cmd_id, const void *message, size_t length)
{
    static char buffer[4096];

    buffer[0] = '\0';
    switch (cmd_id)
    {
        case AERON_RESPONSE_ON_OPERATION_SUCCESS:
        {
            aeron_operation_succeeded_t *command = (aeron_operation_succeeded_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_OPERATION_SUCCEEDED %" PRId64, command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *command = (aeron_publication_buffers_ready_t *)message;

            const char *log_file_name = (const char *)message + sizeof(aeron_publication_buffers_ready_t);
            snprintf(buffer, sizeof(buffer) - 1, "%s %d:%d %d %d [%" PRId64 " %" PRId64 "]\n    \"%*s\"",
                AERON_RESPONSE_ON_PUBLICATION_READY == cmd_id ? "ON_PUBLICATION_READY" : "ON_EXCLUSIVE_PUBLICATION_READY",
                command->session_id,
                command->stream_id,
                command->position_limit_counter_id,
                command->channel_status_indicator_id,
                command->correlation_id,
                command->registration_id,
                command->log_file_length,
                log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_SUBSCRIPTION_READY:
        {
            aeron_subscription_ready_t *command = (aeron_subscription_ready_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_SUBSCRIPTION_READY %" PRId64 " %d",
                command->correlation_id,
                command->channel_status_indicator_id);
            break;
        }

        case AERON_RESPONSE_ON_ERROR:
        {
            aeron_error_response_t *command = (aeron_error_response_t *)message;

            const char *error_message = (const char *)message + sizeof(aeron_error_response_t);
            snprintf(buffer, sizeof(buffer) - 1, "ON_ERROR %" PRId64 "%d %*s",
                command->offending_command_correlation_id,
                command->error_code,
                command->error_message_length,
                error_message);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
        {
            aeron_image_message_t *command = (aeron_image_message_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_image_message_t);
            snprintf(buffer, sizeof(buffer) - 1, "ON_UNAVAILABLE_IMAGE %d %*s [%" PRId64 "]",
                command->stream_id,
                command->channel_length,
                channel,
                command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_AVAILABLE_IMAGE:
        {
            aeron_image_buffers_ready_t *command = (aeron_image_buffers_ready_t *)message;
            char *log_file_name_ptr = (char *)message + sizeof(aeron_image_buffers_ready_t);
            int32_t log_file_name_length;
            memcpy(&log_file_name_length, log_file_name_ptr, sizeof(int32_t));
            const char *log_file_name = log_file_name_ptr + sizeof(int32_t);

            char *source_identity_ptr =
                log_file_name_ptr + AERON_ALIGN(log_file_name_length, sizeof(int32_t)) + sizeof(int32_t);
            int32_t source_identity_length;
            memcpy(&source_identity_length, source_identity_ptr, sizeof(int32_t));
            const char *source_identity = source_identity_ptr + sizeof(int32_t);

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "ON_AVAILABLE_IMAGE %d:%d [%" PRId32 ":%" PRId64 "] \"%*s\" [%" PRId64 "] \"%*s\"",
                command->session_id,
                command->stream_id,
                command->subscriber_position_id,
                command->subscriber_registration_id,
                source_identity_length,
                source_identity,
                command->correlation_id,
                log_file_name_length,
                log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_COUNTER_READY:
        {
            aeron_counter_update_t *command = (aeron_counter_update_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_COUNTER_READY %" PRId64 " %d",
                command->correlation_id,
                command->counter_id);
            break;
        }

        case AERON_RESPONSE_ON_CLIENT_TIMEOUT:
        {
            aeron_client_timeout_t *command = (aeron_client_timeout_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "ON_CLIENT_TIMEOUT %" PRId64, command->client_id);
            break;
        }

        default:
            break;
    }

    return buffer;
}

static char *dissect_flags(uint8_t flags, char *dissected_flags, size_t flags_length)
{
    const size_t len = flags_length < sizeof(uint8_t) ? flags_length : sizeof(uint8_t);
    uint8_t flag_mask = (uint8_t)(1 << (len - 1));

    for (size_t i = 0; i < len; i++)
    {
        dissected_flags[i] = (flags & flag_mask) == (flag_mask ? '1' : '0');
        flag_mask >>= 1;
    }

    return dissected_flags;
}

static const char *dissect_res_address(int8_t res_type, const uint8_t *address)
{
    static char addr_buffer[INET6_ADDRSTRLEN];
    int af = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD == res_type ? AF_INET6 : AF_INET;
    inet_ntop(af, address, addr_buffer, sizeof(addr_buffer));
    return addr_buffer;
}

static const char *dissect_sockaddr(const struct sockaddr *addr)
{
    static char addr_buffer[128], buffer[256];
    unsigned short port = 0;

    if (AF_INET == addr->sa_family)
    {
        struct sockaddr_in *addr4 = (struct sockaddr_in *)addr;

        inet_ntop(AF_INET, &addr4->sin_addr, addr_buffer, sizeof(addr_buffer));
        port = ntohs(addr4->sin_port);
    }
    else if (AF_INET6 == addr->sa_family)
    {
        struct sockaddr_in6 *addr6 = (struct sockaddr_in6 *)addr;

        inet_ntop(AF_INET6, &addr6->sin6_addr, addr_buffer, sizeof(addr_buffer));
        port = ntohs(addr6->sin6_port);
    }
    else
    {
        snprintf(addr_buffer, sizeof(addr_buffer) - 1, "%s", "unknown");
    }

    snprintf(buffer, sizeof(buffer) - 1, "%s.%d", addr_buffer, port);

    return buffer;
}

static const char *dissect_frame_type(int16_t type)
{
    switch (type)
    {
        case AERON_HDR_TYPE_DATA:
            return "DATA";

        case AERON_HDR_TYPE_PAD:
            return "PAD";

        case AERON_HDR_TYPE_SM:
            return "SM";

        case AERON_HDR_TYPE_NAK:
            return "NAK";

        case AERON_HDR_TYPE_SETUP:
            return "SETUP";

        case AERON_HDR_TYPE_RTTM:
            return "RTT";

        case AERON_HDR_TYPE_RES:
            return "RES";

        case AERON_HDR_TYPE_ATS_DATA:
            return "ATS_DATA";

        case AERON_HDR_TYPE_ATS_SETUP:
            return "ATS_SETUP";

        case AERON_HDR_TYPE_ATS_SM:
            return "ATS_SM";

        default:
            return "unknown command";
    }
}

static const char *dissect_frame(const void *message, size_t length)
{
    static char buffer[256];
    aeron_frame_header_t *hdr = (aeron_frame_header_t *)message;

    buffer[0] = '\0';
    switch (hdr->type)
    {
        case AERON_HDR_TYPE_DATA:
        case AERON_HDR_TYPE_PAD:
        case AERON_HDR_TYPE_ATS_DATA:
        {
            aeron_data_header_t *data = (aeron_data_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d:%d @%x",
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length,
                data->session_id,
                data->stream_id,
                data->term_id,
                data->term_offset);
            break;
        }

        case AERON_HDR_TYPE_SM:
        case AERON_HDR_TYPE_ATS_SM:
        {
            aeron_status_message_header_t *sm = (aeron_status_message_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d:%d @%x %d %" PRId64,
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length,
                sm->session_id,
                sm->stream_id,
                sm->consumption_term_id,
                sm->consumption_term_offset,
                sm->receiver_window,
                sm->receiver_id);
            break;
        }

        case AERON_HDR_TYPE_NAK:
        {
            aeron_nak_header_t *nak = (aeron_nak_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d:%d @%x %d",
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length,
                nak->session_id,
                nak->stream_id,
                nak->term_id,
                nak->term_offset,
                nak->length);
            break;
        }

        case AERON_HDR_TYPE_SETUP:
        case AERON_HDR_TYPE_ATS_SETUP:
        {
            aeron_setup_header_t *setup = (aeron_setup_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d:%d %d @%x %d MTU %d TTL %d",
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length,
                setup->session_id,
                setup->stream_id,
                setup->active_term_id,
                setup->initial_term_id,
                setup->term_offset,
                setup->term_length,
                setup->mtu,
                setup->ttl);
            break;
        }

        case AERON_HDR_TYPE_RTTM:
        {
            aeron_rttm_header_t *rttm = (aeron_rttm_header_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "%s 0x%x len %d %d:%d %" PRId64 " %" PRId64 " %" PRId64,
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length,
                rttm->session_id,
                rttm->stream_id,
                rttm->echo_timestamp,
                rttm->reception_delta,
                rttm->receiver_id);
            break;
        }

        case AERON_HDR_TYPE_RES:
        {
            uint8_t null_buffer[16] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
            char dissected_flags[8] = { 0, 0, 0, 0, 0, 0, 0, 0 };
            const uint8_t *message_bytes = (uint8_t *)message;
            const int buffer_available = sizeof(buffer) - 1;

            int buffer_used = snprintf(
                buffer,
                buffer_available,
                "%s 0x%x len %d",
                dissect_frame_type(hdr->type),
                hdr->flags,
                hdr->frame_length);
            size_t message_offset = sizeof(aeron_frame_header_t);

            while (message_offset < length && buffer_used < buffer_available)
            {
                aeron_resolution_header_t *res = (aeron_resolution_header_t *)&message_bytes[message_offset];
                const int entry_length = aeron_res_header_entry_length(res, length - message_offset);
                if (entry_length < 0)
                {
                    break;
                }

                const uint8_t *address = null_buffer;
                const uint8_t *name = null_buffer;
                int16_t name_length = 0;

                switch (res->res_type)
                {
                    case AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD:
                    {
                        aeron_resolution_header_ipv6_t *res_ipv6 = (aeron_resolution_header_ipv6_t *)res;
                        address = res_ipv6->addr;
                        name_length = res_ipv6->name_length;
                        name = &message_bytes[message_offset + sizeof(aeron_resolution_header_ipv6_t)];
                        break;
                    }

                    case AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD:
                    {
                        aeron_resolution_header_ipv4_t *res_ipv4 = (aeron_resolution_header_ipv4_t *)res;
                        address = res_ipv4->addr;
                        name_length = res_ipv4->name_length;
                        name = &message_bytes[message_offset + sizeof(aeron_resolution_header_ipv4_t)];
                        break;
                    }
                }

                buffer_used += snprintf(
                    &buffer[buffer_used], buffer_available - buffer_used,
                    " [%" PRId8 " %.*s port %" PRIu16 " %" PRId32 " %s %.*s]",
                    res->res_type,
                    (int)sizeof(dissected_flags),
                    dissect_flags(res->res_flags, dissected_flags, sizeof(dissected_flags)),
                    res->udp_port,
                    res->age_in_ms,
                    dissect_res_address(res->res_type, address),
                    (int)name_length,
                    (char *)name);

                message_offset += entry_length;
            }

            if (buffer_available < buffer_used)
            {
                snprintf(&buffer[buffer_available - 3], 4, "...");
            }

            break;
        }

        default:
            break;
    }

    return buffer;
}

static const char *dissect_tether_state(aeron_subscription_tether_state_t state)
{
    switch (state)
    {
        case AERON_SUBSCRIPTION_TETHER_ACTIVE:
            return "ACTIVE";

        case AERON_SUBSCRIPTION_TETHER_LINGER:
            return "LINGER";

        case AERON_SUBSCRIPTION_TETHER_RESTING:
            return "RESTING";

        default:
            return "unknown tether state";
    }
}

void aeron_driver_agent_log_dissector(int32_t msg_type_id, const void *message, size_t length, void *clientd)
{
    switch (msg_type_id)
    {
        case AERON_DRIVER_EVENT_FRAME_IN:
        case AERON_DRIVER_EVENT_FRAME_OUT:
        {
            aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)message;
            const struct sockaddr *addr =
                (const struct sockaddr *)((const char *)message + sizeof(aeron_driver_agent_frame_log_header_t));
            const char *frame =
                (const char *)message + sizeof(aeron_driver_agent_frame_log_header_t) + hdr->sockaddr_len;

            fprintf(
                logfp,
                "%s: %s %s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, (size_t)hdr->message_len),
                dissect_sockaddr(addr),
                dissect_frame(frame, (size_t)hdr->message_len));
            break;
        }

        case AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP:
        {
            aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_remove_resource_cleanup_t);
            fprintf(
                    logfp,
                    "%s: sessionId=%d, streamId=%d, uri=%*s\n",
                    aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                    hdr->session_id,
                    hdr->stream_id,
                    hdr->channel_length,
                    channel);
            break;
        }

        case AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP:
        {
            aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_remove_resource_cleanup_t);
            fprintf(
                logfp,
                "%s: streamId=%d, id=%" PRId64 ", uri=%*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->stream_id,
                hdr->id,
                hdr->channel_length,
                channel);
            break;
        }

        case AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP:
        {
            aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_remove_resource_cleanup_t);
            fprintf(
                logfp,
                "%s: sessionId=%d, streamId=%d, id=%" PRId64 ", uri=%*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->session_id,
                hdr->stream_id,
                hdr->id,
                hdr->channel_length,
                channel);
            break;
        }

        case AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION:
        case AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE:
        case AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION:
        case AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE:
        {
            aeron_driver_agent_on_endpoint_change_t *hdr = (aeron_driver_agent_on_endpoint_change_t *)message;
            fprintf(
                logfp,
                "%s: localData: %s, remoteData: %s, ttl: %d\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                dissect_sockaddr((const struct sockaddr *) &hdr->local_data),
                dissect_sockaddr((const struct sockaddr *) &hdr->remote_data),
                hdr->multicast_ttl);
            break;
        }

        case AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE:
        {
            aeron_driver_agent_untethered_subscription_state_change_log_header_t *hdr =
                    (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)message;

            fprintf(
                logfp,
                "%s: subscriptionId=%" PRId64 ", streamId=%d, sessionId=%d, %s -> %s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->subscription_id,
                hdr->stream_id,
                hdr->session_id,
                dissect_tether_state(hdr->old_state),
                dissect_tether_state(hdr->new_state));
            break;
        }

        case AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED:
        case AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED:
        {
            aeron_driver_agent_log_header_t *hdr = (aeron_driver_agent_log_header_t *)message;
            const struct sockaddr *addr =
                    (const struct sockaddr *)((const char *)message + sizeof(aeron_driver_agent_untethered_subscription_state_change_log_header_t));

            fprintf(
                logfp,
                "%s: %s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                dissect_sockaddr(addr));
            break;
        }

        default:
            if (is_valid_event_id(msg_type_id))
            {
                if (CMD_IN_EVENTS[msg_type_id])
                {
                    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)message;

                    fprintf(
                        logfp,
                        "%s: %s\n",
                        aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                        dissect_cmd_in(
                            hdr->cmd_id,
                            (const char *)message + sizeof(aeron_driver_agent_cmd_log_header_t),
                            length - sizeof(aeron_driver_agent_cmd_log_header_t)));
                }
                else if (CMD_OUT_EVENTS[msg_type_id])
                {
                    aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)message;

                    fprintf(
                        logfp,
                        "%s: %s\n",
                        aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                        dissect_cmd_out(
                            hdr->cmd_id,
                            (const char *)message + sizeof(aeron_driver_agent_cmd_log_header_t),
                            length - sizeof(aeron_driver_agent_cmd_log_header_t)));
                }
            }
            break;
    }
}

static void log_remove_resource_cleanup_event(
    const int64_t id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel,
    const aeron_driver_agent_event_t event_id,
    const size_t command_length,
    char *buffer)
{
    aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)buffer;

    hdr->time_ns = aeron_nano_clock();
    hdr->id = id;
    hdr->stream_id = stream_id;
    hdr->session_id = session_id;
    hdr->channel_length = channel_length;

    memcpy(buffer + sizeof(aeron_driver_agent_remove_resource_cleanup_t), channel, channel_length);

    aeron_mpsc_rb_write(&logging_mpsc_rb, event_id, buffer, command_length);
}

static void log_remove_resource_cleanup(
    const int64_t id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel,
    const aeron_driver_agent_event_t event_id)
{
    const size_t command_length = sizeof(aeron_driver_agent_remove_resource_cleanup_t) + channel_length;

    if (command_length > sizeof(aeron_driver_agent_remove_resource_cleanup_t) + AERON_MAX_PATH)
    {
        char *buffer = NULL;
        if (aeron_alloc((void **)&buffer, command_length) < 0)
        {
            return;
        }
        log_remove_resource_cleanup_event(
            id, session_id, stream_id, channel_length, channel, event_id, command_length, buffer);
        aeron_free(buffer);
    }
    else
    {
        char buffer[sizeof(aeron_driver_agent_remove_resource_cleanup_t) + AERON_MAX_PATH];
        log_remove_resource_cleanup_event(
            id, session_id, stream_id, channel_length, channel, event_id, command_length, buffer);
    }
}

void aeron_driver_agent_remove_publication_cleanup(
    const int32_t session_id, const int32_t stream_id, const size_t channel_length, const char *channel)
{
    log_remove_resource_cleanup(
        AERON_NULL_VALUE,
        session_id,
        stream_id,
        channel_length,
        channel,
        AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP);
}

void aeron_driver_agent_remove_subscription_cleanup(
    const int64_t id, const int32_t stream_id, const size_t channel_length, const char *channel)
{
    log_remove_resource_cleanup(
        id,
        AERON_NULL_VALUE,
        stream_id,
        channel_length,
        channel,
        AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP);
}

void aeron_driver_agent_remove_image_cleanup(
    const int64_t id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel)
{
    log_remove_resource_cleanup(
        id,
        session_id,
        stream_id,
        channel_length,
        channel,
        AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP);
}

static void log_endpoint_change_event(const aeron_driver_agent_event_t event_id, const void *channel)
{
    char buffer[sizeof(aeron_driver_agent_on_endpoint_change_t)];
    aeron_driver_agent_on_endpoint_change_t *hdr = (aeron_driver_agent_on_endpoint_change_t *)buffer;

    const aeron_udp_channel_t *udp_channel = channel;

    hdr->time_ns = aeron_nano_clock();
    memcpy(&hdr->local_data, &udp_channel->local_data, AERON_ADDR_LEN(&udp_channel->local_data));
    memcpy(&hdr->remote_data, &udp_channel->remote_data, AERON_ADDR_LEN(&udp_channel->remote_data));
    hdr->multicast_ttl = udp_channel->multicast_ttl;

    aeron_mpsc_rb_write(&logging_mpsc_rb, event_id, buffer, sizeof(aeron_driver_agent_on_endpoint_change_t));
}

void aeron_driver_agent_sender_proxy_on_add_endpoint(const void *channel)
{
    log_endpoint_change_event(AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION, channel);
}

void aeron_driver_agent_sender_proxy_on_remove_endpoint(const void *channel)
{
    log_endpoint_change_event(AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE, channel);
}

void aeron_driver_agent_receiver_proxy_on_add_endpoint(const void *channel)
{
    log_endpoint_change_event(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION, channel);
}

void aeron_driver_agent_receiver_proxy_on_remove_endpoint(const void *channel)
{
    log_endpoint_change_event(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE, channel);
}
