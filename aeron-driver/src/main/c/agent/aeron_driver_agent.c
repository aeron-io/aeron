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
#include "aeron_alloc.h"
#include "aeron_driver_version.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_strutil.h"
#include "aeron_windows.h"

#if !defined(HAVE_STRUCT_MMSGHDR)
struct mmsghdr
{
    struct msghdr msg_hdr;
    unsigned int msg_len;
};
#endif

#define AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN (0)
#define AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN (1)
#define AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT (2)
#define AERON_DRIVER_AGENT_EVENT_TYPE_OTHER (3)

#define AERON_DRIVER_AGENT_MAX_EVENT_NAME_LENGTH (64)

typedef struct aeron_driver_agent_dynamic_dissector_entry_stct
{
    aeron_driver_agent_generic_dissector_func_t dissector_func;
}
aeron_driver_agent_dynamic_dissector_entry_t;

typedef struct aeron_driver_agent_log_event_stct
{
    char name[AERON_DRIVER_AGENT_MAX_EVENT_NAME_LENGTH];
    uint8_t type;
    bool enabled;
}
aeron_driver_agent_log_event_t;

static AERON_INIT_ONCE agent_is_initialized = AERON_INIT_ONCE_VALUE;
static aeron_mpsc_rb_t logging_mpsc_rb;
static uint8_t *rb_buffer = NULL;
static FILE *logfp = NULL;
static aeron_thread_t log_reader_thread;
static aeron_driver_agent_dynamic_dissector_entry_t *dynamic_dissector_entries = NULL;
static size_t num_dynamic_dissector_entries = 0;
static int64_t dynamic_dissector_index = 0;
static aeron_driver_agent_log_event_t log_events[] =
    {
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { "FRAME_IN",                             AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "FRAME_OUT",                            AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "CMD_IN_ADD_PUBLICATION",               AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REMOVE_PUBLICATION",            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_ADD_SUBSCRIPTION",              AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REMOVE_SUBSCRIPTION",           AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_OUT_PUBLICATION_READY",            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_OUT_AVAILABLE_IMAGE",              AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { "CMD_OUT_ON_OPERATION_SUCCESS",         AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_IN_KEEPALIVE_CLIENT",              AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "REMOVE_PUBLICATION_CLEANUP",           AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "REMOVE_SUBSCRIPTION_CLEANUP",          AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "REMOVE_IMAGE_CLEANUP",                 AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "CMD_OUT_ON_UNAVAILABLE_IMAGE",         AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { "SEND_CHANNEL_CREATION",                AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "RECEIVE_CHANNEL_CREATION",             AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "SEND_CHANNEL_CLOSE",                   AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "RECEIVE_CHANNEL_CLOSE",                AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME,  AERON_DRIVER_AGENT_EVENT_TYPE_UNKNOWN, false },
        { "CMD_IN_ADD_DESTINATION",               AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REMOVE_DESTINATION",            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_ADD_EXCLUSIVE_PUBLICATION",     AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_OUT_EXCLUSIVE_PUBLICATION_READY",  AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_OUT_ERROR",                        AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_IN_ADD_COUNTER",                   AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REMOVE_COUNTER",                AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_OUT_SUBSCRIPTION_READY",           AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_OUT_COUNTER_READY",                AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_OUT_ON_UNAVAILABLE_COUNTER",       AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_IN_CLIENT_CLOSE",                  AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_ADD_RCV_DESTINATION",           AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REMOVE_RCV_DESTINATION",        AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_OUT_ON_CLIENT_TIMEOUT",            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, false },
        { "CMD_IN_TERMINATE_DRIVER",              AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "UNTETHERED_SUBSCRIPTION_STATE_CHANGE", AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAME_RESOLUTION_NEIGHBOR_ADDED",       AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAME_RESOLUTION_NEIGHBOR_REMOVED",     AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "FLOW_CONTROL_RECEIVER_ADDED",          AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "FLOW_CONTROL_RECEIVER_REMOVED",        AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAME_RESOLUTION_RESOLVE",              AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "GENERIC_MESSAGE",                      AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAME_RESOLUTION_LOOKUP",               AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAME_RESOLUTION_HOST_NAME",            AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "NAK_SENT",                             AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "RESEND",                               AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "CMD_IN_REMOVE_DESTINATION_BY_ID",      AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "CMD_IN_REJECT_IMAGE",                  AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,  false },
        { "NAK_RECEIVED",                         AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "PUBLICATION_REVOKE",                   AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN,   false },
        { "PUBLICATION_IMAGE_REVOKE",             AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "ADD_DYNAMIC_DISSECTOR",                AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
        { "DYNAMIC_DISSECTOR_EVENT",              AERON_DRIVER_AGENT_EVENT_TYPE_OTHER,   false },
    };

#define AERON_DRIVER_EVENT_NUM_ELEMENTS (sizeof(log_events) / sizeof(aeron_driver_agent_log_event_t))

size_t aeron_driver_agent_max_event_count(void)
{
    return AERON_DRIVER_EVENT_NUM_ELEMENTS;
}

aeron_mpsc_rb_t *aeron_driver_agent_mpsc_rb(void)
{
    return &logging_mpsc_rb;
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

void aeron_driver_agent_logging_ring_buffer_init(void)
{
    size_t rb_length = AERON_EVENT_RB_LENGTH + AERON_RB_TRAILER_LENGTH;

    if ((rb_buffer = (uint8_t *)malloc(rb_length)) == NULL)
    {
        fprintf(stderr, "could not allocate ring buffer buffer. exiting.\n");
        exit(EXIT_FAILURE);
    }

    memset(rb_buffer, 0, rb_length);

    if (aeron_mpsc_rb_init(&logging_mpsc_rb, rb_buffer, rb_length) < 0)
    {
        fprintf(stderr, "could not init logging mpsc_rb. exiting.\n");
        exit(EXIT_FAILURE);
    }
}

void aeron_driver_agent_logging_ring_buffer_free(void)
{
    if (NULL != rb_buffer)
    {
        aeron_free(rb_buffer);
        rb_buffer = NULL;
    }
}

static bool aeron_driver_agent_is_unknown_event(const char *event_name)
{
    return 0 == strncmp(
        AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME, event_name, strlen(AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME) + 1);
}

static aeron_driver_agent_event_t aeron_driver_agent_event_name_to_id(const char *event_name)
{
    if (aeron_driver_agent_is_unknown_event(event_name))
    {
        return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
    }

    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        const char *name = log_events[i].name;
        if (0 == strncmp(name, event_name, strlen(name) + 1))
        {
            return (aeron_driver_agent_event_t)i;
        }
    }

    return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
}

static inline bool is_valid_event_id(const int id)
{
    return id >= 0 && id < (int)AERON_DRIVER_EVENT_NUM_ELEMENTS;
}

const char *aeron_driver_agent_event_name(const aeron_driver_agent_event_t id)
{
    if (is_valid_event_id(id))
    {
        return log_events[id].name;
    }

    return AERON_DRIVER_AGENT_EVENT_UNKNOWN_NAME;
}

bool aeron_driver_agent_is_event_enabled(const aeron_driver_agent_event_t id)
{
    return is_valid_event_id(id) && log_events[id].enabled;
}

static void aeron_driver_agent_set_enabled_all_events(const bool is_enabled)
{
    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        const char *event_name = log_events[i].name;
        if (!aeron_driver_agent_is_unknown_event(event_name))
        {
            log_events[i].enabled = is_enabled;
        }
    }
}

static void aeron_driver_agent_set_enabled_admin_events(const bool is_enabled)
{
    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        if (AERON_DRIVER_EVENT_FRAME_IN != i &&
            AERON_DRIVER_EVENT_FRAME_OUT != i &&
            AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR != i &&
            AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT != i)
        {
            const char *event_name = log_events[i].name;
            if (!aeron_driver_agent_is_unknown_event(event_name))
            {
                log_events[i].enabled = is_enabled;
            }
        }
    }
}

static void aeron_driver_agent_set_enabled_specific_events(const uint8_t type, const bool is_enabled)
{
    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        if (type == log_events[i].type)
        {
            log_events[i].enabled = is_enabled;
        }
    }
}

static bool any_event_enabled(const uint8_t type)
{
    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        if (type == log_events[i].type && log_events[i].enabled)
        {
            return true;
        }
    }

    return false;
}

static aeron_driver_agent_event_t parse_event_name(const char *event_name)
{
    if (0 == strncmp("SEND_NAK_MESSAGE", event_name, strlen("SEND_NAK_MESSAGE") + 1))
    {
        return AERON_DRIVER_EVENT_NAK_SENT;
    }

    aeron_driver_agent_event_t event_id = aeron_driver_agent_event_name_to_id(event_name);
    if (AERON_DRIVER_EVENT_UNKNOWN_EVENT != event_id)
    {
        return event_id;
    }

    const long id = strtol(event_name, NULL, 0);
    if (is_valid_event_id((int)id) &&
        !aeron_driver_agent_is_unknown_event(aeron_driver_agent_event_name((aeron_driver_agent_event_t)id)))
    {
        return (aeron_driver_agent_event_t)id;
    }

    return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
}

static bool aeron_driver_agent_events_set_enabled(char const **events, const int num_events, const bool is_enabled)
{
    bool result = false;

    for (int i = num_events - 1; i >= 0; i--)
    {
        const char *event_name = events[i];
        if (0 == strncmp(AERON_DRIVER_AGENT_ALL_EVENTS, event_name, strlen(AERON_DRIVER_AGENT_ALL_EVENTS) + 1))
        {
            aeron_driver_agent_set_enabled_all_events(is_enabled);
            result = true;
            break;
        }
        else if (0 == strncmp(AERON_DRIVER_AGENT_ADMIN_EVENTS, event_name, strlen(AERON_DRIVER_AGENT_ADMIN_EVENTS) + 1))
        {
            aeron_driver_agent_set_enabled_admin_events(is_enabled);
            result = true;
        }
        else if (0 == strncmp("0x", event_name, 2))  // Legacy mask-based events
        {
            const uint64_t mask = strtoull(event_name, NULL, 0);

            if (0 != mask)
            {
                if (mask >= 0xFFFFu)
                {
                    aeron_driver_agent_set_enabled_all_events(is_enabled);
                    result = true;
                }
                else
                {
                    if (mask & 0x1u)
                    {
                        aeron_driver_agent_set_enabled_specific_events(
                            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN, is_enabled);
                        result = true;
                    }

                    if (mask & 0x2u)
                    {
                        aeron_driver_agent_set_enabled_specific_events(
                            AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT, is_enabled);
                        result = true;
                    }

                    if (mask & 0x4u)
                    {
                        log_events[AERON_DRIVER_EVENT_FRAME_IN].enabled = is_enabled;
                        result = true;
                    }

                    if (mask & 0x8u || mask & 0x10u)
                    {
                        log_events[AERON_DRIVER_EVENT_FRAME_OUT].enabled = is_enabled;
                        result = true;
                    }

                    if (mask & 0x80u)
                    {
                        log_events[AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE].enabled = is_enabled;
                        result = true;
                    }

                    if (mask & 0x100u)
                    {
                        log_events[AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT].enabled = is_enabled;
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
                log_events[event_id].enabled = is_enabled;
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

bool aeron_driver_agent_logging_events_init(const char *event_log, const char *event_log_disable)
{
    aeron_driver_agent_logging_events_free();

    if (NULL == event_log)
    {
        return false;
    }

    char *event_log_dup = strdup(event_log);
    if (NULL == event_log_dup)
    {
        fprintf(stderr, "failed to copy logging events string\n");
        return false;
    }

    char *events[AERON_DRIVER_EVENT_NUM_ELEMENTS];
    const int num_events = aeron_tokenise(event_log_dup, ',', AERON_DRIVER_EVENT_NUM_ELEMENTS, events);

    if (num_events < 0)
    {
        fprintf(stderr, "failed to parse logging events: '%s'\n", event_log);
        aeron_free(event_log_dup);
        return false;
    }

    char *events_disable[AERON_DRIVER_EVENT_NUM_ELEMENTS];
    int num_events_disable = 0;
    char *event_log_disable_dup = NULL;

    if (NULL != event_log_disable)
    {
        event_log_disable_dup = strdup(event_log_disable);
        if (NULL == event_log_disable_dup)
        {
            fprintf(stderr, "failed to copy logging disable events string\n");
            aeron_free(event_log_dup);
            return false;
        }

        num_events_disable = aeron_tokenise(
            event_log_disable_dup, ',', AERON_DRIVER_EVENT_NUM_ELEMENTS, events_disable);

        if (num_events_disable < 0)
        {
            fprintf(stderr, "failed to parse logging events: '%s'\n", event_log_disable);
            aeron_free(event_log_dup);
            aeron_free(event_log_disable_dup);
            return false;
        }
    }

    bool result = aeron_driver_agent_events_set_enabled((const char **)events, num_events, true);
    if (0 < num_events_disable)
    {
        result &= aeron_driver_agent_events_set_enabled((const char **)events_disable, num_events_disable, false);
    }

    aeron_free(event_log_dup);
    aeron_free(event_log_disable_dup);

    return result;
}

void aeron_driver_agent_logging_events_free(void)
{
    for (size_t i = 0; i < AERON_DRIVER_EVENT_NUM_ELEMENTS; i++)
    {
        log_events[i].enabled = false;
    }

    dynamic_dissector_entries = NULL;
    dynamic_dissector_index = 0;
    num_dynamic_dissector_entries = 0;
}

static void initialize_agent_logging(void)
{
    const char *event_log_str = getenv(AERON_EVENT_LOG_ENV_VAR);
    const char *event_log_disable_str = getenv(AERON_EVENT_LOG_DISABLE_ENV_VAR);

    if (aeron_driver_agent_logging_events_init(event_log_str, event_log_disable_str))
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
            return AERON_DRIVER_EVENT_CMD_OUT_AVAILABLE_IMAGE;

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

        case AERON_COMMAND_REMOVE_DESTINATION_BY_ID:
            return AERON_DRIVER_EVENT_CMD_IN_REMOVE_DESTINATION_BY_ID;

        case AERON_COMMAND_REJECT_IMAGE:
            return AERON_DRIVER_EVENT_CMD_IN_REJECT_IMAGE;

        default:
            return AERON_DRIVER_EVENT_UNKNOWN_EVENT;
    }
}

static void aeron_driver_agent_log_nak_message(
    const aeron_driver_agent_event_t log_event,
    const struct sockaddr_storage *address,
    const int32_t session_id,
    const int32_t stream_id,
    const int32_t term_id,
    const int32_t term_offset,
    const int32_t nak_length,
    const size_t channel_length,
    const char *channel)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        log_event,
        sizeof(aeron_driver_agent_nak_message_header_t) +
            channel_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_nak_message_header_t *hdr =
            (aeron_driver_agent_nak_message_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        memcpy(&hdr->address, address, sizeof(hdr->address));
        hdr->session_id = session_id;
        hdr->stream_id = stream_id;
        hdr->term_id = term_id;
        hdr->term_offset = term_offset;
        hdr->nak_length = nak_length;
        hdr->channel_length = (int32_t)channel_length;

        ptr += sizeof(aeron_driver_agent_nak_message_header_t);
        memcpy(ptr, channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
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

    int32_t offset = aeron_mpsc_rb_try_claim(&logging_mpsc_rb, event_id, command_length);
    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)ptr;
        hdr->time_ns = aeron_nano_clock();
        hdr->cmd_id = msg_type_id;
        memcpy(ptr + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
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
    int32_t offset = aeron_mpsc_rb_try_claim(&logging_mpsc_rb, event_id, command_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_cmd_log_header_t *hdr = (aeron_driver_agent_cmd_log_header_t *)ptr;
        hdr->time_ns = aeron_nano_clock();
        hdr->cmd_id = msg_type_id;
        memcpy(ptr + sizeof(aeron_driver_agent_cmd_log_header_t), message, length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_log_frame(int32_t msg_type_id, const struct msghdr *msghdr, int32_t message_len)
{
    const int32_t copy_length = message_len < AERON_MAX_FRAME_LENGTH ? message_len : AERON_MAX_FRAME_LENGTH;
    const size_t command_length =
        sizeof(aeron_driver_agent_frame_log_header_t) + msghdr->msg_namelen + copy_length;
    int32_t offset = aeron_mpsc_rb_try_claim(&logging_mpsc_rb, msg_type_id, command_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->sockaddr_len = msghdr->msg_namelen;
        hdr->message_len = message_len;

        if (msghdr->msg_iovlen > 1)
        {
            fprintf(stderr, "only aware of 1 iov. %d iovs detected.\n", (int)msghdr->msg_iovlen);
        }

        ptr += sizeof(aeron_driver_agent_frame_log_header_t);
        memcpy(ptr, msghdr->msg_name, msghdr->msg_namelen);

        ptr += msghdr->msg_namelen;
        memcpy(ptr, msghdr->msg_iov[0].iov_base, (size_t)copy_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_log_frame_iov(
    int32_t msg_type_id, const struct sockaddr_storage *address, struct iovec *iov, int32_t message_len)
{
    const int32_t copy_length = message_len < AERON_MAX_FRAME_LENGTH ? message_len : AERON_MAX_FRAME_LENGTH;
    size_t address_length = AERON_ADDR_LEN(address);
    const size_t command_length =
        sizeof(aeron_driver_agent_frame_log_header_t) + address_length + copy_length;
    int32_t offset = aeron_mpsc_rb_try_claim(&logging_mpsc_rb, msg_type_id, command_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_frame_log_header_t *hdr = (aeron_driver_agent_frame_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->sockaddr_len = (int32_t)address_length;
        hdr->message_len = message_len;

        ptr += sizeof(aeron_driver_agent_frame_log_header_t);
        memcpy(ptr, address, address_length);

        ptr += address_length;
        memcpy(ptr, iov->iov_base, (size_t)copy_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

int aeron_driver_agent_outgoing_send(
    void *interceptor_state,
    aeron_udp_channel_outgoing_interceptor_t *delegate,
    aeron_udp_channel_transport_t *transport,
    struct sockaddr_storage *address,
    struct iovec *iov,
    size_t iov_length,
    int64_t *bytes_sent)
{
    for (size_t i = 0; i < iov_length; i++)
    {
        aeron_driver_agent_log_frame_iov(
            AERON_DRIVER_EVENT_FRAME_OUT,
            address,
            &iov[i],
            (int32_t)iov[i].iov_len);
    }

    int result = 0;
    if (NULL != delegate)
    {
        result = delegate->outgoing_send_func(
            delegate->interceptor_state, delegate->next_interceptor, transport, address, iov, iov_length, bytes_sent);
    }

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
    struct sockaddr_storage *addr,
    struct timespec *media_timestamp)
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
    message.msg_namelen = AERON_ADDR_LEN(addr);

    aeron_driver_agent_log_frame(AERON_DRIVER_EVENT_FRAME_IN, &message, (int32_t)length);

    if (NULL != delegate)
    {
        delegate->incoming_func(
            delegate->interceptor_state,
            delegate->next_interceptor,
            transport,
            receiver_clientd,
            endpoint_clientd,
            destination_clientd,
            buffer,
            length,
            addr,
            media_timestamp);
    }
}

void aeron_driver_agent_untethered_subscription_state_change(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t old_state,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE,
        sizeof(aeron_driver_agent_untethered_subscription_state_change_log_header_t));

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_untethered_subscription_state_change_log_header_t *hdr =
            (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->subscription_id = tetherable_position->subscription_registration_id;
        hdr->stream_id = stream_id;
        hdr->session_id = session_id;
        hdr->old_state = old_state;
        hdr->new_state = new_state;

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void log_name_resolution_neighbor_change(const aeron_driver_agent_event_t id, const struct sockaddr_storage *addr)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb, id, sizeof(aeron_driver_agent_log_header_t) + AERON_ADDR_LEN(addr));

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_log_header_t *hdr = (aeron_driver_agent_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();

        ptr += sizeof(aeron_driver_agent_log_header_t);
        memcpy(ptr, addr, AERON_ADDR_LEN(addr));

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_name_resolution_on_neighbor_added(const struct sockaddr_storage *addr)
{
    log_name_resolution_neighbor_change(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED, addr);
}

void aeron_driver_agent_name_resolution_on_neighbor_removed(const struct sockaddr_storage *addr)
{
    log_name_resolution_neighbor_change(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED, addr);
}

void log_flow_control_on_receiver_change(
    const aeron_driver_agent_event_t event_id,
    const int64_t receiver_id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel,
    const size_t receiver_count)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        event_id,
        sizeof(aeron_driver_agent_flow_control_receiver_change_log_header_t) + channel_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_flow_control_receiver_change_log_header_t *hdr =
            (aeron_driver_agent_flow_control_receiver_change_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->receiver_id = receiver_id;
        hdr->session_id = session_id;
        hdr->stream_id = stream_id;
        hdr->channel_length = (int32_t)channel_length;
        hdr->receiver_count = (int32_t)receiver_count;

        memcpy(ptr + sizeof(aeron_driver_agent_flow_control_receiver_change_log_header_t), channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_flow_control_on_receiver_added(
    int64_t receiver_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    size_t receiver_count)
{
    log_flow_control_on_receiver_change(
        AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_ADDED,
        receiver_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        receiver_count);
}

void aeron_driver_agent_flow_control_on_receiver_removed(
    int64_t receiver_id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel,
    size_t receiver_count)
{
    log_flow_control_on_receiver_change(
        AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_REMOVED,
        receiver_id,
        session_id,
        stream_id,
        channel_length,
        channel,
        receiver_count);
}

int32_t aeron_driver_agent_socket_address_length(const struct sockaddr_storage *address)
{
    if (NULL == address)
    {
        return 0;
    }
    else if (AF_INET == address->ss_family)
    {
        return sizeof(((struct sockaddr_in*)(address))->sin_addr);
    }
    else if (AF_INET6 == address->ss_family)
    {
        return sizeof(((struct sockaddr_in6*)(address))->sin6_addr);
    }

    return 0;
}

void aeron_driver_agent_send_nak_message(
    const struct sockaddr_storage *address,
    const int32_t session_id,
    const int32_t stream_id,
    const int32_t term_id,
    const int32_t term_offset,
    const int32_t nak_length,
    const size_t channel_length,
    const char *channel)
{
    aeron_driver_agent_log_nak_message(
        AERON_DRIVER_EVENT_NAK_SENT,
        address,
        session_id,
        stream_id,
        term_id,
        term_offset,
        nak_length,
        channel_length,
        channel);
}

void aeron_driver_agent_on_nak_message(
    const struct sockaddr_storage *address,
    const int32_t session_id,
    const int32_t stream_id,
    const int32_t term_id,
    const int32_t term_offset,
    const int32_t nak_length,
    const size_t channel_length,
    const char *channel)
{
    aeron_driver_agent_log_nak_message(
        AERON_DRIVER_EVENT_NAK_RECEIVED,
        address,
        session_id,
        stream_id,
        term_id,
        term_offset,
        nak_length,
        channel_length,
        channel);
}

void aeron_driver_agent_resend(
    const int32_t session_id,
    const int32_t stream_id,
    const int32_t term_id,
    const int32_t term_offset,
    const int32_t resend_length,
    const size_t channel_length,
    const char *channel)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_RESEND,
        sizeof(aeron_driver_agent_resend_header_t) +
            channel_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_resend_header_t *hdr =
            (aeron_driver_agent_resend_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->session_id = session_id;
        hdr->stream_id = stream_id;
        hdr->term_id = term_id;
        hdr->term_offset = term_offset;
        hdr->resend_length = resend_length;
        hdr->channel_length = (int32_t)channel_length;

        ptr += sizeof(aeron_driver_agent_resend_header_t);
        memcpy(ptr, channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_publication_revoke(
    int64_t revoked_pos,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_PUBLICATION_REVOKE,
        sizeof(aeron_driver_agent_publication_revoke_header_t) +
            channel_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_publication_revoke_header_t *hdr =
            (aeron_driver_agent_publication_revoke_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->revoked_pos = revoked_pos;
        hdr->session_id = session_id;
        hdr->stream_id = stream_id;
        hdr->channel_length = (int32_t)channel_length;

        ptr += sizeof(aeron_driver_agent_publication_revoke_header_t);
        memcpy(ptr, channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_publication_image_revoke(
    int64_t revoked_pos,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel)
{
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_PUBLICATION_IMAGE_REVOKE,
        sizeof(aeron_driver_agent_publication_image_revoke_header_t) +
            channel_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_publication_image_revoke_header_t *hdr =
            (aeron_driver_agent_publication_image_revoke_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->revoked_pos = revoked_pos;
        hdr->session_id = session_id;
        hdr->stream_id = stream_id;
        hdr->channel_length = (int32_t)channel_length;

        ptr += sizeof(aeron_driver_agent_publication_image_revoke_header_t);
        memcpy(ptr, channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_socket_address_copy(uint8_t *ptr, const struct sockaddr_storage *address, const size_t address_length)
{
    if (AF_INET == address->ss_family)
    {
        struct sockaddr_in *address_in = (struct sockaddr_in *)address;
        memcpy(ptr, &address_in->sin_addr, address_length);
    }
    else if (AF_INET6 == address->ss_family)
    {
        struct sockaddr_in6 *address_in6 = (struct sockaddr_in6 *)address;
        memcpy(ptr, &address_in6->sin6_addr, address_length);
    }
}


void aeron_driver_agent_name_resolver_on_resolve(
    aeron_name_resolver_t *name_resolver,
    int64_t duration_ns,
    const char *hostname,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    const size_t resolverNameFullLength = strlen(name_resolver->name);
    const size_t resolverNameLength = resolverNameFullLength < AERON_MAX_HOSTNAME_LEN ?
        resolverNameFullLength : AERON_MAX_HOSTNAME_LEN;

    const size_t hostnameFullLength = strlen(hostname);
    const size_t hostnameLength = hostnameFullLength < AERON_MAX_HOSTNAME_LEN ?
        hostnameFullLength : AERON_MAX_HOSTNAME_LEN;

    const size_t addressLength = aeron_driver_agent_socket_address_length(address);

    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_NAME_RESOLUTION_RESOLVE,
        sizeof(aeron_driver_agent_name_resolver_resolve_log_header_t) +
            resolverNameLength +
            hostnameLength +
            addressLength);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_name_resolver_resolve_log_header_t *hdr =
            (aeron_driver_agent_name_resolver_resolve_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->duration_ns = duration_ns;
        hdr->resolver_name_length = (int32_t)resolverNameLength;
        hdr->hostname_length = (int32_t)hostnameLength;
        hdr->address_length = (int32_t)addressLength;
        hdr->is_re_resolution = is_re_resolution;

        uint8_t *bodyPtr = ptr + sizeof(aeron_driver_agent_name_resolver_resolve_log_header_t);
        memcpy(bodyPtr, name_resolver->name, (size_t)resolverNameLength);
        memcpy(bodyPtr + resolverNameLength, hostname, (size_t)hostnameLength);

        if (NULL != address)
        {
            aeron_driver_agent_socket_address_copy(
                bodyPtr + resolverNameLength + hostnameLength,
                address,
                addressLength);
        }

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_name_resolver_on_lookup(
    aeron_name_resolver_t *name_resolver,
    int64_t duration_ns,
    const char *name,
    bool is_re_lookup,
    const char *resolved_name)
{
    const size_t resolverNameFullLength = strlen(name_resolver->name);
    const size_t resolverNameLength = resolverNameFullLength < AERON_MAX_HOSTNAME_LEN ?
        resolverNameFullLength : AERON_MAX_HOSTNAME_LEN;

    const size_t nameFullLength = strlen(name);
    const size_t nameLength = nameFullLength < AERON_MAX_HOSTNAME_LEN ? nameFullLength : AERON_MAX_HOSTNAME_LEN;

    const size_t resolvedNameFullLength = NULL != resolved_name ? strlen(resolved_name) : 0;
    const size_t resolvedNameLength = resolvedNameFullLength < AERON_MAX_HOSTNAME_LEN ?
        resolvedNameFullLength : AERON_MAX_HOSTNAME_LEN;

    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_NAME_RESOLUTION_LOOKUP,
        sizeof(aeron_driver_agent_name_resolver_lookup_log_header_t) +
            resolverNameLength +
            nameLength +
            resolvedNameLength);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_name_resolver_lookup_log_header_t *hdr =
            (aeron_driver_agent_name_resolver_lookup_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->duration_ns = duration_ns;
        hdr->resolver_name_length = (int32_t)resolverNameLength;
        hdr->name_length = (int32_t)nameLength;
        hdr->resolved_name_length = (int32_t)resolvedNameLength;
        hdr->is_re_lookup = is_re_lookup;

        uint8_t *bodyPtr = ptr + sizeof(aeron_driver_agent_name_resolver_lookup_log_header_t);
        memcpy(bodyPtr, name_resolver->name, (size_t)resolverNameLength);
        memcpy(bodyPtr + resolverNameLength, name, (size_t)nameLength);

        if (NULL != resolved_name)
        {
            memcpy(bodyPtr + resolverNameLength + nameLength, resolved_name, (size_t)resolvedNameLength);
        }

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_name_resolver_on_host_name(int64_t duration_ns, const char *host_name)
{
    const size_t hostNameFullLength = strlen(host_name);
    const size_t hostNameLength =
        hostNameFullLength < AERON_MAX_HOSTNAME_LEN ? hostNameFullLength : AERON_MAX_HOSTNAME_LEN;

    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_NAME_RESOLUTION_HOST_NAME,
        sizeof(aeron_driver_agent_name_resolver_host_name_log_header_t) + hostNameLength);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_name_resolver_host_name_log_header_t *hdr =
            (aeron_driver_agent_name_resolver_host_name_log_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->duration_ns = duration_ns;
        hdr->host_name_length = (int32_t)hostNameLength;

        uint8_t *bodyPtr = ptr + sizeof(aeron_driver_agent_name_resolver_host_name_log_header_t);
        memcpy(bodyPtr, host_name, (size_t)hostNameLength);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

int aeron_driver_agent_interceptor_init(
    void **interceptor_state, aeron_driver_context_t *context, aeron_udp_channel_transport_affinity_t affinity)
{
    return 0;
}

aeron_udp_channel_interceptor_bindings_t *aeron_driver_agent_new_interceptor(void)
{
    aeron_udp_channel_interceptor_bindings_t *interceptor = NULL;

    if (aeron_alloc((void **)&interceptor, sizeof(aeron_udp_channel_interceptor_bindings_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate interceptor bindings for logging agent");
        return NULL;
    }

    interceptor->incoming_init_func = aeron_driver_agent_interceptor_init;
    interceptor->incoming_close_func = NULL;
    interceptor->incoming_func = NULL;
    interceptor->incoming_transport_notification_func = NULL;
    interceptor->incoming_publication_notification_func = NULL;
    interceptor->incoming_image_notification_func = NULL;
    interceptor->outgoing_init_func = aeron_driver_agent_interceptor_init;
    interceptor->outgoing_close_func = NULL;
    interceptor->outgoing_send_func = NULL;
    interceptor->outgoing_transport_notification_func = NULL;
    interceptor->outgoing_publication_notification_func = NULL;
    interceptor->outgoing_image_notification_func = NULL;

    interceptor->meta_info.name = "logging";
    interceptor->meta_info.type = "interceptor";
    interceptor->meta_info.source_symbol = (aeron_fptr_t)aeron_driver_agent_context_init;
    interceptor->meta_info.next_interceptor_bindings = NULL;

    return interceptor;
}

int aeron_driver_agent_init_logging_events_interceptors(aeron_driver_context_t *context)
{
    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_IN))
    {
        aeron_udp_channel_interceptor_bindings_t *interceptors = context->udp_channel_incoming_interceptor_bindings;
        aeron_udp_channel_interceptor_bindings_t *first_interceptor = aeron_driver_agent_new_interceptor();
        if (NULL == first_interceptor)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        first_interceptor->incoming_func = aeron_driver_agent_incoming_msg;
        first_interceptor->meta_info.name = "incoming pre logging";
        first_interceptor->meta_info.next_interceptor_bindings = interceptors;
        context->udp_channel_incoming_interceptor_bindings = first_interceptor;

        if (NULL != interceptors)
        {
            aeron_udp_channel_interceptor_bindings_t *last_interceptor = aeron_driver_agent_new_interceptor();
            if (NULL == last_interceptor)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            last_interceptor->incoming_func = aeron_driver_agent_incoming_msg;
            last_interceptor->meta_info.name = "incoming post logging";

            while (NULL != interceptors->meta_info.next_interceptor_bindings)
            {
                interceptors = (aeron_udp_channel_interceptor_bindings_t *)interceptors->meta_info.next_interceptor_bindings;
            }
            interceptors->meta_info.next_interceptor_bindings = last_interceptor;
        }
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FRAME_OUT))
    {
        aeron_udp_channel_interceptor_bindings_t *interceptors = context->udp_channel_outgoing_interceptor_bindings;
        aeron_udp_channel_interceptor_bindings_t *first_interceptor = aeron_driver_agent_new_interceptor();
        if (NULL == first_interceptor)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        first_interceptor->outgoing_send_func = aeron_driver_agent_outgoing_send;
        first_interceptor->meta_info.name = "outgoing pre logging";
        first_interceptor->meta_info.next_interceptor_bindings = interceptors;
        context->udp_channel_outgoing_interceptor_bindings = first_interceptor;

        if (NULL != interceptors)
        {
            aeron_udp_channel_interceptor_bindings_t *last_interceptor = aeron_driver_agent_new_interceptor();
            if (NULL == last_interceptor)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }

            last_interceptor->outgoing_send_func = aeron_driver_agent_outgoing_send;
            last_interceptor->meta_info.name = "outgoing post logging";

            while (NULL != interceptors->meta_info.next_interceptor_bindings)
            {
                interceptors = (aeron_udp_channel_interceptor_bindings_t *)interceptors->meta_info.next_interceptor_bindings;
            }
            interceptors->meta_info.next_interceptor_bindings = last_interceptor;
        }
    }

    if (any_event_enabled(AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN))
    {
        context->log.to_driver_interceptor = aeron_driver_agent_conductor_to_driver_interceptor;
    }

    if (any_event_enabled(AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT))
    {
        context->log.to_client_interceptor = aeron_driver_agent_conductor_to_client_interceptor;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP))
    {
        context->log.remove_publication_cleanup = aeron_driver_agent_remove_publication_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_SUBSCRIPTION_CLEANUP))
    {
        context->log.remove_subscription_cleanup = aeron_driver_agent_remove_subscription_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_REMOVE_IMAGE_CLEANUP))
    {
        context->log.remove_image_cleanup = aeron_driver_agent_remove_image_cleanup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CREATION))
    {
        context->log.sender_proxy_on_add_endpoint = aeron_driver_agent_sender_proxy_on_add_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_SEND_CHANNEL_CLOSE))
    {
        context->log.sender_proxy_on_remove_endpoint = aeron_driver_agent_sender_proxy_on_remove_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CREATION))
    {
        context->log.receiver_proxy_on_add_endpoint = aeron_driver_agent_receiver_proxy_on_add_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RECEIVE_CHANNEL_CLOSE))
    {
        context->log.receiver_proxy_on_remove_endpoint = aeron_driver_agent_receiver_proxy_on_remove_endpoint;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE))
    {
        context->log.untethered_subscription_on_state_change =
            aeron_driver_agent_untethered_subscription_state_change;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_ADDED))
    {
        context->log.name_resolution_on_neighbor_added = aeron_driver_agent_name_resolution_on_neighbor_added;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_NEIGHBOR_REMOVED))
    {
        context->log.name_resolution_on_neighbor_removed = aeron_driver_agent_name_resolution_on_neighbor_removed;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_ADDED))
    {
        context->log.flow_control_on_receiver_added = aeron_driver_agent_flow_control_on_receiver_added;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_REMOVED))
    {
        context->log.flow_control_on_receiver_removed = aeron_driver_agent_flow_control_on_receiver_removed;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_RESOLVE))
    {
        context->log.on_name_resolve = aeron_driver_agent_name_resolver_on_resolve;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_LOOKUP))
    {
        context->log.on_name_lookup = aeron_driver_agent_name_resolver_on_lookup;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAME_RESOLUTION_HOST_NAME))
    {
        context->log.on_host_name = aeron_driver_agent_name_resolver_on_host_name;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAK_SENT))
    {
        context->log.send_nak_message = aeron_driver_agent_send_nak_message;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_NAK_RECEIVED))
    {
        context->log.on_nak_message = aeron_driver_agent_on_nak_message;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_RESEND))
    {
        context->log.resend = aeron_driver_agent_resend;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_PUBLICATION_REVOKE))
    {
        context->log.publication_revoke = aeron_driver_agent_publication_revoke;
    }

    if (aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_PUBLICATION_IMAGE_REVOKE))
    {
        context->log.publication_image_revoke = aeron_driver_agent_publication_image_revoke;
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
    int64_t time_ns,
    aeron_driver_agent_event_t event_id,
    size_t capture_length,
    size_t message_length)
{
    static char buffer[256];

    const int64_t seconds = time_ns / NANOS_PER_SECOND;
    const int64_t nanos = time_ns - seconds * NANOS_PER_SECOND;
    const char *event_name = aeron_driver_agent_event_name(event_id);
    snprintf(
        buffer,
        sizeof(buffer) - 1,
        "[%" PRIu64".%09" PRIu64 "] %s: %.*s [%" PRIu64 "/%" PRIu64 "]",
        seconds,
        nanos,
        AERON_DRIVER_AGENT_LOG_CONTEXT,
        AERON_DRIVER_AGENT_MAX_EVENT_NAME_LENGTH,
        event_name,
        (uint64_t)capture_length,
        (uint64_t)message_length);

    return buffer;
}

const char *aeron_driver_agent_dissect_log_start(int64_t time_ns, int64_t time_ms)
{
    static char buffer[512];
    static char datestamp[256];

    const int64_t seconds = time_ns / NANOS_PER_SECOND;
    const int64_t nanos = time_ns - seconds * NANOS_PER_SECOND;
    aeron_format_date(datestamp, sizeof(datestamp) - 1, time_ms);
    snprintf(buffer, sizeof(buffer) - 1, "[%" PRIu64".%09" PRIu64 "] log started %s, enabled loggers: {DRIVER: version=%s commit=%s}",
        seconds,
        nanos,
        datestamp,
        aeron_driver_version_text(),
        aeron_driver_version_git_sha());

    return buffer;
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
            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "streamId=%d clientId=%" PRId64 " correlationId=%" PRId64 " channel=%.*s",
                command->stream_id,
                command->correlated.client_id,
                command->correlated.correlation_id,
                command->channel_length,
                channel);
            break;
        }

        case AERON_COMMAND_REMOVE_PUBLICATION:
        {
            aeron_remove_publication_command_t *command = (aeron_remove_publication_command_t *)message;

            if (length >= sizeof(aeron_remove_publication_command_t))
            {
                snprintf(
                    buffer,
                    sizeof(buffer) - 1,
                    "registrationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64 " revoke=%s",
                    command->registration_id,
                    command->correlated.client_id,
                    command->correlated.correlation_id,
                    command->flags & AERON_COMMAND_REMOVE_PUBLICATION_FLAG_REVOKE ? "true" : "false");
            }
            else
            {
                snprintf(
                    buffer,
                    sizeof(buffer) - 1,
                    "registrationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64,
                    command->registration_id,
                    command->correlated.client_id,
                    command->correlated.correlation_id);
            }

            break;
        }

        case AERON_COMMAND_REMOVE_SUBSCRIPTION:
        {
            aeron_remove_subscription_command_t *command = (aeron_remove_subscription_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "registrationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64,
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);

            break;
        }

        case AERON_COMMAND_REMOVE_COUNTER:
        {
            aeron_remove_counter_command_t *command = (aeron_remove_counter_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "registrationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64,
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id);

            break;
        }

        case AERON_COMMAND_ADD_SUBSCRIPTION:
        {
            aeron_subscription_command_t *command = (aeron_subscription_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_subscription_command_t);
            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "streamId=%d registrationCorrelationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64 " channel=%.*s",
                command->stream_id,
                command->registration_correlation_id,
                command->correlated.client_id,
                command->correlated.correlation_id,
                command->channel_length,
                channel);
            break;
        }

        case AERON_COMMAND_CLIENT_KEEPALIVE:
        case AERON_COMMAND_CLIENT_CLOSE:
        {
            aeron_correlated_command_t *command = (aeron_correlated_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "clientId=%" PRId64 " correlationId=%" PRId64,
                command->client_id,
                command->correlation_id);
            break;
        }

        case AERON_COMMAND_ADD_DESTINATION:
        case AERON_COMMAND_REMOVE_DESTINATION:
        case AERON_COMMAND_ADD_RCV_DESTINATION:
        case AERON_COMMAND_REMOVE_RCV_DESTINATION:
        {
            aeron_destination_command_t *command = (aeron_destination_command_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_destination_command_t);
            snprintf(
                buffer,
                sizeof(buffer) - 1, "registrationCorrelationId=%" PRId64 " clientId=%" PRId64 " correlationId=%" PRId64 " channel=%.*s",
                command->registration_id,
                command->correlated.client_id,
                command->correlated.correlation_id,
                command->channel_length,
                channel);
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

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "ADD_COUNTER typeId=%d keyBufferOffset=%d keyBufferLength=%d labelBufferOffset=%d labelBufferLength=%d clientId=%" PRId64 " correlationId=%" PRId64,
                command->type_id,
                (int)(sizeof(aeron_counter_command_t) + sizeof(int32_t)),
                key_length,
                (int)(sizeof(aeron_counter_command_t) + (2 * sizeof(int32_t)) + AERON_ALIGN(key_length, sizeof(int32_t))),
                label_length,
                command->correlated.client_id,
                command->correlated.correlation_id);
            break;
        }

        case AERON_COMMAND_TERMINATE_DRIVER:
        {
            aeron_terminate_driver_command_t *command = (aeron_terminate_driver_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "clientId=%" PRId64 " tokenBufferLength=%d",
                command->correlated.client_id,
                command->token_length);
            break;
        }

        case AERON_COMMAND_REMOVE_DESTINATION_BY_ID:
        {
            aeron_destination_by_id_command_t *command = (aeron_destination_by_id_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "resourceRegistrationId=%" PRId64 "  destinationRegistrationId=%" PRId64,
                command->resource_registration_id,
                command->destination_registration_id);
            break;
        }

        case AERON_COMMAND_REJECT_IMAGE:
        {
            aeron_reject_image_command_t *command = (aeron_reject_image_command_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "clientId=%" PRId64 " correlationId=%" PRId64 " imageCorrelationId=%" PRId64 "position=%" PRId64 " reason=%.*s",
                command->correlated.client_id,
                command->correlated.correlation_id,
                command->image_correlation_id,
                command->position,
                command->reason_length,
                command->reason_text);
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

            snprintf(buffer, sizeof(buffer) - 1, "correlationId=%" PRId64, command->correlation_id);
            break;
        }

        case AERON_RESPONSE_ON_PUBLICATION_READY:
        case AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY:
        {
            aeron_publication_buffers_ready_t *command = (aeron_publication_buffers_ready_t *)message;

            const char *log_file_name = (const char *)message + sizeof(aeron_publication_buffers_ready_t);

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "sessionId=%d streamId=%d publicationLimitCounterId=%d channelStatusCounterId=%d correlationId=%" PRId64 " registrationId=%" PRId64 " logFileName=%.*s",
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

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "correlationId=%" PRId64 " channelStatusCounterId=%d",
                command->correlation_id,
                command->channel_status_indicator_id);
            break;
        }

        case AERON_RESPONSE_ON_ERROR:
        {
            aeron_error_response_t *command = (aeron_error_response_t *)message;

            const char *error_message = (const char *)message + sizeof(aeron_error_response_t);
            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "offendingCommandCorrelationId=%" PRId64 " errorCode=%s message=%.*s",
                command->offending_command_correlation_id,
                aeron_error_code_str(command->error_code),
                command->error_message_length,
                error_message);
            break;
        }

        case AERON_RESPONSE_ON_UNAVAILABLE_IMAGE:
        {
            aeron_image_message_t *command = (aeron_image_message_t *)message;

            const char *channel = (const char *)message + sizeof(aeron_image_message_t);
            snprintf(
                buffer,
                sizeof(buffer) - 1, "streamId=%d correlationId=%" PRId64 " subscriptionRegistrationId=%" PRId64 " channel=%.*s",
                command->stream_id,
                command->correlation_id,
                command->subscription_registration_id,
                command->channel_length,
                channel);
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
                "sessionId=%d streamId=%d subscriberPositionId=%" PRId32 " subscriptionRegistrationId=%" PRId64 " correlationId=%" PRId64 " sourceIdentity=%.*s logFileName=%.*s",
                command->session_id,
                command->stream_id,
                command->subscriber_position_id,
                command->subscriber_registration_id,
                command->correlation_id,
                source_identity_length,
                source_identity,
                log_file_name_length,
                log_file_name);
            break;
        }

        case AERON_RESPONSE_ON_COUNTER_READY:
        {
            aeron_counter_update_t *command = (aeron_counter_update_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "correlationId=%" PRId64 " counterId=%d",
                command->correlation_id,
                command->counter_id);
            break;
        }

        case AERON_RESPONSE_ON_CLIENT_TIMEOUT:
        {
            aeron_client_timeout_t *command = (aeron_client_timeout_t *)message;

            snprintf(buffer, sizeof(buffer) - 1, "clientId=%" PRId64, command->client_id);
            break;
        }

        default:
            break;
    }

    return buffer;
}

static char *dissect_flags(uint8_t flags, char *dissected_flags)
{
    const size_t len = 8;
    uint8_t flag_mask = (uint8_t)(1 << (len - 1));

    for (size_t i = 0; i < len; i++)
    {
        dissected_flags[i] = (flags & flag_mask) == flag_mask ? '1' : '0';
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

        case AERON_HDR_TYPE_RSP_SETUP:
            return "RSP_SETUP";

        default:
            return "unknown command";
    }
}

static const char *dissect_frame(const void *message, size_t length)
{
    static char buffer[256];
    static char dissected_flags[8] = { '0', '0', '0', '0', '0', '0', '0', '0' };
    aeron_frame_header_t *hdr = (aeron_frame_header_t *)message;

    buffer[0] = '\0';
    switch (hdr->type)
    {
        case AERON_HDR_TYPE_DATA:
        case AERON_HDR_TYPE_PAD:
        case AERON_HDR_TYPE_ATS_DATA:
        {
            aeron_data_header_t *data = (aeron_data_header_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d termId=%d termOffset=%d",
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d termId=%d termOffset=%d receiverWindowLength=%d receiverId=%" PRId64,
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d termId=%d termOffset=%d length=%d",
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d activeTermId=%d initialTermId=%d termOffset=%d termLength=%d mtu=%d ttl=%d",
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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

        case AERON_HDR_TYPE_RSP_SETUP:
        {
            aeron_response_setup_header_t *rsp_setup = (aeron_response_setup_header_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d responseSessionId=%d",
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
                hdr->frame_length,
                rsp_setup->session_id,
                rsp_setup->stream_id,
                rsp_setup->response_session_id);
            break;
        }

        case AERON_HDR_TYPE_RTTM:
        {
            aeron_rttm_header_t *rttm = (aeron_rttm_header_t *)message;

            snprintf(
                buffer,
                sizeof(buffer) - 1,
                "type=%s flags=%.*s frameLength=%d sessionId=%d streamId=%d echoTimestampNs=%" PRId64 " receptionDelta=%" PRId64 " receiverId=%" PRId64,
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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
            const uint8_t *message_bytes = (uint8_t *)message;
            const int buffer_available = sizeof(buffer) - 1;

            int buffer_used = snprintf(
                buffer,
                buffer_available,
                "type=%s flags=%.*s frameLength=%d",
                dissect_frame_type(hdr->type),
                (int)sizeof(dissected_flags),
                dissect_flags(hdr->flags, dissected_flags),
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
                    " [resType=%" PRId8 " flags=%.*s port=%" PRIu16 " ageInMs=%" PRId32 " address=%s name=%.*s]",
                    res->res_type,
                    (int)sizeof(dissected_flags),
                    dissect_flags(res->res_flags, dissected_flags),
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

        case AERON_SUBSCRIPTION_TETHER_CLOSED:
            return "CLOSED";

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
            struct sockaddr_storage *addr =
                (struct sockaddr_storage *)((const char *)message + sizeof(aeron_driver_agent_frame_log_header_t));
            const char *frame =
                (const char *)message + sizeof(aeron_driver_agent_frame_log_header_t) + hdr->sockaddr_len;
            char addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            const int addr_length = aeron_format_source_identity(addr_buf, sizeof(addr_buf), addr);

            fprintf(
                logfp,
                "%s: address=%.*s %s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, (size_t)hdr->message_len),
                addr_length,
                addr_buf,
                dissect_frame(frame, (size_t)hdr->message_len));
            break;
        }

        case AERON_DRIVER_EVENT_REMOVE_PUBLICATION_CLEANUP:
        {
            aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_remove_resource_cleanup_t);
            fprintf(
                logfp,
                "%s: sessionId=%d streamId=%d channel=%.*s\n",
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
                "%s: streamId=%d id=%" PRId64 " channel=%.*s\n",
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
                "%s: sessionId=%d streamId=%d id=%" PRId64 " channel=%.*s\n",
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
            char local_addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            const int local_addr_length = aeron_format_source_identity(
                local_addr_buf, sizeof(local_addr_buf), &hdr->local_data);
            char remote_addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            const int remote_addr_length = aeron_format_source_identity(
                remote_addr_buf, sizeof(remote_addr_buf), &hdr->remote_data);

            fprintf(
                logfp,
                "%s: localData: %.*s, remoteData: %.*s, ttl: %d\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                local_addr_length,
                local_addr_buf,
                remote_addr_length,
                remote_addr_buf,
                hdr->multicast_ttl);
            break;
        }

        case AERON_DRIVER_EVENT_UNTETHERED_SUBSCRIPTION_STATE_CHANGE:
        {
            aeron_driver_agent_untethered_subscription_state_change_log_header_t *hdr =
                (aeron_driver_agent_untethered_subscription_state_change_log_header_t *)message;

            fprintf(
                logfp,
                "%s: subscriptionId=%" PRId64 " streamId=%d sessionId=%d %s -> %s\n",
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
            struct sockaddr_storage *addr =
                (struct sockaddr_storage *)((const char *)message + sizeof(aeron_driver_agent_log_header_t));
            char addr_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            const int addr_length = aeron_format_source_identity(addr_buf, sizeof(addr_buf), addr);

            fprintf(
                logfp,
                "%s: %.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                addr_length,
                addr_buf);
            break;
        }

        case AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_ADDED:
        case AERON_DRIVER_EVENT_FLOW_CONTROL_RECEIVER_REMOVED:
        {
            aeron_driver_agent_flow_control_receiver_change_log_header_t *hdr = (aeron_driver_agent_flow_control_receiver_change_log_header_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_flow_control_receiver_change_log_header_t);
            fprintf(
                logfp,
                "%s: receiverCount=%d receiverId=%" PRId64 " sessionId=%d streamId=%d channel=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->receiver_count,
                hdr->receiver_id,
                hdr->session_id,
                hdr->stream_id,
                hdr->channel_length,
                channel);
            break;
        }

        case AERON_DRIVER_EVENT_NAK_SENT:
        case AERON_DRIVER_EVENT_NAK_RECEIVED:
        {
            aeron_driver_agent_nak_message_header_t *hdr = (aeron_driver_agent_nak_message_header_t *)message;
            char address_buf[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
            const int address_length = aeron_format_source_identity(address_buf, sizeof(address_buf), &hdr->address);
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_nak_message_header_t);

            fprintf(
                logfp,
                "%s: address=%.*s sessionId=%d streamId=%d termId=%d termOffset=%d length=%d channel=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                address_length,
                address_buf,
                hdr->session_id,
                hdr->stream_id,
                hdr->term_id,
                hdr->term_offset,
                hdr->nak_length,
                hdr->channel_length,
                channel);

            break;
        }

        case AERON_DRIVER_EVENT_RESEND:
        {
            aeron_driver_agent_resend_header_t *hdr = (aeron_driver_agent_resend_header_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_resend_header_t);

            fprintf(
                logfp,
                "%s: sessionId=%d streamId=%d termId=%d termOffset=%d length=%d channel=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->session_id,
                hdr->stream_id,
                hdr->term_id,
                hdr->term_offset,
                hdr->resend_length,
                hdr->channel_length,
                channel);

            break;
        }

        case AERON_DRIVER_EVENT_PUBLICATION_REVOKE:
        {
            aeron_driver_agent_publication_revoke_header_t *hdr = (aeron_driver_agent_publication_revoke_header_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_publication_revoke_header_t);

            fprintf(
                logfp,
                "%s: revokedPos=%" PRIu64 "sessionId=%d streamId=%d channel=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->revoked_pos,
                hdr->session_id,
                hdr->stream_id,
                hdr->channel_length,
                channel);

            break;
        }

        case AERON_DRIVER_EVENT_PUBLICATION_IMAGE_REVOKE:
        {
            aeron_driver_agent_publication_image_revoke_header_t *hdr = (aeron_driver_agent_publication_image_revoke_header_t *)message;
            const char *channel = (const char *)message + sizeof(aeron_driver_agent_publication_image_revoke_header_t);

            fprintf(
                logfp,
                "%s: revokedPos=%" PRIu64 "sessionId=%d streamId=%d channel=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->revoked_pos,
                hdr->session_id,
                hdr->stream_id,
                hdr->channel_length,
                channel);

            break;
        }

        case AERON_DRIVER_EVENT_NAME_RESOLUTION_RESOLVE:
        {
            aeron_driver_agent_name_resolver_resolve_log_header_t *hdr =
                (aeron_driver_agent_name_resolver_resolve_log_header_t *)message;
            char *address_str = "unknown-address";
            char address_buf[INET6_ADDRSTRLEN] = { 0 };
            uint8_t *resolver_name_ptr =
                (uint8_t *)message + sizeof(aeron_driver_agent_name_resolver_resolve_log_header_t);
            uint8_t *hostname_ptr = resolver_name_ptr + hdr->resolver_name_length;
            uint8_t *address_ptr = hostname_ptr + hdr->hostname_length;

            const char *addr_prefix = "";
            const char *addr_suffix = "";
            if (sizeof(((struct sockaddr_in *)0)->sin_addr) == hdr->address_length)
            {
                inet_ntop(AF_INET, address_ptr, address_buf, sizeof(address_buf));
                address_str = address_buf;
            }
            else if (sizeof(((struct sockaddr_in6 *)0)->sin6_addr) == hdr->address_length)
            {
                addr_prefix = "[";
                addr_suffix = "]";

                inet_ntop(AF_INET, address_ptr, address_buf, sizeof(address_buf));
                address_str = address_buf;
            }

            fprintf(
                logfp,
                "%s: resolver=%.*s durationNs=%" PRIu64 " name=%.*s isReResolution=%s address=%s%s%s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                (int)hdr->resolver_name_length,
                resolver_name_ptr,
                (uint64_t)hdr->duration_ns,
                (int)hdr->hostname_length,
                hostname_ptr,
                hdr->is_re_resolution ? "true" : "false",
                addr_prefix,
                address_str,
                addr_suffix);
            break;
        }

        case AERON_DRIVER_EVENT_NAME_RESOLUTION_LOOKUP:
        {
            aeron_driver_agent_name_resolver_lookup_log_header_t *hdr =
                (aeron_driver_agent_name_resolver_lookup_log_header_t *)message;
            uint8_t *resolver_name_ptr =
                (uint8_t *)message + sizeof(aeron_driver_agent_name_resolver_lookup_log_header_t);
            uint8_t *name_ptr = resolver_name_ptr + hdr->resolver_name_length;
            uint8_t *resolved_name_ptr = name_ptr + hdr->name_length;

            fprintf(
                logfp,
                "%s: resolver=%.*s durationNs=%" PRIu64 " name=%.*s isReLookup=%s resolvedName=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                (int)hdr->resolver_name_length,
                resolver_name_ptr,
                (uint64_t)hdr->duration_ns,
                (int)hdr->name_length,
                name_ptr,
                hdr->is_re_lookup ? "true" : "false",
                (int)hdr->resolved_name_length,
                resolved_name_ptr);
            break;
        }

        case AERON_DRIVER_EVENT_NAME_RESOLUTION_HOST_NAME:
        {
            aeron_driver_agent_name_resolver_host_name_log_header_t *hdr =
                (aeron_driver_agent_name_resolver_host_name_log_header_t *)message;
            uint8_t *host_name_name_ptr =
                (uint8_t *)message + sizeof(aeron_driver_agent_name_resolver_host_name_log_header_t);

            fprintf(
                logfp,
                "%s: durationNs=%" PRIu64 " hostName=%.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                (uint64_t)hdr->duration_ns,
                (int)hdr->host_name_length,
                host_name_name_ptr);
            break;
        }

        case AERON_DRIVER_EVENT_NAME_TEXT_DATA:
        {
            aeron_driver_agent_text_data_log_header_t *hdr =
                (aeron_driver_agent_text_data_log_header_t *)message;
            uint8_t *message_ptr =
                (uint8_t *)message + sizeof(aeron_driver_agent_text_data_log_header_t);

            fprintf(
                logfp,
                "%s: %.*s\n",
                aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                hdr->message_length,
                message_ptr);
            break;
        }

        case AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR:
        {
            aeron_driver_agent_add_dissector_header_t *hdr = (aeron_driver_agent_add_dissector_header_t *)message;

            if (aeron_array_ensure_capacity(
                (uint8_t **)&dynamic_dissector_entries,
                sizeof(aeron_driver_agent_dynamic_dissector_entry_t),
                num_dynamic_dissector_entries,
                hdr->index + 1) >= 0)
            {
                dynamic_dissector_entries[hdr->index].dissector_func = hdr->dissector_func;
                num_dynamic_dissector_entries = hdr->index + 1;
            }
            break;
        }

        case AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT:
        {
            aeron_driver_agent_dynamic_event_header_t *hdr = (aeron_driver_agent_dynamic_event_header_t *)message;

            if (hdr->index >= 0 && hdr->index < (int64_t)num_dynamic_dissector_entries &&
                NULL != dynamic_dissector_entries[hdr->index].dissector_func)
            {
                dynamic_dissector_entries[hdr->index].dissector_func(
                    logfp,
                    aeron_driver_agent_dissect_log_header(hdr->time_ns, msg_type_id, length, length),
                    message,
                    length);
            }
            break;
        }

        default:
            if (is_valid_event_id(msg_type_id))
            {
                const uint8_t event_type = log_events[msg_type_id].type;
                if (AERON_DRIVER_AGENT_EVENT_TYPE_CMD_IN == event_type)
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
                else if (AERON_DRIVER_AGENT_EVENT_TYPE_CMD_OUT == event_type)
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

static void log_remove_resource_cleanup(
    const int64_t id,
    const int32_t session_id,
    const int32_t stream_id,
    const size_t channel_length,
    const char *channel,
    const aeron_driver_agent_event_t event_id)
{
    const size_t command_length = sizeof(aeron_driver_agent_remove_resource_cleanup_t) + channel_length;
    int32_t offset = aeron_mpsc_rb_try_claim(&logging_mpsc_rb, event_id, command_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_remove_resource_cleanup_t *hdr = (aeron_driver_agent_remove_resource_cleanup_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->id = id;
        hdr->stream_id = stream_id;
        hdr->session_id = session_id;
        hdr->channel_length = (int32_t)channel_length;

        memcpy(ptr + sizeof(aeron_driver_agent_remove_resource_cleanup_t), channel, channel_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}

void aeron_driver_agent_remove_publication_cleanup(
    int32_t session_id, int32_t stream_id, size_t channel_length, const char *channel)
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
    int64_t id, int32_t stream_id, size_t channel_length, const char *channel)
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
    int64_t id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
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
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb, event_id, sizeof(aeron_driver_agent_on_endpoint_change_t));

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_on_endpoint_change_t *hdr = (aeron_driver_agent_on_endpoint_change_t *)ptr;

        const aeron_udp_channel_t *udp_channel = channel;

        hdr->time_ns = aeron_nano_clock();
        memcpy(&hdr->local_data, &udp_channel->local_data, AERON_ADDR_LEN(&udp_channel->local_data));
        memcpy(&hdr->remote_data, &udp_channel->remote_data, AERON_ADDR_LEN(&udp_channel->remote_data));
        hdr->multicast_ttl = udp_channel->multicast_ttl;

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
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

int64_t aeron_driver_agent_add_dynamic_dissector(aeron_driver_agent_generic_dissector_func_t func)
{
    if (!aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT))
    {
        return -1;
    }

    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_ADD_DYNAMIC_DISSECTOR,
        sizeof(aeron_driver_agent_add_dissector_header_t));

    if (offset > 0)
    {

        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);

        aeron_driver_agent_add_dissector_header_t *hdr =
            (aeron_driver_agent_add_dissector_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        AERON_GET_AND_ADD_INT64(hdr->index, dynamic_dissector_index, INT64_C(1));
        hdr->dissector_func = func;

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
        return hdr->index;
    }

    return -1;
}

void aeron_driver_agent_log_dynamic_event(int64_t index, const void *message, size_t length)
{
    if (!aeron_driver_agent_is_event_enabled(AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT))
    {
        return;
    }

    const size_t copy_length = length < AERON_MAX_FRAME_LENGTH ? length : AERON_MAX_FRAME_LENGTH;
    int32_t offset = aeron_mpsc_rb_try_claim(
        &logging_mpsc_rb,
        AERON_DRIVER_EVENT_DYNAMIC_DISSECTOR_EVENT,
        sizeof(aeron_driver_agent_dynamic_event_header_t) + copy_length);

    if (offset > 0)
    {
        uint8_t *ptr = (logging_mpsc_rb.buffer + offset);
        aeron_driver_agent_dynamic_event_header_t *hdr = (aeron_driver_agent_dynamic_event_header_t *)ptr;

        hdr->time_ns = aeron_nano_clock();
        hdr->index = index;
        memcpy(ptr + sizeof(aeron_driver_agent_dynamic_event_header_t), message, copy_length);

        aeron_mpsc_rb_commit(&logging_mpsc_rb, offset);
    }
}
