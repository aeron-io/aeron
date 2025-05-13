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

#ifndef AERON_CONTROL_PROTOCOL_H
#define AERON_CONTROL_PROTOCOL_H

#include <stdint.h>

#define AERON_COMMAND_ADD_PUBLICATION (0x01)
#define AERON_COMMAND_REMOVE_PUBLICATION (0x02)
#define AERON_COMMAND_ADD_EXCLUSIVE_PUBLICATION (0x03)

#define AERON_COMMAND_ADD_SUBSCRIPTION (0x04)
#define AERON_COMMAND_REMOVE_SUBSCRIPTION (0x05)
#define AERON_COMMAND_CLIENT_KEEPALIVE (0x06)
#define AERON_COMMAND_ADD_DESTINATION (0x07)
#define AERON_COMMAND_REMOVE_DESTINATION (0x08)
#define AERON_COMMAND_ADD_COUNTER (0x09)
#define AERON_COMMAND_REMOVE_COUNTER (0x0A)
#define AERON_COMMAND_CLIENT_CLOSE (0x0B)
#define AERON_COMMAND_ADD_RCV_DESTINATION (0x0C)
#define AERON_COMMAND_REMOVE_RCV_DESTINATION (0x0D)
#define AERON_COMMAND_TERMINATE_DRIVER (0x0E)
#define AERON_COMMAND_ADD_STATIC_COUNTER (0x0F)
#define AERON_COMMAND_REJECT_IMAGE (0x10)
#define AERON_COMMAND_REMOVE_DESTINATION_BY_ID (0x11)

#define AERON_RESPONSE_ON_ERROR (0x0F01)
#define AERON_RESPONSE_ON_AVAILABLE_IMAGE (0x0F02)
#define AERON_RESPONSE_ON_PUBLICATION_READY (0x0F03)
#define AERON_RESPONSE_ON_OPERATION_SUCCESS (0x0F04)
#define AERON_RESPONSE_ON_UNAVAILABLE_IMAGE (0x0F05)
#define AERON_RESPONSE_ON_EXCLUSIVE_PUBLICATION_READY (0x0F06)
#define AERON_RESPONSE_ON_SUBSCRIPTION_READY (0x0F07)
#define AERON_RESPONSE_ON_COUNTER_READY (0x0F08)
#define AERON_RESPONSE_ON_UNAVAILABLE_COUNTER (0x0F09)
#define AERON_RESPONSE_ON_CLIENT_TIMEOUT (0x0F0A)
#define AERON_RESPONSE_ON_STATIC_COUNTER (0x0F0B)
#define AERON_RESPONSE_ON_PUBLICATION_ERROR (0x0F0C)

/* error codes */
#define AERON_ERROR_CODE_UNKNOWN_CODE_VALUE (-1)
#define AERON_ERROR_CODE_UNUSED (0)
#define AERON_ERROR_CODE_INVALID_CHANNEL (1)
#define AERON_ERROR_CODE_UNKNOWN_SUBSCRIPTION (2)
#define AERON_ERROR_CODE_UNKNOWN_PUBLICATION (3)
#define AERON_ERROR_CODE_CHANNEL_ENDPOINT_ERROR (4)
#define AERON_ERROR_CODE_UNKNOWN_COUNTER (5)
#define AERON_ERROR_CODE_UNKNOWN_COMMAND_TYPE_ID (6)
#define AERON_ERROR_CODE_MALFORMED_COMMAND (7)
#define AERON_ERROR_CODE_NOT_SUPPORTED (8)
#define AERON_ERROR_CODE_UNKNOWN_HOST (9)
#define AERON_ERROR_CODE_RESOURCE_TEMPORARILY_UNAVAILABLE (10)
#define AERON_ERROR_CODE_GENERIC_ERROR (11)
#define AERON_ERROR_CODE_STORAGE_SPACE (12)
#define AERON_ERROR_CODE_IMAGE_REJECTED (13)
#define AERON_ERROR_CODE_PUBLICATION_REVOKED (14)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_correlated_command_stct
{
    int64_t client_id;
    int64_t correlation_id;
}
aeron_correlated_command_t;

typedef struct aeron_publication_command_stct
{
    aeron_correlated_command_t correlated;
    int32_t stream_id;
    int32_t channel_length;
}
aeron_publication_command_t;

typedef struct aeron_publication_buffers_ready_stct
{
    int64_t correlation_id;
    int64_t registration_id;
    int32_t session_id;
    int32_t stream_id;
    int32_t position_limit_counter_id;
    int32_t channel_status_indicator_id;
    int32_t log_file_length;
}
aeron_publication_buffers_ready_t;

typedef struct aeron_subscription_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_correlation_id;
    int32_t stream_id;
    int32_t channel_length;
}
aeron_subscription_command_t;

typedef struct aeron_subscription_ready_stct
{
    int64_t correlation_id;
    int32_t channel_status_indicator_id;
}
aeron_subscription_ready_t;

typedef struct aeron_image_buffers_ready_stct
{
    int64_t correlation_id;
    int32_t session_id;
    int32_t stream_id;
    int64_t subscriber_registration_id;
    int32_t subscriber_position_id;
}
aeron_image_buffers_ready_t;

typedef struct aeron_operation_succeeded_stct
{
    int64_t correlation_id;
}
aeron_operation_succeeded_t;

typedef struct aeron_error_response_stct
{
    int64_t offending_command_correlation_id;
    int32_t error_code;
    int32_t error_message_length;
}
aeron_error_response_t;

typedef struct aeron_remove_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_id;
}
aeron_remove_command_t;

typedef struct aeron_image_message_stct
{
    int64_t correlation_id;
    int64_t subscription_registration_id;
    int32_t stream_id;
    int32_t channel_length;
}
aeron_image_message_t;

typedef struct aeron_destination_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_id;
    int32_t channel_length;
}
aeron_destination_command_t;

typedef struct aeron_destination_by_id_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t resource_registration_id;
    int64_t destination_registration_id;
}
aeron_destination_by_id_command_t;

typedef struct aeron_counter_command_stct
{
    aeron_correlated_command_t correlated;
    int32_t type_id;
}
aeron_counter_command_t;

typedef struct aeron_counter_update_stct
{
    int64_t correlation_id;
    int32_t counter_id;
}
aeron_counter_update_t;

typedef struct aeron_static_counter_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t registration_id;
    int32_t type_id;
}
aeron_static_counter_command_t;

typedef struct aeron_static_counter_response_stct
{
    int64_t correlation_id;
    int32_t counter_id;
}
aeron_static_counter_response_t;

typedef struct aeron_client_timeout_stct
{
    int64_t client_id;
}
aeron_client_timeout_t;

typedef struct aeron_terminate_driver_command_stct
{
    aeron_correlated_command_t correlated;
    int32_t token_length;
}
aeron_terminate_driver_command_t;

typedef struct aeron_reject_image_command_stct
{
    aeron_correlated_command_t correlated;
    int64_t image_correlation_id;
    int64_t position;
    int32_t reason_length;
    uint8_t reason_text[1];
}
aeron_reject_image_command_t;

struct aeron_publication_error_stct
{
    int64_t registration_id;
    int64_t destination_registration_id;
    int32_t session_id;
    int32_t stream_id;
    int64_t receiver_id;
    int64_t group_tag;
    int16_t address_type;
    uint16_t source_port;
    uint8_t source_address[16];
    int32_t error_code;
    int32_t error_message_length;
    uint8_t error_message[1];
};
typedef struct aeron_publication_error_stct aeron_publication_error_t;

#pragma pack(pop)

#endif //AERON_CONTROL_PROTOCOL_H
