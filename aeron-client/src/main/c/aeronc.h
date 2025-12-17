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

#ifndef AERON_C_H
#define AERON_C_H

#ifdef __cplusplus
extern "C"
{
#endif

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define AERON_NULL_VALUE (-1)

#define AERON_CLIENT_ERROR_DRIVER_TIMEOUT (-1000)
#define AERON_CLIENT_ERROR_CLIENT_TIMEOUT (-1001)
#define AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT (-1002)
#define AERON_CLIENT_ERROR_BUFFER_FULL (-1003)
#define AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN (64)

#define AERON_RESPONSE_ADDRESS_TYPE_IPV4 (0x1)
#define AERON_RESPONSE_ADDRESS_TYPE_IPV6 (0x2)

typedef struct aeron_context_stct aeron_context_t;
typedef struct aeron_stct aeron_t;
typedef struct aeron_buffer_claim_stct aeron_buffer_claim_t;
typedef struct aeron_publication_stct aeron_publication_t;
typedef struct aeron_exclusive_publication_stct aeron_exclusive_publication_t;
typedef struct aeron_header_stct aeron_header_t;

#pragma pack(push)
#pragma pack(4)
struct aeron_header_values_frame_stct
{
    int32_t frame_length;
    int8_t version;
    uint8_t flags;
    int16_t type;
    int32_t term_offset;
    int32_t session_id;
    int32_t stream_id;
    int32_t term_id;
    int64_t reserved_value;
};
typedef struct aeron_header_values_frame_stct aeron_header_values_frame_t;

typedef struct aeron_header_values_stct
{
    aeron_header_values_frame_t frame;
    int32_t initial_term_id;
    size_t position_bits_to_shift;
}
aeron_header_values_t;

struct aeron_publication_error_values_stct
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
typedef struct aeron_publication_error_values_stct aeron_publication_error_values_t;
#pragma pack(pop)

typedef struct aeron_subscription_stct aeron_subscription_t;
typedef struct aeron_image_stct aeron_image_t;
typedef struct aeron_counter_stct aeron_counter_t;
typedef struct aeron_log_buffer_stct aeron_log_buffer_t;

typedef struct aeron_counters_reader_stct aeron_counters_reader_t;

typedef struct aeron_client_registering_resource_stct aeron_async_add_publication_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_exclusive_publication_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_subscription_t;
typedef struct aeron_client_registering_resource_stct aeron_async_add_counter_t;
typedef struct aeron_client_registering_resource_stct aeron_async_destination_t;
typedef struct aeron_client_registering_resource_stct aeron_async_destination_by_id_t;
typedef struct aeron_client_registering_resource_stct aeron_async_get_next_available_session_id_t;

typedef struct aeron_image_fragment_assembler_stct aeron_image_fragment_assembler_t;
typedef struct aeron_image_controlled_fragment_assembler_stct aeron_image_controlled_fragment_assembler_t;
typedef struct aeron_fragment_assembler_stct aeron_fragment_assembler_t;
typedef struct aeron_controlled_fragment_assembler_stct aeron_controlled_fragment_assembler_t;


/**
 * Environment variables and functions used for setting values of an aeron_context_t.
 */

/**
 * The top level Aeron directory used for communication between a Media Driver and client.
 */
#define AERON_DIR_ENV_VAR "AERON_DIR"

int aeron_context_set_dir(aeron_context_t *context, const char *value);
const char *aeron_context_get_dir(aeron_context_t *context);

#define AERON_DRIVER_TIMEOUT_ENV_VAR "AERON_DRIVER_TIMEOUT"

int aeron_context_set_driver_timeout_ms(aeron_context_t *context, uint64_t value);
uint64_t aeron_context_get_driver_timeout_ms(aeron_context_t *context);

int aeron_context_set_keepalive_interval_ns(aeron_context_t *context, uint64_t value);
uint64_t aeron_context_get_keepalive_interval_ns(aeron_context_t *context);

#define AERON_CLIENT_RESOURCE_LINGER_DURATION_ENV_VAR "AERON_CLIENT_RESOURCE_LINGER_DURATION"

int aeron_context_set_resource_linger_duration_ns(aeron_context_t *context, uint64_t value);
uint64_t aeron_context_get_resource_linger_duration_ns(aeron_context_t *context);

#define AERON_CLIENT_IDLE_SLEEP_DURATION_ENV_VAR "AERON_CLIENT_IDLE_SLEEP_DURATION"

uint64_t aeron_context_get_idle_sleep_duration_ns(aeron_context_t *context);
int aeron_context_set_idle_sleep_duration_ns(aeron_context_t *context, uint64_t value);

#define AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY_ENV_VAR "AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY"

int aeron_context_set_pre_touch_mapped_memory(aeron_context_t *context, bool value);
bool aeron_context_get_pre_touch_mapped_memory(aeron_context_t *context);

#define AERON_CLIENT_NAME_ENV_VAR "AERON_CLIENT_NAME"

int aeron_context_set_client_name(aeron_context_t *context, const char *value);
const char *aeron_context_get_client_name(aeron_context_t *context);

/**
 * The error handler to be called when an error occurs.
 */
typedef void (*aeron_error_handler_t)(void *clientd, int errcode, const char *message);

/**
 * The error frame handler to be called when the driver notifies the client about an error frame being received.
 * The data passed to this callback will only be valid for the lifetime of the callback. The user should use
 * <code>aeron_publication_error_values_copy</code> if they require the data to live longer than that.
 */
typedef void (*aeron_publication_error_frame_handler_t)(void *clientd, aeron_publication_error_values_t *error_frame);

/**
 * Copy an existing aeron_publication_error_values_t to the supplied pointer. The caller is responsible for freeing the
 * allocated memory using aeron_publication_error_values_delete when the copy is not longer required.
 *
 * @param dst to copy the values to.
 * @param src to copy the values from.
 * @return 0 if this is successful, -1 otherwise. Will set aeron_errcode() and aeron_errmsg() on failure.
 */
int aeron_publication_error_values_copy(aeron_publication_error_values_t **dst, aeron_publication_error_values_t *src);

/**
 * Delete a instance of aeron_publication_error_values_t that was created when making a copy
 * (aeron_publication_error_values_copy). This should not be use on the pointer received via the aeron_frame_handler_t.
 * @param to_delete to be deleted.
 */
void aeron_publication_error_values_delete(aeron_publication_error_values_t *to_delete);

/**
 * Generalised notification callback.
 */
typedef void (*aeron_notification_t)(void *clientd);

int aeron_context_set_error_handler(aeron_context_t *context, aeron_error_handler_t handler, void *clientd);
aeron_error_handler_t aeron_context_get_error_handler(aeron_context_t *context);
void *aeron_context_get_error_handler_clientd(aeron_context_t *context);

int aeron_context_set_publication_error_frame_handler(aeron_context_t *context, aeron_publication_error_frame_handler_t handler, void *clientd);
aeron_publication_error_frame_handler_t aeron_context_get_publication_error_frame_handler(aeron_context_t *context);
void *aeron_context_get_publication_error_frame_handler_clientd(aeron_context_t *context);

/**
 * Function called by aeron_client_t to deliver notification that the media driver has added an aeron_publication_t
 * or aeron_exclusive_publication_t successfully.
 *
 * Implementations should do the minimum work for passing off state to another thread for later processing.
 *
 * @param clientd to be returned in the call
 * @param async associated with the original add publication call
 * @param channel of the publication
 * @param stream_id within the channel of the publication
 * @param session_id of the publication
 * @param correlation_id used by the publication
 */
typedef void (*aeron_on_new_publication_t)(
    void *clientd,
    aeron_async_add_publication_t *async,
    const char *channel,
    int32_t stream_id,
    int32_t session_id,
    int64_t correlation_id);

int aeron_context_set_on_new_publication(aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd);
aeron_on_new_publication_t aeron_context_get_on_new_publication(aeron_context_t *context);
void *aeron_context_get_on_new_publication_clientd(aeron_context_t *context);

int aeron_context_set_on_new_exclusive_publication(
    aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd);
aeron_on_new_publication_t aeron_context_get_on_new_exclusive_publication(aeron_context_t *context);
void *aeron_context_get_on_new_exclusive_publication_clientd(aeron_context_t *context);

/**
 * Function called by aeron_client_t to deliver notification that the media driver has added an aeron_subscription_t
 * successfully.
 *
 * Implementations should do the minimum work for handing off state to another thread for later processing.
 *
 * @param clientd to be returned in the call
 * @param async associated with the original aeron_add_async_subscription call
 * @param channel of the subscription
 * @param stream_id within the channel of the subscription
 * @param session_id of the subscription
 * @param correlation_id used by the subscription
 */
typedef void (*aeron_on_new_subscription_t)(
    void *clientd,
    aeron_async_add_subscription_t *async,
    const char *channel,
    int32_t stream_id,
    int64_t correlation_id);

int aeron_context_set_on_new_subscription(
    aeron_context_t *context, aeron_on_new_subscription_t handler, void *clientd);
aeron_on_new_subscription_t aeron_context_get_on_new_subscription(aeron_context_t *context);
void *aeron_context_get_on_new_subscription_clientd(aeron_context_t *context);

/**
 * Function called by aeron_client_t to deliver notifications that an aeron_image_t was added.
 *
 * @param clientd to be returned in the call.
 * @param subscription that image is part of.
 * @param image that has become available.
 */
typedef void (*aeron_on_available_image_t)(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);

/**
 * Function called by aeron_client_t to deliver notifications that an aeron_image_t has been removed from use and
 * should not be used any longer.
 *
 * @param clientd to be returned in the call.
 * @param subscription that image is part of.
 * @param image that has become unavailable.
 */
typedef void (*aeron_on_unavailable_image_t)(void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);

/**
 * Function called by aeron_client_t to deliver notifications that a counter has been added to the driver.
 *
 * @param clientd to be returned in the call.
 * @param counters_reader that holds the counter.
 * @param registration_id of the counter.
 * @param counter_id of the counter.
 */
typedef void (*aeron_on_available_counter_t)(
    void *clientd, aeron_counters_reader_t *counters_reader, int64_t registration_id, int32_t counter_id);

int aeron_context_set_on_available_counter(
    aeron_context_t *context, aeron_on_available_counter_t handler, void *clientd);
aeron_on_available_counter_t aeron_context_get_on_available_counter(aeron_context_t *context);
void *aeron_context_get_on_available_counter_clientd(aeron_context_t *context);

/**
 * Function called by aeron_client_t to deliver notifications that a counter has been removed from the driver.
 *
 * @param clientd to be returned in the call.
 * @param counters_reader that holds the counter.
 * @param registration_id of the counter.
 * @param counter_id of the counter.
 */
typedef void (*aeron_on_unavailable_counter_t)(
    void *clientd, aeron_counters_reader_t *counters_reader, int64_t registration_id, int32_t counter_id);

int aeron_context_set_on_unavailable_counter(
    aeron_context_t *context, aeron_on_unavailable_counter_t handler, void *clientd);
aeron_on_unavailable_counter_t aeron_context_get_on_unavailable_counter(aeron_context_t *context);
void *aeron_context_get_on_unavailable_counter_clientd(aeron_context_t *context);

/**
 * Function called by aeron_client_t to deliver notifications that the client is closing.
 *
 * @param clientd to be returned in the call.
 */
typedef void (*aeron_on_close_client_t)(void *clientd);

int aeron_context_set_on_close_client(
    aeron_context_t *context, aeron_on_close_client_t handler, void *clientd);
aeron_on_close_client_t aeron_context_get_on_close_client(aeron_context_t *context);
void *aeron_context_get_on_close_client_clientd(aeron_context_t *context);

/**
 * Whether to use an invoker to control the conductor agent or spawn a thread.
 */
int aeron_context_set_use_conductor_agent_invoker(aeron_context_t *context, bool value);
bool aeron_context_get_use_conductor_agent_invoker(aeron_context_t *context);

/**
 * Function name to call on start of each agent.
 */
#define AERON_AGENT_ON_START_FUNCTION_ENV_VAR "AERON_AGENT_ON_START_FUNCTION"

typedef void (*aeron_agent_on_start_func_t)(void *state, const char *role_name);

int aeron_context_set_agent_on_start_function(
    aeron_context_t *context, aeron_agent_on_start_func_t value, void *state);
aeron_agent_on_start_func_t aeron_context_get_agent_on_start_function(aeron_context_t *context);
void *aeron_context_get_agent_on_start_state(aeron_context_t *context);

/**
 * Create a aeron_context_t struct and initialize with default values.
 *
 * @param context to create and initialize
 * @return 0 for success and -1 for error.
 */
int aeron_context_init(aeron_context_t **context);

/**
 * Close and delete aeron_context_t struct.
 *
 * @param context to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_context_close(aeron_context_t *context);

/**
 * Create a aeron_t client struct and initialize from the aeron_context_t struct.
 *
 * The given aeron_context_t struct will be used exclusively by the client. Do not reuse between clients.
 *
 * @param aeron  client to create and initialize.
 * @param context to use for initialization.
 * @return 0 for success and -1 for error.
 */
int aeron_init(aeron_t **client, aeron_context_t *context);

/**
 * Start an aeron_t. This may spawn a thread for the Client Conductor.
 *
 * @param client to start.
 * @return 0 for success and -1 for error.
 */
int aeron_start(aeron_t *client);

/**
 * Call the Conductor main do_work duty cycle once.
 *
 * Client must have been created with use conductor invoker set to true.
 *
 * @param client to call do_work duty cycle on.
 * @return 0 for success and -1 for error.
 */
int aeron_main_do_work(aeron_t *client);

/**
 * Call the Conductor Idle Strategy.
 *
 * @param client to idle.
 * @param work_count to pass to idle strategy.
 */
void aeron_main_idle_strategy(aeron_t *client, int work_count);

/**
 * Close and delete aeron_t struct.
 *
 * @param client to close and delete
 * @return 0 for success and -1 for error.
 */
int aeron_close(aeron_t *client);

/**
 * Determines if the client has been closed, e.g. via a driver timeout. Don't call this method after calling
 * aeron_close as that will have already freed the associated memory.
 *
 * @param client to check if closed.
 * @return true if it has been closed, false otherwise.
 */
bool aeron_is_closed(aeron_t *client);

/**
 * Aeron API functions
 */

/**
 * Call stream_out to print the counter labels and values.
 *
 * @param client to get the counters from.
 * @param stream_out to call for each label and value.
 */
void aeron_print_counters(aeron_t *client, void (*stream_out)(const char *));

/**
 * Return the aeron_context_t that is in use by the given client.
 *
 * @param client to return the aeron_context_t for.
 * @return the aeron_context_t for the given client or NULL for an error.
 */
aeron_context_t *aeron_context(aeron_t *client);

/**
 * Return the client id in use by the client.
 *
 * @param client to return the client id for.
 * @return id value or -1 for an error.
 */
int64_t aeron_client_id(aeron_t *client);

/**
 * Return a unique correlation id from the driver.
 *
 * @param client to use to get the id.
 * @return unique correlation id or -1 for an error.
 */
int64_t aeron_next_correlation_id(aeron_t *client);

/**
 * Asynchronously request next available session from the media driver. The session id will be unique for the
 * connected media driver and given {@code stream_id}.
 *
 * @param async object to use for polling completion.
 * @param client connected to the media driver.
 * @param stream_id for which a new session id is requested. Media driver only checks for session clashes at the
 *                 stream level.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_next_session_id(
    aeron_async_get_next_available_session_id_t **async, aeron_t *client, int32_t stream_id);

/**
 * Poll the completion of the aeron_async_next_session_id call.
 *
 * @param next_session_id to set if completed successfully.
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_async_next_session_id_poll(int32_t *next_session_id, aeron_async_get_next_available_session_id_t *async);

/**
 * Asynchronously add a publication using the given client and return an object to use to determine when the
 * publication is available.
 *
 * @param async object to use for polling completion.
 * @param client to add the publication to.
 * @param uri for the channel of the publication.
 * @param stream_id for the publication.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_add_publication(
    aeron_async_add_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id);

/**
 * Poll the completion of the aeron_async_add_publication call.
 *
 * @param publication to set if completed successfully.
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_async_add_publication_poll(aeron_publication_t **publication, aeron_async_add_publication_t *async);

/**
 * Asynchronously add an exclusive publication using the given client and return an object to use to determine when the
 * publication is available.
 *
 * @param async object to use for polling completion.
 * @param client to add the publication to.
 * @param uri for the channel of the publication.
 * @param stream_id for the publication.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id);

/**
 * Poll the completion of the aeron_async_add_exclusive_publication call.
 *
 * @param publication to set if completed successfully.
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_async_add_exclusive_publication_poll(
    aeron_exclusive_publication_t **publication, aeron_async_add_exclusive_publication_t *async);

/**
 * Asynchronously add a subscription using the given client and return an object to use to determine when the
 * subscription is available.
 *
 * @param async object to use for polling completion.
 * @param client to add the subscription to.
 * @param uri for the channel of the subscription.
 * @param stream_id for the subscription.
 * @param on_available_image_handler to be called when images become available on the subscription.
 * @param on_available_image_clientd to be passed when images become available on the subscription.
 * @param on_unavailable_image_handler to be called when images go unavailable on the subscription.
 * @param on_unavailable_image_clientd to be passed when images go unavailable on the subscription.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_t *client,
    const char *uri,
    int32_t stream_id,
    aeron_on_available_image_t on_available_image_handler,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image_handler,
    void *on_unavailable_image_clientd);

/**
 * Poll the completion of the aeron_async_add_subscription call.
 *
 * @param subscription to set if completed successfully.
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_async_add_subscription_poll(aeron_subscription_t **subscription, aeron_async_add_subscription_t *async);

/**
 * Return a reference to the counters reader of the given client.
 *
 * The aeron_counters_reader_t is maintained by the client. And should not be freed.
 *
 * @param client that contains the counters reader.
 * @return aeron_counters_reader_t or NULL for error.
 */
aeron_counters_reader_t *aeron_counters_reader(aeron_t *client);

/**
 * Asynchronously add a counter using the given client and return an object to use to determine when the
 * counter is available.
 *
 * @param async object to use for polling completion.
 * @param client to add the counter to.
 * @param type_id for the counter.
 * @param key_buffer for the counter.
 * @param key_buffer_length for the counter.
 * @param label_buffer for the counter.
 * @param label_buffer_length for the counter.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_add_counter(
    aeron_async_add_counter_t **async,
    aeron_t *client,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length);

/**
 * Poll the completion of the <code>aeron_async_add_counter</code> or <code>aeron_async_add_static_counter</code> calls.
 *
 * @param counter to set if completed successfully.
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_async_add_counter_poll(aeron_counter_t **counter, aeron_async_add_counter_t *async);

/**
 * Asynchronously allocates or returns an existing static counter instance using specified <code>type_id</code> and
 * <code>registration_id</code>. Such counter cannot be deleted and its lifecycle is decoupled from the client that created it.
 * Returns an object to use to determine when the counter is available.
 *
 * @param async object to use for polling completion.
 * @param client to add the counter to.
 * @param type_id for the counter.
 * @param key_buffer for the counter.
 * @param key_buffer_length for the counter.
 * @param label_buffer for the counter.
 * @param label_buffer_length for the counter.
 * @param registration_id that uniquely identifies the counter.
 * @return 0 for success or -1 for an error.
 */
int aeron_async_add_static_counter(
    aeron_async_add_counter_t **async,
    aeron_t *client,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length,
    int64_t registration_id);

typedef struct aeron_on_available_counter_pair_stct
{
    aeron_on_available_counter_t handler;
    void *clientd;
}
aeron_on_available_counter_pair_t;

typedef struct aeron_on_unavailable_counter_pair_stct
{
    aeron_on_unavailable_counter_t handler;
    void *clientd;
}
aeron_on_unavailable_counter_pair_t;

typedef struct aeron_on_close_client_pair_stct
{
    aeron_on_close_client_t handler;
    void *clientd;
}
aeron_on_close_client_pair_t;

/**
 * Add a handler to be called when a new counter becomes available.
 *
 * NOTE: This function blocks until the handler is added by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_add_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair);

/**
 * Remove a previously added handler to be called when a new counter becomes available.
 *
 * NOTE: This function blocks until the handler is removed by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_remove_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair);

/**
 * Add a handler to be called when a new counter becomes unavailable or goes away.
 *
 * NOTE: This function blocks until the handler is added by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_add_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair);

/**
 * Remove a previously added handler to be called when a new counter becomes unavailable or goes away.
 *
 * NOTE: This function blocks until the handler is removed by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_remove_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair);

/**
 * Add a handler to be called when client is closed.
 *
 * NOTE: This function blocks until the handler is added by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_add_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair);

/**
 * Remove a previously added handler to be called when client is closed.
 *
 * NOTE: This function blocks until the handler is removed by the client conductor thread.
 *
 * @param client for the counter
 * @param pair holding the handler to call and a clientd to pass when called.
 * @return 0 for success and -1 for error
 */
int aeron_remove_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair);

/**
 * Counters Reader functions and definitions
 */

// Separate definition to avoid needing aeron_bitutil.h
#define AERON_COUNTER_CACHE_LINE_LENGTH (64u)

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_counter_value_descriptor_stct
{
    volatile int64_t counter_value;
    volatile int64_t registration_id;
    int64_t owner_id;
    int64_t reference_id;
    uint8_t pad1[(2 * AERON_COUNTER_CACHE_LINE_LENGTH) - (4 * sizeof(int64_t))];
}
aeron_counter_value_descriptor_t;

typedef struct aeron_counter_metadata_descriptor_stct
{
    volatile int32_t state;
    int32_t type_id;
    volatile int64_t free_for_reuse_deadline_ms;
    uint8_t key[(2 * AERON_COUNTER_CACHE_LINE_LENGTH) - (2 * sizeof(int32_t)) - sizeof(int64_t)];
    volatile int32_t label_length;
    uint8_t label[(6 * AERON_COUNTER_CACHE_LINE_LENGTH) - sizeof(int32_t)];
}
aeron_counter_metadata_descriptor_t;
#pragma pack(pop)


#define AERON_COUNTER_VALUE_LENGTH sizeof(aeron_counter_value_descriptor_t)
#define AERON_COUNTER_REGISTRATION_ID_OFFSET offsetof(aeron_counter_value_descriptor_t, registration_id)

#define AERON_COUNTER_METADATA_LENGTH sizeof(aeron_counter_metadata_descriptor_t)
#define AERON_COUNTER_TYPE_ID_OFFSET offsetof(aeron_counter_metadata_descriptor_t, type_id)
#define AERON_COUNTER_FREE_FOR_REUSE_DEADLINE_OFFSET offsetof(aeron_counter_metadata_descriptor_t, free_for_reuse_deadline_ms)
#define AERON_COUNTER_KEY_OFFSET offsetof(aeron_counter_metadata_descriptor_t, key)
#define AERON_COUNTER_LABEL_LENGTH_OFFSET offsetof(aeron_counter_metadata_descriptor_t, label)

#define AERON_COUNTER_MAX_LABEL_LENGTH sizeof(((aeron_counter_metadata_descriptor_t *)NULL)->label)
#define AERON_COUNTER_MAX_KEY_LENGTH sizeof(((aeron_counter_metadata_descriptor_t *)NULL)->key)
#define AERON_COUNTER_MAX_CLIENT_NAME_LENGTH (100)

#define AERON_COUNTER_RECORD_UNUSED (0)
#define AERON_COUNTER_RECORD_ALLOCATED (1)
#define AERON_COUNTER_RECORD_RECLAIMED (-1)

#define AERON_COUNTER_REGISTRATION_ID_DEFAULT INT64_C(0)
#define AERON_COUNTER_NOT_FREE_TO_REUSE (INT64_MAX)
#define AERON_COUNTER_OWNER_ID_DEFAULT INT64_C(0)
#define AERON_COUNTER_REFERENCE_ID_DEFAULT INT64_C(0)

#define AERON_NULL_COUNTER_ID (-1)

#define AERON_COUNTER_OFFSET(id) ((id) * AERON_COUNTER_VALUE_LENGTH)
#define AERON_COUNTER_METADATA_OFFSET(id) ((id) * AERON_COUNTER_METADATA_LENGTH)

typedef struct aeron_counters_reader_buffers_stct
{
    uint8_t *values;
    uint8_t *metadata;
    size_t values_length;
    size_t metadata_length;
}
aeron_counters_reader_buffers_t;

/**
 * Get buffer pointers and lengths for the counters reader.
 *
 * @param reader reader containing the buffers.
 * @param buffers output structure to return the buffers.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_get_buffers(aeron_counters_reader_t *reader, aeron_counters_reader_buffers_t *buffers);

/**
 * Function called by aeron_counters_reader_foreach_counter for each counter in the aeron_counters_reader_t.
 *
 * @param value of the counter.
 * @param id of the counter.
 * @param label for the counter.
 * @param label_length for the counter.
 * @param clientd to be returned in the call
 */
typedef void (*aeron_counters_reader_foreach_counter_func_t)(
    int64_t value,
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length,
    void *clientd);

/**
 * Iterate over the counters in the counters_reader and call the given function for each counter.
 *
 * @param counters_reader to iterate over.
 * @param func to call for each counter.
 * @param clientd to pass for each call to func.
 */
void aeron_counters_reader_foreach_counter(
    aeron_counters_reader_t *counters_reader, aeron_counters_reader_foreach_counter_func_t func, void *clientd);

/**
 * Iterate over allocated counters and find the first matching a given type id and registration id.
 *
 * @param counters_reader
 * @param type_id to find.
 * @param registration_id to find.
 * @return the counter id if found otherwise AERON_NULL_COUNTER_ID.
 */
int32_t aeron_counters_reader_find_by_type_id_and_registration_id(
    aeron_counters_reader_t *counters_reader, int32_t type_id, int64_t registration_id);

/**
 * Get the current max counter id.
 *
 * @param reader to query
 * @return -1 on failure, max counter id on success.
 */
int32_t aeron_counters_reader_max_counter_id(aeron_counters_reader_t *reader);

/**
 * Get the address for a counter.
 *
 * @param counters_reader that contains the counter
 * @param counter_id to find
 * @return address of the counter value
 */
int64_t *aeron_counters_reader_addr(aeron_counters_reader_t *counters_reader, int32_t counter_id);

/**
 * Get the registration id assigned to a counter.
 *
 * @param counters_reader representing the this pointer.
 * @param counter_id      for which the registration id is requested.
 * @param registration_id pointer for value to be set on success.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_counter_registration_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *registration_id);

/**
 * Get the owner id assigned to a counter which will typically be the client id.
 *
 * @param counters_reader representing the this pointer.
 * @param counter_id      for which the owner id is requested.
 * @param owner_id        pointer for value to be set on success.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_counter_owner_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *owner_id);

/**
 * Get the reference id assigned to a counter which will typically be the registration id of an associated Image,
 * Subscription, Publication, etc.
 *
 * @param counters_reader representing the this pointer.
 * @param counter_id      for which the reference id is requested.
 * @param reference_id    pointer for value to be set on success.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_counter_reference_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *reference_id);

/**
 * Get the state for a counter.
 *
 * @param counters_reader that contains the counter
 * @param counter_id to find
 * @param state out pointer for the current state to be stored in.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_counter_state(aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t *state);

/**
 * Get the type id for a counter.
 *
 * @param counters_reader that contains the counter
 * @param counter_id to find
 * @param type id out pointer for the current state to be stored in.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_counter_type_id(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int32_t *type_id);

/**
 * Get a pointer to the key of a counter's metadata
 *
 * @param counters_reader that contains the counter
 * @param counter_id to find
 * @param key_p out pointer set to location of metadata key
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_metadata_key(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, uint8_t **key_p);

/**
 * Get the label for a counter.
 *
 * @param counters_reader that contains the counter
 * @param counter_id to find
 * @param buffer to store the counter in.
 * @param buffer_length length of the output buffer
 * @return -1 on failure, number of characters copied to buffer on success.
 */
int aeron_counters_reader_counter_label(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, char *buffer, size_t buffer_length);

/**
 * Get the free for reuse deadline (ms) for a counter.
 *
 * @param counters_reader that contains the counter.
 * @param counter_id to find.
 * @param deadline_ms output value to store the deadline.
 * @return -1 on failure, 0 on success.
 */
int aeron_counters_reader_free_for_reuse_deadline_ms(
    aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t *deadline_ms);

/**
 * Publication functions
 */

/**
 * The publication is not connected to a subscriber, this can be an intermittent state as subscribers come and go.
 */
#define AERON_PUBLICATION_NOT_CONNECTED (-1L)

/**
 * The offer failed due to back pressure from the subscribers preventing further transmission.
 */
#define AERON_PUBLICATION_BACK_PRESSURED (-2L)

/**
 * The offer failed due to an administration action and should be retried.
 * The action is an operation such as log rotation which is likely to have succeeded by the next retry attempt.
 */
#define AERON_PUBLICATION_ADMIN_ACTION (-3L)

/**
 * The publication has been closed and should no longer be used.
 */
#define AERON_PUBLICATION_CLOSED (-4L)

/**
 * The offer failed due to reaching the maximum position of the stream given term buffer length times the total
 * possible number of terms.
 * <p>
 * If this happens then the publication should be closed and a new one added. To make it less likely to happen then
 * increase the term buffer length.
 */
#define AERON_PUBLICATION_MAX_POSITION_EXCEEDED (-5L)

/**
 * An error has occurred. Such as a bad argument.
 */
#define AERON_PUBLICATION_ERROR (-6L)

/**
 * Function called when filling in the reserved value field of a message.
 *
 * @param clientd passed to the offer function.
 * @param buffer of the entire frame, including Aeron data header.
 * @param frame_length of the entire frame.
 */
typedef int64_t (*aeron_reserved_value_supplier_t)(void *clientd, uint8_t *buffer, size_t frame_length);

/**
 * Structure to hold pointer to a buffer and the buffer length.
 */
#if !defined(AERON_IOVEC)
typedef struct aeron_iovec_stct
{
    uint8_t *iov_base;
    size_t iov_len;
}
aeron_iovec_t;
#else
typedef struct iov aeron_iovec_t;
#endif

/**
 * Structure used to hold information for a try_claim function call.
 */
typedef struct aeron_buffer_claim_stct
{
    uint8_t *frame_header;
    uint8_t *data;
    size_t length;
}
aeron_buffer_claim_t;

/**
 * Commit the given buffer_claim as a complete message available for consumption.
 *
 * @param buffer_claim to commit.
 * @return 0 for success or -1 for error.
 */
int aeron_buffer_claim_commit(aeron_buffer_claim_t *buffer_claim);

/**
 * Abort the given buffer_claim and assign its position as padding.
 *
 * @param buffer_claim to abort.
 * @return 0 for success or -1 for error.
 */
int aeron_buffer_claim_abort(aeron_buffer_claim_t *buffer_claim);

/**
 * Configuration for a publication that does not change during it's lifetime.
 */
typedef struct aeron_publication_constants_stct
{
    /**
     * Media address for delivery to the channel.
     *
     * This returns a pointer only valid for the lifetime of the publication.
     */
    const char *channel;

    /**
     * The registration used to register this Publication with the media driver by the first publisher.
     */
    int64_t original_registration_id;

    /**
     * Get the registration id used to register this Publication with the media driver.
     *
     * If this value is different from the original_registration_id then a previous active registration exists.
     */
    int64_t registration_id;

    /**
     * The maximum possible position this stream can reach due to its term buffer length.
     *
     * Maximum possible position is term-length times 2^31 in bytes.
     */
    int64_t max_possible_position;

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    size_t position_bits_to_shift;

    /**
     * Get the length in bytes for each term partition in the log buffer.
     */
    size_t term_buffer_length;

    /**
     * Maximum message length supported in bytes. Messages may be made of multiple fragments if greater than
     * MTU length.
     */
    size_t max_message_length;

    /**
     * Maximum length of a message payload that fits within a message fragment.
     *
     * This is the MTU length minus the message fragment header length.
     */
    size_t max_payload_length;

    /**
     * Stream id of the publication.
     */
    int32_t stream_id;

    /**
     * Session id of the publication.
     */
    int32_t session_id;

    /**
     * The initial term id assigned when this publication was created. This can be used to determine how many
     * terms have passed since creation.
     */
    int32_t initial_term_id;

    /**
     * Counter id for the publication limit.
     */
    int32_t publication_limit_counter_id;

    /**
     * Counter id for the channel status indicator
     */
    int32_t channel_status_indicator_id;
}
aeron_publication_constants_t;

/**
 * Non-blocking publish of a buffer containing a message.
 *
 * @param publication to publish on.
 * @param buffer to publish.
 * @param length of the buffer.
 * @param reserved_value_supplier to use for setting the reserved value field or NULL.
 * @param clientd to pass to the reserved_value_supplier.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_publication_offer(
    aeron_publication_t *publication,
    const uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);

/**
 * Non-blocking publish by gathering buffer vectors into a message.
 *
 * @param publication to publish on.
 * @param iov array for the vectors
 * @param iovcnt of the number of vectors
 * @param reserved_value_supplier to use for setting the reserved value field or NULL.
 * @param clientd to pass to the reserved_value_supplier.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_publication_offerv(
    aeron_publication_t *publication,
    aeron_iovec_t *iov,
    size_t iovcnt,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);

/**
 * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
 * Once the message has been written then aeron_buffer_claim_commit should be called thus making it available.
 * A claim length cannot be greater than max payload length.
 * <p>
 * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
 * If the claim is held for more than the aeron.publication.unblock.timeout system property then the driver will
 * assume the publication thread is dead and will unblock the claim thus allowing other threads to make progress
 * and other claims to be sent to reach end-of-stream (EOS).
 *
 * @code
 * aeron_buffer_claim_t buffer_claim;
 *
 * if (aeron_publication_try_claim(publication, length, &buffer_claim) > 0L)
 * {
 *     // work with buffer_claim->data directly.
 *     aeron_buffer_claim_commit(&buffer_claim);
 * }
 * @endcode
 *
 * @param publication to publish to.
 * @param length of the message.
 * @param buffer_claim to be populated if the claim succeeds.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_publication_try_claim(
    aeron_publication_t *publication, size_t length, aeron_buffer_claim_t *buffer_claim);

/**
 * Get the status of the media channel for this publication.
 * <p>
 * The status will be ERRORED (-1) if a socket exception occurs on setup and ACTIVE (1) if all is well.
 *
 * @param publication to check status of.
 * @return 1 for ACTIVE, -1 for ERRORED
 */
int64_t aeron_publication_channel_status(aeron_publication_t *publication);

/**
 * Has the publication closed?
 *
 * @param publication to check
 * @return true if this publication is closed.
 */
bool aeron_publication_is_closed(aeron_publication_t *publication);

/**
 * Has the publication seen an active Subscriber recently?
 *
 * @param publication to check.
 * @return true if this publication has recently seen an active subscriber otherwise false.
 */
bool aeron_publication_is_connected(aeron_publication_t *publication);

/**
 * Fill in a structure with the constants in use by a publication.
 *
 * @param publication to get the constants for.
 * @param constants structure to fill in with the constants
 * @return 0 for success and -1 for error.
 */
int aeron_publication_constants(aeron_publication_t *publication, aeron_publication_constants_t *constants);

/**
 * Get the current position to which the publication has advanced for this stream.
 *
 * @param publication to query.
 * @return the current position to which the publication has advanced for this stream or a negative error value.
 */
int64_t aeron_publication_position(aeron_publication_t *publication);

/**
 * Get the position limit beyond which this publication will be back pressured.
 *
 * This should only be used as a guide to determine when back pressure is likely to be applied.
 *
 * @param publication to query.
 * @return the position limit beyond which this publication will be back pressured or a negative error value.
 */
int64_t aeron_publication_position_limit(aeron_publication_t *publication);

/**
 * Add a destination manually to a multi-destination-cast publication.
 *
 * @param async object to use for polling completion.
 * @param publication to add destination to.
 * @param uri for the destination to add.
 * @return 0 for success and -1 for error.
 */
int aeron_publication_async_add_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_publication_t *publication,
    const char *uri);

/**
 * Remove a destination manually from a multi-destination-cast publication.
 *
 * @param async object to use for polling completion.
 * @param publication to remove destination from.
 * @param uri for the destination to remove.
 * @return 0 for success and -1 for error.
 */
int aeron_publication_async_remove_destination(
    aeron_async_destination_t **async, aeron_t *client, aeron_publication_t *publication, const char *uri);

/**
 * Remove a destination manually from a multi-destination-cast publication.
 *
 * @param async object to use for polling completion.
 * @param publication to remove destination from.
 * @param destination_registration_id for the destination to remove.
 * @return 0 for success and -1 for error.
 */
int aeron_publication_async_remove_destination_by_id(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_publication_t *publication,
    int64_t destination_registration_id);

/**
 * Poll the completion of the add/remove of a destination to/from a publication.
 *
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_publication_async_destination_poll(aeron_async_destination_t *async);

/**
 * Add a destination manually to a multi-destination-cast exclusive publication.
 *
 * @param async object to use for polling completion.
 * @param publication to add destination to.
 * @param uri for the destination to add.
 * @return 0 for success and -1 for error.
 */
int aeron_exclusive_publication_async_add_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    const char *uri);

/**
 * Remove a destination manually from a multi-destination-cast exclusive publication.
 *
 * @param async object to use for polling completion.
 * @param publication to remove destination from.
 * @param uri for the destination to remove.
 * @return 0 for success and -1 for error.
 */
int aeron_exclusive_publication_async_remove_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    const char *uri);

/**
 * Remove a destination manually from a multi-destination-cast publication.
 *
 * @param async object to use for polling completion.
 * @param publication to remove destination from.
 * @param destination_registration_id for the destination to remove.
 * @return 0 for success and -1 for error.
 */
int aeron_exclusive_publication_async_remove_destination_by_id(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    int64_t destination_registration_id);

/**
 * Poll the completion of the add/remove of a destination to/from an exclusive publication.
 *
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_exclusive_publication_async_destination_poll(aeron_async_destination_t *async);

/**
 * Asynchronously close the publication. Will callback on the on_complete notification when the publication is closed.
 * The callback is optional, use NULL for the on_complete callback if not required.
 *
 * @param publication to close
 * @param on_close_complete optional callback to execute once the publication has been closed and freed. This may
 * happen on a separate thread, so the caller should ensure that clientd has the appropriate lifetime.
 * @param on_close_complete_clientd parameter to pass to the on_complete callback.
 * @return 0 for success or -1 for error.
 */
int aeron_publication_close(
    aeron_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd);

/**
 * Get the publication's channel
 *
 * @param publication this
 * @return channel uri string
 */
const char *aeron_publication_channel(aeron_publication_t *publication);

/**
 * Get the publication's stream id
 *
 * @param publication this
 * @return stream id
 */
int32_t aeron_publication_stream_id(aeron_publication_t *publication);

/**
 * Get the publication's session id
 * @param publication this
 * @return session id
 */
int32_t aeron_publication_session_id(aeron_publication_t *publication);

/**
 * Get all of the local socket addresses for this publication. Typically only one representing the control address.
 *
 * @param subscription to query
 * @param address_vec to hold the received addresses
 * @param address_vec_len available length of the vector to hold the addresses
 * @return number of addresses found or -1 if there is an error.
 * @see aeron_subscription_local_sockaddrs
 */
int aeron_publication_local_sockaddrs(
    aeron_publication_t *publication,
    aeron_iovec_t *address_vec,
    size_t address_vec_len);

/*
 * Exclusive Publication functions
 */

/**
 * Non-blocking publish of a buffer containing a message.
 *
 * @param publication to publish on.
 * @param buffer to publish.
 * @param length of the buffer.
 * @param reserved_value_supplier to use for setting the reserved value field or NULL.
 * @param clientd to pass to the reserved_value_supplier.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_exclusive_publication_offer(
    aeron_exclusive_publication_t *publication,
    const uint8_t *buffer,
    size_t length,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);

/**
 * Non-blocking publish by gathering buffer vectors into a message.
 *
 * @param publication to publish on.
 * @param iov array for the vectors
 * @param iovcnt of the number of vectors
 * @param reserved_value_supplier to use for setting the reserved value field or NULL.
 * @param clientd to pass to the reserved_value_supplier.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_exclusive_publication_offerv(
    aeron_exclusive_publication_t *publication,
    aeron_iovec_t *iov,
    size_t iovcnt,
    aeron_reserved_value_supplier_t reserved_value_supplier,
    void *clientd);

/**
 * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
 * Once the message has been written then aeron_buffer_claim_commit should be called thus making it available.
 * A claim length cannot be greater than max payload length.
 * <p>
 * <b>Note:</b> This method can only be used for message lengths less than MTU length minus header.
 *
 * @code
 * aeron_buffer_claim_t buffer_claim;
 *
 * if (aeron_exclusive_publication_try_claim(publication, length, &buffer_claim) > 0L)
 * {
 *     // work with buffer_claim->data directly.
 *     aeron_buffer_claim_commit(&buffer_claim);
 * }
 * @endcode
 *
 * @param publication to publish to.
 * @param length of the message.
 * @param buffer_claim to be populated if the claim succeeds.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_exclusive_publication_try_claim(
    aeron_exclusive_publication_t *publication, size_t length, aeron_buffer_claim_t *buffer_claim);

/**
 * Append a padding record log of a given length to make up the log to a position.
 *
 * @param length of the range to claim, in bytes.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_exclusive_publication_append_padding(aeron_exclusive_publication_t *publication, size_t length);

/**
 * Offer a block of pre-formatted message fragments directly into the current term.
 *
 * @param buffer containing the pre-formatted block of message fragments.
 * @param offset offset in the buffer at which the first fragment begins.
 * @param length in bytes of the encoded block.
 * @return the new stream position otherwise a negative error value.
 */
int64_t aeron_exclusive_publication_offer_block(
    aeron_exclusive_publication_t *publication, const uint8_t *buffer, size_t length);

/**
 * Get the status of the media channel for this publication.
 * <p>
 * The status will be ERRORED (-1) if a socket exception occurs on setup and ACTIVE (1) if all is well.
 *
 * @param publication to check status of.
 * @return 1 for ACTIVE, -1 for ERRORED
 */
int64_t aeron_exclusive_publication_channel_status(aeron_exclusive_publication_t *publication);

/**
 * Fill in a structure with the constants in use by a publication.
 *
 * @param publication to get the constants for.
 * @param constants structure to fill in with the constants
 * @return 0 for success and -1 for error.
 */
int aeron_exclusive_publication_constants(
    aeron_exclusive_publication_t *publication, aeron_publication_constants_t *constants);

/**
 * Get the current position to which the publication has advanced for this stream.
 *
 * @param publication to query.
 * @return the current position to which the publication has advanced for this stream or a negative error value.
 */
int64_t aeron_exclusive_publication_position(aeron_exclusive_publication_t *publication);

/**
 * Get the position limit beyond which this publication will be back pressured.
 *
 * This should only be used as a guide to determine when back pressure is likely to be applied.
 *
 * @param publication to query.
 * @return the position limit beyond which this publication will be back pressured or a negative error value.
 */
int64_t aeron_exclusive_publication_position_limit(aeron_exclusive_publication_t *publication);

/**
 * Asynchronously close the publication.
 *
 * @param publication to close
 * @return 0 for success or -1 for error.
 */
int aeron_exclusive_publication_close(
    aeron_exclusive_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd);

/**
 * Revoke this publication when it's closed.
 *
 * @param publication to revoke on close
 */
void aeron_exclusive_publication_revoke_on_close(aeron_exclusive_publication_t *publication);

/**
 * Asynchronously revoke and close the publication. Will callback on the on_complete notification when the publicaiton is closed.
 * The callback is optional, use NULL for the on_complete callback if not required.
 *
 * @param publication to revoke and close
 * @param on_close_complete optional callback to execute once the publication has been revoked, closed and freed. This may
 * happen on a separate thread, so the caller should ensure that clientd has the appropriate lifetime.
 * @param on_close_complete_clientd parameter to pass to the on_complete callback.
 * @return 0 for success or -1 for error.
 */
int aeron_exclusive_publication_revoke(
    aeron_exclusive_publication_t *publication,
    aeron_notification_t on_close_complete,
    void *on_close_complete_clientd);

/**
 * Has the exclusive publication closed?
 *
 * @param publication to check
 * @return true if this publication is closed.
 */
bool aeron_exclusive_publication_is_closed(aeron_exclusive_publication_t *publication);

/**
 * Has the exclusive publication seen an active Subscriber recently?
 *
 * @param publication to check.
 * @return true if this publication has recently seen an active subscriber otherwise false.
 */
bool aeron_exclusive_publication_is_connected(aeron_exclusive_publication_t *publication);

/**
 * Get all of the local socket addresses for this exclusive publication. Typically only one representing the control
 * address.
 *
 * @see aeron_subscription_local_sockaddrs
 * @param subscription to query
 * @param address_vec to hold the received addresses
 * @param address_vec_len available length of the vector to hold the addresses
 * @return number of addresses found or -1 if there is an error.
 */
int aeron_exclusive_publication_local_sockaddrs(
    aeron_exclusive_publication_t *publication, aeron_iovec_t *address_vec, size_t address_vec_len);

/**
 * Subscription functions
 *
 * Aeron Subscriber API for receiving a reconstructed image for a stream of messages from publishers on
 * a given channel and stream id pair. Images are aggregated under a subscription.
 * <p>
 * Subscription are created via an aeron_t object, and received messages are delivered
 * to the fragment handler.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the subscriber
 * should be created with a fragment assembler or a custom implementation.
 * <p>
 * It is an application's responsibility to poll the subscription for new messages.
 * <p>
 * <b>Note:</b>Subscriptions are not threadsafe and should not be shared between subscribers.
 */

/**
 * Callback for handling fragments of data being read from a log.
 *
 * The frame will either contain a whole message or a fragment of a message to be reassembled. Messages are fragmented
 * if greater than the frame for MTU in length.
 *
 * @param clientd passed to the poll function.
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 */
typedef void (*aeron_fragment_handler_t)(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

typedef enum aeron_controlled_fragment_handler_action_en
{
    /**
     * Abort the current polling operation and do not advance the position for this fragment.
     */
    AERON_ACTION_ABORT = 1,

    /**
     * Break from the current polling operation and commit the position as of the end of the current fragment
     * being handled.
     */
    AERON_ACTION_BREAK = 2,

    /**
     * Continue processing but commit the position as of the end of the current fragment so that
     * flow control is applied to this point.
     */
    AERON_ACTION_COMMIT = 3,

    /**
     * Continue processing until fragment limit or no fragments with position commit at end of poll as in
     * aeron_fragment_handler_t.
     */
    AERON_ACTION_CONTINUE = 4
}
aeron_controlled_fragment_handler_action_t;

/**
 * Callback for handling fragments of data being read from a log.
 *
 * Handler for reading data that is coming from a log buffer. The frame will either contain a whole message
 * or a fragment of a message to be reassembled. Messages are fragmented if greater than the frame for MTU in length.
 *
 * @param clientd passed to the controlled poll function.
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 * @return The action to be taken with regard to the stream position after the callback.
 */
typedef aeron_controlled_fragment_handler_action_t (*aeron_controlled_fragment_handler_t)(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/**
 * Callback for handling a block of messages being read from a log.
 *
 * @param clientd passed to the block poll function.
 * @param buffer containing the block of message fragments.
 * @param offset at which the block begins, including any frame headers.
 * @param length of the block in bytes, including any frame headers that is aligned.
 * @param session_id of the stream containing this block of message fragments.
 * @param term_id of the stream containing this block of message fragments.
 */
typedef void (*aeron_block_handler_t)(
    void *clientd, const uint8_t *buffer, size_t length, int32_t session_id, int32_t term_id);

/**
 * Get all of the field values from the header. This will do a memcpy into the supplied header_values_t pointer.
 *
 * @param header to read values from.
 * @param values to copy values to, must not be null.
 * @return 0 on success, -1 on failure.
 */
int aeron_header_values(aeron_header_t *header, aeron_header_values_t *values);

/**
 * Get the current position to which the Image has advanced on reading this message.
 *
 * @param header the current header message
 * @return the current position to which the Image has advanced on reading this message.
 */
int64_t aeron_header_position(aeron_header_t *header);

/**
 * Get the number of times to left shift the term count to multiply by term length.
 *
 * @return number of times to left shift the term count to multiply by term length.
 */
size_t aeron_header_position_bits_to_shift(aeron_header_t *header);

/**
 * Calculates the offset of the frame immediately after this one.
 *
 * @return the offset of the next frame.
 */
int32_t aeron_header_next_term_offset(aeron_header_t *header);

/**
 * Get a pointer to the context associated with this message. Only valid during poll handling. Is normally a
 * pointer to an Image instance.
 *
 * @return a pointer to the context associated with this message.
 */
void *aeron_header_context(aeron_header_t *header);

typedef struct aeron_subscription_constants_stct
{
    /**
     * Media address for delivery to the channel.
     *
     * This returns a pointer only valid for the lifetime of the subscription.
     */
    const char *channel;

    /**
     * Callback used to indicate when an Image becomes available under this Subscription.
     */
    aeron_on_available_image_t on_available_image;

    /**
     * Callback used to indicate when an Image goes unavailable under this Subscription.
     */
    aeron_on_unavailable_image_t on_unavailable_image;

    /**
     * Return the registration id used to register this Subscription with the media driver.
     */
    int64_t registration_id;

    /**
     * Stream identity for scoping within the channel media address.
     */
    int32_t stream_id;

    /**
     * Counter id for the channel status indicator
     */
    int32_t channel_status_indicator_id;
}
aeron_subscription_constants_t;

/**
 * Poll the images under the subscription for available message fragments.
 * <p>
 * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
 * as a series of fragments ordered within a session.
 * <p>
 * To assemble messages that span multiple fragments then use aeron_fragment_assembler_t.
 *
 * @param subscription to poll.
 * @param handler for handling each message fragment as it is read.
 * @param fragment_limit number of message fragments to limit when polling across multiple images.
 * @return the number of fragments received or -1 for error.
 */
int aeron_subscription_poll(
    aeron_subscription_t *subscription, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit);

/**
 * Poll in a controlled manner the images under the subscription for available message fragments.
 * Control is applied to fragments in the stream. If more fragments can be read on another stream
 * they will even if BREAK or ABORT is returned from the fragment handler.
 * <p>
 * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
 * as a series of fragments ordered within a session.
 * <p>
 * To assemble messages that span multiple fragments then use aeron_controlled_fragment_assembler_t.
 *
 * @param subscription to poll.
 * @param handler for handling each message fragment as it is read.
 * @param fragment_limit number of message fragments to limit when polling across multiple images.
 * @return the number of fragments received or -1 for error.
 */
int aeron_subscription_controlled_poll(
    aeron_subscription_t *subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);

/**
 * Poll the images under the subscription for available message fragments in blocks.
 * <p>
 * This method is useful for operations like bulk archiving and messaging indexing.
 *
 * @param subscription to poll.
 * @param handler to receive a block of fragments from each image.
 * @param block_length_limit for each image polled.
 * @return the number of bytes consumed or -1 for error.
 */
long aeron_subscription_block_poll(
    aeron_subscription_t *subscription, aeron_block_handler_t handler, void *clientd, size_t block_length_limit);

/**
 * Is this subscription connected by having at least one open publication image.
 *
 * @param subscription to check.
 * @return true if this subscription connected by having at least one open publication image.
 */
bool aeron_subscription_is_connected(aeron_subscription_t *subscription);

/**
 * Fill in a structure with the constants in use by a subscription.
 *
 * @param subscription to get the constants for.
 * @param constants structure to fill in with the constants
 * @return 0 for success and -1 for error.
 */
int aeron_subscription_constants(aeron_subscription_t *subscription, aeron_subscription_constants_t *constants);

/**
 * Count of images associated to this subscription.
 *
 * @param subscription to count images for.
 * @return count of count associated to this subscription or -1 for error.
 */
int aeron_subscription_image_count(aeron_subscription_t *subscription);

/**
 * Return the image associated with the given session_id under the given subscription.
 *
 * Note: the returned image is considered retained by the application and thus must be released via
 * aeron_image_release when finished or if the image becomes unavailable.
 *
 * @param subscription to search.
 * @param session_id associated with the image.
 * @return image associated with the given session_id or NULL if no image exists.
 */
aeron_image_t *aeron_subscription_image_by_session_id(aeron_subscription_t *subscription, int32_t session_id);

/**
 * Return the image at the given index.
 *
 * Note: the returned image is considered retained by the application and thus must be released via
 * aeron_image_release when finished or if the image becomes unavailable.
 *
 * @param subscription to search.
 * @param index for the image.
 * @return image at the given index or NULL if no image exists.
 */
aeron_image_t *aeron_subscription_image_at_index(aeron_subscription_t *subscription, size_t index);

/**
 * Iterate over the images for this subscription calling the given function.
 *
 * @param subscription to iterate over.
 * @param handler to be called for each image.
 * @param clientd to be passed to the handler.
 */
void aeron_subscription_for_each_image(
    aeron_subscription_t *subscription, void (*handler)(aeron_image_t *image, void *clientd), void *clientd);

/**
 * Retain the given image for access in the application.
 *
 * Note: A retain call must have a corresponding release call.
 * Note: Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @param subscription that image is part of.
 * @param image to retain
 * @return 0 for success and -1 for error.
 */
int aeron_subscription_image_retain(aeron_subscription_t *subscription, aeron_image_t *image);

/**
 * Release the given image and relinquish desire to use the image directly.
 *
 * Note: Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @param subscription that image is part of.
 * @param image to release
 * @return 0 for success and -1 for error.
 */
int aeron_subscription_image_release(aeron_subscription_t *subscription, aeron_image_t *image);

/**
 * Is the subscription closed.
 *
 * @param subscription to be checked.
 * @return true if it has been closed otherwise false.
 */
bool aeron_subscription_is_closed(aeron_subscription_t *subscription);

/**
 * Get the status of the media channel for this subscription.
 * <p>
 * The status will be ERRORED (-1) if a socket exception occurs on setup and ACTIVE (1) if all is well.
 *
 * @param subscription to check status of.
 * @return 1 for ACTIVE, -1 for ERRORED
 */
int64_t aeron_subscription_channel_status(aeron_subscription_t *subscription);

/**
 * Add a destination manually to a multi-destination-subscription.
 *
 * @param async object to use for polling completion.
 * @param subscription to add destination to.
 * @param uri for the destination to add.
 * @return 0 for success and -1 for error.
 */
int aeron_subscription_async_add_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_subscription_t *subscription,
    const char *uri);

/**
 * Remove a destination manually from a multi-destination-subscription.
 *
 * @param async object to use for polling completion.
 * @param subscription to remove destination from.
 * @param uri for the destination to remove.
 * @return 0 for success and -1 for error.
 */
int aeron_subscription_async_remove_destination(
    aeron_async_destination_t **async, aeron_t *client, aeron_subscription_t *subscription, const char *uri);

/**
 * Poll the completion of add/remove of a destination to/from a subscription.
 *
 * @param async to check for completion.
 * @return 0 for not complete (try again), 1 for completed successfully, or -1 for an error.
 */
int aeron_subscription_async_destination_poll(aeron_async_destination_t *async);

/**
 * Asynchronously close the subscription. Will callback on the on_complete notification when the subscription is
 * closed. The callback is optional, use NULL for the on_complete callback if not required.
 *
 * @param subscription to close
 * @param on_close_complete optional callback to execute once the subscription has been closed and freed. This may
 * happen on a separate thread, so the caller should ensure that clientd has the appropriate lifetime.
 * @param on_close_complete_clientd parameter to pass to the on_complete callback.
 * @return 0 for success or -1 for error.
 */
int aeron_subscription_close(
    aeron_subscription_t *subscription, aeron_notification_t on_close_complete, void *on_close_complete_clientd);

/**
 * Get all of the local socket addresses for this subscription. Multiple addresses can occur if this is a
 * multi-destination subscription. Addresses will a string representation in numeric form. IPv6 addresses will be
 * surrounded by '[' and ']' so that the ':' that separate the parts are distinguishable from the port delimiter.
 * E.g. [fe80::7552:c06e:6bf4:4160]:12345. As of writing the maximum length for a formatted address is 54 bytes
 * including the NULL terminator. AERON_CLIENT_MAX_LOCAL_ADDRESS_STR_LEN is defined to provide enough space to fit the
 * returned string. Returned strings will be NULL terminated. If the buffer to hold the address can not hold enough
 * of the message it will be truncated and the last character will be null.
 *
 * If the address_vec_len is less the total number of addresses available then the first addresses found up to that
 * length will be placed into the address_vec. However the function will return the total number of addresses available
 * so if if that is larger than the input array then the client code may wish to re-query with a larger array to get
 * them all.
 *
 * @param subscription to query
 * @param address_vec to hold the received addresses
 * @param address_vec_len available length of the vector to hold the addresses
 * @return number of addresses found or -1 if there is an error.
 */
int aeron_subscription_local_sockaddrs(
    aeron_subscription_t *subscription, aeron_iovec_t *address_vec, size_t address_vec_len);

/**
 * Retrieves the first local socket address for this subscription. If this is not MDS then it will be the one
 * representing endpoint for this subscription.
 *
 * @see aeron_subscription_local_sockaddrs
 * @param subscription to query
 * @param address for the received address
 * @param address_len available length for the copied address.
 * @return -1 on error, 0 if address not found, 1 if address is found.
 */
int aeron_subscription_resolved_endpoint(aeron_subscription_t *subscription, const char *address, size_t address_len);

/**
 * Retrieves the channel URI for this subscription with any wildcard ports filled in. If the channel is not UDP or
 * does not have a wildcard port (<code>0</code>), then it will return the original URI.
 *
 * @param subscription to query
 * @param uri buffer to hold the resolved uri
 * @param uri_len length of the buffer
 * @return -1 on failure or the number of bytes written to the buffer (excluding the NULL terminator). Writing is done
 * on a per key basis, so if the buffer was truncated before writing completed, it will only include the byte count up
 * to the key that overflowed. However, the invariant that if the number returned >= uri_len, then output will have been
 * truncated.
 */
int aeron_subscription_try_resolve_channel_endpoint_port(
    aeron_subscription_t *subscription, char *uri, size_t uri_len);

/**
 * Image Functions
 *
 * Represents a replicated publication image from a publisher to a subscription.
 * Each image identifies a source publisher by session id.
 * <p>
 * By default fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, whether or not they were fragmented, then the subscriber
 * should be created with a fragment assembler or a custom implementation.
 * <p>
 * It is an application's responsibility to poll the image for new messages.
 * <p>
 * <b>Note:</b>Images are not threadsafe and should not be shared between subscribers.
 */

/**
 * Configuration for an image that does not change during it's lifetime.
 */
typedef struct aeron_image_constants_stct
{
    /**
     * The subscription to which this image belongs.
     */
    aeron_subscription_t *subscription;

    /**
     * The source identity of the sending publisher as an abstract concept appropriate for the media.
     */
    const char *source_identity;

    /**
     * The correlationId for identification of the image with the media driver.
     */
    int64_t correlation_id;

    /**
     * Get the position the subscriber joined this stream at.
     */
    int64_t join_position;

    /**
     * Number of bits to right shift a position to get a term count for how far the stream has progressed.
     */
    size_t position_bits_to_shift;

    /**
     * Get the length in bytes for each term partition in the log buffer.
     */
    size_t term_buffer_length;

    /**
     * The length in bytes of the MTU (Maximum Transmission Unit) the Sender used for the datagram.
     */
    size_t mtu_length;

    /**
     * The sessionId for the steam of messages. Sessions are unique within a subscription and unique across
     * all publications from a source identity.
     */
    int32_t session_id;

    /**
     * The initial term at which the stream started for this session.
     */
    int32_t initial_term_id;

    /**
     * Counter id that refers to the subscriber position for this image.
     */
    int32_t subscriber_position_id;
}
aeron_image_constants_t;

/**
 * Fill in a structure with the constants in use by a image.
 *
 * @param image to get the constants for.
 * @param constants structure to fill in with the constants
 * @return 0 for success and -1 for error.
 */
int aeron_image_constants(aeron_image_t *image, aeron_image_constants_t *constants);

/**
 * The position this image has been consumed to by the subscriber.
 *
 * @param image to query position of.
 * @return the position this image has been consumed to by the subscriber.
 */
int64_t aeron_image_position(aeron_image_t *image);

/**
 * Set the subscriber position for this image to indicate where it has been consumed to.
 *
 * @param image to set the position of.
 * @param new_position for the consumption point.
 */
int aeron_image_set_position(aeron_image_t *image, int64_t position);

/**
 * Is the current consumed position at the end of the stream?
 *
 * @param image to check.
 * @return true if at the end of the stream or false if not.
 */
bool aeron_image_is_end_of_stream(aeron_image_t *image);

/**
 * The position the stream reached when EOS was received from the publisher. The position will be
 * INT64_MAX until the stream ends and EOS is set.
 *
 * @param image to check.
 * @return position the stream reached when EOS was received from the publisher.
 */
int64_t aeron_image_end_of_stream_position(aeron_image_t *image);

/**
 * Count of observed active transports within the image liveness timeout.
 *
 * If the image is closed, then this is 0. This may also be 0 if no actual datagrams have arrived. IPC
 * Images also will be 0.
 *
 * @param image to check.
 * @return count of active transports - 0 if Image is closed, no datagrams yet, or IPC. Or -1 for error.
 */
int aeron_image_active_transport_count(aeron_image_t *image);

/**
 * Was the associated publication revoked?
 *
 * @param image to check
 * @return true if the associated publication was revoked.
 */
bool aeron_image_is_publication_revoked(aeron_image_t *image);

/**
 * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
 * will be delivered to the handler up to a limited number of fragments as specified.
 * <p>
 * Use a fragment assembler to assemble messages which span multiple fragments.
 *
 * @param image to poll.
 * @param handler to which message fragments are delivered.
 * @param clientd to pass to the handler.
 * @param fragment_limit for the number of fragments to be consumed during one polling operation.
 * @return the number of fragments that have been consumed or -1 for error.
 */
int aeron_image_poll(aeron_image_t *image, aeron_fragment_handler_t handler, void *clientd, size_t fragment_limit);

/**
 * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
 * will be delivered to the handler up to a limited number of fragments as specified.
 * <p>
 * Use a controlled fragment assembler to assemble messages which span multiple fragments.
 *
 * @param image to poll.
 * @param handler to which message fragments are delivered.
 * @param clientd to pass to the handler.
 * @param fragment_limit for the number of fragments to be consumed during one polling operation.
 * @return the number of fragments that have been consumed or -1 for error.
 */
int aeron_image_controlled_poll(
    aeron_image_t *image, aeron_controlled_fragment_handler_t handler, void *clientd, size_t fragment_limit);

/**
 * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
 * will be delivered to the handler up to a limited number of fragments as specified or the maximum position specified.
 * <p>
 * Use a fragment assembler to assemble messages which span multiple fragments.
 *
 * @param image to poll.
 * @param handler to which message fragments are delivered.
 * @param clientd to pass to the handler.
 * @param limit_position to consume messages up to.
 * @param fragment_limit for the number of fragments to be consumed during one polling operation.
 * @return the number of fragments that have been consumed or -1 for error.
 */
int aeron_image_bounded_poll(
    aeron_image_t *image,
    aeron_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position,
    size_t fragment_limit);

/**
 * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
 * will be delivered to the handler up to a limited number of fragments as specified or the maximum position specified.
 * <p>
 * Use a controlled fragment assembler to assemble messages which span multiple fragments.
 *
 * @param image to poll.
 * @param handler to which message fragments are delivered.
 * @param clientd to pass to the handler.
 * @param limit_position to consume messages up to.
 * @param fragment_limit for the number of fragments to be consumed during one polling operation.
 * @return the number of fragments that have been consumed or -1 for error.
 */
int aeron_image_bounded_controlled_poll(
    aeron_image_t *image,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position,
    size_t fragment_limit);

/**
 * Peek for new messages in a stream by scanning forward from an initial position. If new messages are found then
 * they will be delivered to the handler up to a limited position.
 * <p>
 * Use a controlled fragment assembler to assemble messages which span multiple fragments. Scans must also
 * start at the beginning of a message so that the assembler is reset.
 *
 * @param image to peek.
 * @param initial_position from which to peek forward.
 * @param handler to which message fragments are delivered.
 * @param clientd to pass to the handler.
 * @param limit_position up to which can be scanned.
 * @return the resulting position after the scan terminates which is a complete message or -1 for error.
 */
int64_t aeron_image_controlled_peek(
    aeron_image_t *image,
    int64_t initial_position,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    int64_t limit_position);

/**
 * Poll for new messages in a stream. If new messages are found beyond the last consumed position then they
 * will be delivered to the handler up to a limited number of bytes.
 * <p>
 * A scan will terminate if a padding frame is encountered. If first frame in a scan is padding then a block
 * for the padding is notified. If the padding comes after the first frame in a scan then the scan terminates
 * at the offset the padding frame begins. Padding frames are delivered singularly in a block.
 * <p>
 * Padding frames may be for a greater range than the limit offset but only the header needs to be valid so
 * relevant length of the frame is data header length.
 *
 * @param image to poll.
 * @param handler to which block is delivered.
 * @param clientd to pass to the handler.
 * @param block_length_limit up to which a block may be in length.
 * @return the number of bytes that have been consumed or -1 for error.
 */
int aeron_image_block_poll(
    aeron_image_t *image, aeron_block_handler_t handler, void *clientd, size_t block_length_limit);

bool aeron_image_is_closed(aeron_image_t *image);

int aeron_image_reject(aeron_image_t *image, const char *reason);

/**
 * A fragment handler that sits in a chain-of-responsibility pattern that reassembles fragmented messages
 * so that the next handler in the chain only sees whole messages.
 * <p>
 * Unfragmented messages are delegated without copy. Fragmented messages are copied to a temporary
 * buffer for reassembly before delegation.
 * <p>
 * The aeron_header_t passed to the delegate on assembling a message will be that of the last fragment.
 * <p>
 * Session based buffers will be allocated and grown as necessary based on the length of messages to be assembled.
 */

/**
 * Create an image fragment assembler for use with a single image.
 *
 * @param assembler to be set when created successfully.
 * @param delegate to call on completed.
 * @param delegate_clientd to pass to delegate handler.
 * @return 0 for success and -1 for error.
 */
int aeron_image_fragment_assembler_create(
    aeron_image_fragment_assembler_t **assembler, aeron_fragment_handler_t delegate, void *delegate_clientd);

/**
 * Delete an image fragment assembler.
 *
 * @param assembler to delete.
 * @return 0 for success or -1 for error.
 */
int aeron_image_fragment_assembler_delete(aeron_image_fragment_assembler_t *assembler);

/**
 * Handler function to be passed for handling fragment assembly.
 *
 * @param clientd passed in the poll call (must be a aeron_image_fragment_assembler_t)
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 */
void aeron_image_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/**
 * Create an image controlled fragment assembler for use with a single image.
 *
 * @param assembler to be set when created successfully.
 * @param delegate to call on completed
 * @param delegate_clientd to pass to delegate handler.
 * @return 0 for success and -1 for error.
 */
int aeron_image_controlled_fragment_assembler_create(
    aeron_image_controlled_fragment_assembler_t **assembler,
    aeron_controlled_fragment_handler_t delegate,
    void *delegate_clientd);

/**
 * Delete an image controlled fragment assembler.
 *
 * @param assembler to delete.
 * @return 0 for success or -1 for error.
 */
int aeron_image_controlled_fragment_assembler_delete(aeron_image_controlled_fragment_assembler_t *assembler);

/**
 * Handler function to be passed for handling fragment assembly.
 *
 * @param clientd passed in the poll call (must be a aeron_image_controlled_fragment_assembler_t)
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 * @return The action to be taken with regard to the stream position after the callback.
 */
aeron_controlled_fragment_handler_action_t aeron_image_controlled_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/**
 * Create a fragment assembler for use with a subscription.
 *
 * @param assembler to be set when created successfully.
 * @param delegate to call on completed
 * @param delegate_clientd to pass to delegate handler.
 * @return 0 for success and -1 for error.
 */
int aeron_fragment_assembler_create(
    aeron_fragment_assembler_t **assembler, aeron_fragment_handler_t delegate, void *delegate_clientd);

/**
 * Delete a fragment assembler.
 *
 * @param assembler to delete.
 * @return 0 for success or -1 for error.
 */
int aeron_fragment_assembler_delete(aeron_fragment_assembler_t *assembler);

/**
 * Handler function to be passed for handling fragment assembly.
 *
 * @param clientd passed in the poll call (must be a aeron_fragment_assembler_t)
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 */
void aeron_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/**
 * Create a controlled fragment assembler for use with a subscription.
 *
 * @param assembler to be set when created successfully.
 * @param delegate to call on completed
 * @param delegate_clientd to pass to delegate handler.
 * @return 0 for success and -1 for error.
 */
int aeron_controlled_fragment_assembler_create(
    aeron_controlled_fragment_assembler_t **assembler,
    aeron_controlled_fragment_handler_t delegate,
    void *delegate_clientd);

/**
 * Delete a controlled fragment assembler.
 *
 * @param assembler to delete.
 * @return 0 for success or -1 for error.
 */
int aeron_controlled_fragment_assembler_delete(aeron_controlled_fragment_assembler_t *assembler);

/**
 * Handler function to be passed for handling fragment assembly.
 *
 * @param clientd passed in the poll call (must be a aeron_controlled_fragment_assembler_t)
 * @param buffer containing the data.
 * @param length of the data in bytes.
 * @param header representing the meta data for the data.
 * @return The action to be taken with regard to the stream position after the callback.
 */
aeron_controlled_fragment_handler_action_t aeron_controlled_fragment_assembler_handler(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header);

/**
 * Counter functions
 */

/**
 * Return a pointer to the counter value.
 *
 * @param counter to pointer to.
 * @return pointer to the counter value.
 */
int64_t *aeron_counter_addr(aeron_counter_t *counter);

/**
 * Configuration for a counter that does not change during it's lifetime.
 */
typedef struct aeron_counter_constants_stct
{
    /**
     * Return the registration id used to register this counter with the media driver.
     */
    int64_t registration_id;

    /**
     * Identity for the counter within the counters reader and counters manager.
     */
    int32_t counter_id;
}
aeron_counter_constants_t;

/**
 * Fill in a structure with the constants in use by a counter.
 *
 * @param counter to get the constants for.
 * @param constants structure to fill in with the constants.
 * @return 0 for success and -1 for error.
 */
int aeron_counter_constants(aeron_counter_t *counter, aeron_counter_constants_t *constants);

/**
 * Asynchronously close the counter.
 *
 * @param counter to close.
 * @return 0 for success or -1 for error.
 */
int aeron_counter_close(
    aeron_counter_t *counter, aeron_notification_t on_close_complete, void *on_close_complete_clientd);

/**
 * Check if the counter is closed
 * @param counter to check
 * @return true if closed, false otherwise.
 */
bool aeron_counter_is_closed(aeron_counter_t *counter);

/**
 * Return full version and build string.
 *
 * @return full version and build string.
 */
const char *aeron_version_full(void);

/**
 * Return version text.
 *
 * @return version text.
 */
const char *aeron_version_text(void);

/**
 * Return major version number.
 *
 * @return major version number.
 */
int aeron_version_major(void);

/**
 * Return minor version number.
 *
 * @return minor version number.
 */
int aeron_version_minor(void);

/**
 * Return patch version number.
 *
 * @return patch version number.
 */
int aeron_version_patch(void);

/**
 * Return the git sha for the current build.
 *
 * @return git version
 */
const char *aeron_version_gitsha(void);

/**
 * Clock function used by aeron.
 */
typedef int64_t (*aeron_clock_func_t)(void);

/**
 * Return time in nanoseconds for machine. Is not wall clock time.
 *
 * @return nanoseconds since epoch for machine.
 */
int64_t aeron_nano_clock(void);

/**
 * Return time in milliseconds since epoch. Is wall clock time.
 *
 * @return milliseconds since epoch.
 */
int64_t aeron_epoch_clock(void);

/**
 * Function to return logging information.
 */
typedef void (*aeron_log_func_t)(const char *);

/**
 * Determine if an aeron driver is using a given aeron directory.
 *
 * @param dirname  for aeron directory
 * @param timeout_ms  to use to determine activity for aeron directory
 * @param log_func to call during activity check to log diagnostic information.
 * @return true for active driver or false for no active driver.
 */
bool aeron_is_driver_active(const char *dirname, int64_t timeout_ms, aeron_log_func_t log_func);

/**
 * Load properties from a string containing name=value pairs and set appropriate environment variables for the
 * process so that subsequent calls to aeron_driver_context_init will use those values.
 *
 * @param buffer containing properties and values.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_buffer_load(const char *buffer);

/**
 * Load properties file and set appropriate environment variables for the process so that subsequent
 * calls to aeron_driver_context_init will use those values.
 *
 * @param filename to load.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_file_load(const char *filename);

/**
 * Load properties from HTTP URL and set environment variables for the process so that subsequent
 * calls to aeron_driver_context_init will use those values.
 *
 * @param url to attempt to retrieve and load.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_http_load(const char *url);

/**
 * Load properties based on URL or filename. If string contains file or http URL, it will attempt
 * to load properties from a file or http as indicated. If not a URL, then it will try to load the string
 * as a filename.
 *
 * @param url_or_filename to load properties from.
 * @return 0 for success and -1 for error.
 */
int aeron_properties_load(const char *url_or_filename);

/**
 * Return current aeron error code (errno) for calling thread.
 *
 * @return aeron error code for calling thread.
 */
int aeron_errcode(void);

/**
 * Return the current aeron error message for calling thread.
 *
 * @return aeron error message for calling thread.
 */
const char *aeron_errmsg(void);

/**
 * Get the default path used by the Aeron media driver.
 *
 * @param path buffer to store the path.
 * @param path_length space available in the buffer
 * @return -1 if there is an issue or the number of bytes written to path excluding the terminator <code>\0</code>. If this
 * is equal to or greater than the path_length then the path has been truncated.
 */
int aeron_default_path(char *path, size_t path_length);

/**
 * Gets the registration id for addition of the counter. Note that using this after a call to poll the succeeds or
 * errors is undefined behaviour. As the async_add_counter_t may have been freed.
 *
 * @param add_counter used to check for completion.
 * @return registration id for the counter.
 */
int64_t aeron_async_add_counter_get_registration_id(aeron_async_add_counter_t *add_counter);

/**
 * Gets the registration id for addition of the publication. Note that using this after a call to poll the succeeds or
 * errors is undefined behaviour. As the async_add_publication_t may have been freed.
 *
 * @param add_publication used to check for completion.
 * @return registration id for the publication.
 */
int64_t aeron_async_add_publication_get_registration_id(aeron_async_add_publication_t *add_publication);

/**
 * Gets the registration id for addition of the exclusive_publication. Note that using this after a call to poll the
 * succeeds or errors is undefined behaviour. As the async_add_exclusive_publication_t may have been freed.
 *
 * @param add_exclusive_publication used to check for completion.
 * @return registration id for the exclusive_publication.
 * @deprecated Use aeron_async_add_exclusive_publication_get_registration_id instead.
 */
int64_t aeron_async_add_exclusive_exclusive_publication_get_registration_id(
    aeron_async_add_exclusive_publication_t *add_exclusive_publication);

/**
 * Gets the registration id for addition of the exclusive_publication. Note that using this after a call to poll the
 * succeeds or errors is undefined behaviour. As the async_add_exclusive_publication_t may have been freed.
 *
 * @param add_exclusive_publication used to check for completion.
 * @return registration id for the exclusive_publication.
 */
int64_t aeron_async_add_exclusive_publication_get_registration_id(
    aeron_async_add_exclusive_publication_t *add_exclusive_publication);

/**
 * Gets the registration id for addition of the subscription. Note that using this after a call to poll the succeeds or
 * errors is undefined behaviour. As the async_add_subscription_t may have been freed.
 *
 * @param add_subscription used to check for completion.
 * @return registration id for the subscription.
 */
int64_t aeron_async_add_subscription_get_registration_id(aeron_async_add_subscription_t *add_subscription);

/**
 * Gets the registration_id for the destination command supplied. Note that this is the correlation_id used for
 * the specified destination command, not the registration_id for the original parent resource (publication,
 * subscription).
 *
 * @param async_destination tracking the current destination command.
 * @return correlation_id sent to driver.
 */
int64_t aeron_async_destination_get_registration_id(aeron_async_destination_t *async_destination);

/**
 * Request the media driver terminates operation and closes all resources.
 *
 * @param directory    in which the media driver is running.
 * @param token_buffer containing the authentication token confirming the client is allowed to terminate the driver.
 * @param token_length of the token in the buffer.
 * @return
 */
int aeron_context_request_driver_termination(const char *directory, const uint8_t *token_buffer, size_t token_length);

typedef struct aeron_cnc_stct aeron_cnc_t;

#pragma pack(push)
#pragma pack(4)
typedef struct aeron_cnc_constants_stct
{
    int32_t cnc_version;
    int32_t to_driver_buffer_length;
    int32_t to_clients_buffer_length;
    int32_t counter_metadata_buffer_length;
    int32_t counter_values_buffer_length;
    int32_t error_log_buffer_length;
    int64_t client_liveness_timeout;
    int64_t start_timestamp;
    int64_t pid;
    int32_t file_page_size;
}
aeron_cnc_constants_t;
#pragma pack(pop)

/**
 * Initialise an aeron_cnc, which gives user level access to the command and control file used to communicate
 * with the media driver. Will wait until the media driver has loaded and the cnc file is created, up to timeout_ms.
 * Use a value of 0 for a non-blocking initialisation.
 *
 * @param aeron_cnc to hold the loaded aeron_cnc
 * @param base_path media driver's base path
 * @param timeout_ms Number of milliseconds to wait before timing out.
 * @return 0 on success, -1 on failure.
 */
int aeron_cnc_init(aeron_cnc_t **aeron_cnc, const char *base_path, int64_t timeout_ms);

/**
 * Fetch the sets of constant values associated with this command and control file.
 *
 * @param aeron_cnc to query
 * @param constants user supplied structure to hold return values.
 * @return 0 on success, -1 on failure.
 */
int aeron_cnc_constants(aeron_cnc_t *aeron_cnc, aeron_cnc_constants_t *constants);

/**
 * Get the current file name of the cnc file.
 *
 * @param aeron_cnc to query
 * @return name of the cnc file
 */
const char *aeron_cnc_filename(aeron_cnc_t *aeron_cnc);

/**
 * Gets the timestamp of the last heartbeat sent to the media driver from any client.
 *
 * @param aeron_cnc to query
 * @return last heartbeat timestamp in ms.
 */
int64_t aeron_cnc_to_driver_heartbeat(aeron_cnc_t *aeron_cnc);

typedef void (*aeron_error_log_reader_func_t)(
    int32_t observation_count,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    const char *error,
    size_t error_length,
    void *clientd);

/**
 * Reads the current error log for this driver.
 *
 * @param aeron_cnc to query
 * @param callback called for every distinct error observation
 * @param clientd client data to be passed to the callback
 * @param since_timestamp only return errors after this timestamp (0 returns all)
 * @return the number of distinct errors seen
 */
size_t aeron_cnc_error_log_read(
    aeron_cnc_t *aeron_cnc,
    aeron_error_log_reader_func_t callback,
    void *clientd,
    int64_t since_timestamp);

/**
 * Gets a counters reader for this command and control file. This does not need to be closed manually, resources
 * are tied to the instance of aeron_cnc.
 *
 * @param aeron_cnc to query
 * @return pointer to a counters reader.
 */
aeron_counters_reader_t *aeron_cnc_counters_reader(aeron_cnc_t *aeron_cnc);

typedef void (*aeron_loss_reporter_read_entry_func_t)(
    void *clientd,
    int64_t observation_count,
    int64_t total_bytes_lost,
    int64_t first_observation_timestamp,
    int64_t last_observation_timestamp,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    int32_t channel_length,
    const char *source,
    int32_t source_length);

/**
 * Read all of the data loss observations from the report in the same media driver instances as the cnc file.
 *
 * @param aeron_cnc to query
 * @param entry_func callback for each observation found
 * @param clientd client data to be passed to the callback.
 * @return -1 on failure, number of observations on success (could be 0).
 */
int aeron_cnc_loss_reporter_read(
    aeron_cnc_t *aeron_cnc, aeron_loss_reporter_read_entry_func_t entry_func, void *clientd);

/**
 * Closes the instance of the aeron cnc and frees its resources.
 *
 * @param aeron_cnc to close
 */
void aeron_cnc_close(aeron_cnc_t *aeron_cnc);

#ifdef __cplusplus
}
#endif

#endif //AERON_C_H
