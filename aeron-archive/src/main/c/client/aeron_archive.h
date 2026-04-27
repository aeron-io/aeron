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

#ifndef AERON_ARCHIVE_H
#define AERON_ARCHIVE_H

#ifdef __cplusplus
extern "C"
{
#endif

#include "aeronc.h"
#include "aeron_common.h"

#define ARCHIVE_ERROR_CODE_GENERIC (0)
#define ARCHIVE_ERROR_CODE_ACTIVE_LISTING (1)
#define ARCHIVE_ERROR_CODE_ACTIVE_RECORDING (2)
#define ARCHIVE_ERROR_CODE_ACTIVE_SUBSCRIPTION (3)
#define ARCHIVE_ERROR_CODE_UNKNOWN_SUBSCRIPTION (4)
#define ARCHIVE_ERROR_CODE_UNKNOWN_RECORDING (5)
#define ARCHIVE_ERROR_CODE_UNKNOWN_REPLAY (6)
#define ARCHIVE_ERROR_CODE_MAX_REPLAYS (7)
#define ARCHIVE_ERROR_CODE_MAX_RECORDINGS (8)
#define ARCHIVE_ERROR_CODE_INVALID_EXTENSION (9)
#define ARCHIVE_ERROR_CODE_AUTHENTICATION_REJECTED (10)
#define ARCHIVE_ERROR_CODE_STORAGE_SPACE (11)
#define ARCHIVE_ERROR_CODE_UNKNOWN_REPLICATION (12)
#define ARCHIVE_ERROR_CODE_UNAUTHORISED_ACTION (13)

#define AERON_NULL_POSITION AERON_NULL_VALUE

/**
 * When replaying a live recording, replay the whole stream and follow the live recording. This will behave the
 * same way as providing AERON_NULL_VALUE
 */
#define ARCHIVE_REPLAY_ALL_AND_FOLLOW (-1)

/**
 * When replaying a live recording, replay up to the current limit then stop the replay and end the stream.
 */
#define ARCHIVE_REPLAY_ALL_AND_STOP (-2)

typedef struct aeron_archive_stct aeron_archive_t;
typedef struct aeron_archive_context_stct aeron_archive_context_t;
typedef struct aeron_archive_async_connect_stct aeron_archive_async_connect_t;

typedef struct aeron_archive_encoded_credentials_stct
{
    const char *data;
    uint32_t length;
}
aeron_archive_encoded_credentials_t;

/**
 * Callback to return encoded credentials.
 *
 * @return encoded credentials to include with the connect request
 */
typedef aeron_archive_encoded_credentials_t *(*aeron_archive_credentials_encoded_credentials_supplier_func_t)(void *clientd);

/**
 * Callback to return encoded credentials given a specific encoded challenge.
 *
 * @param encoded_challenge to use to generate the encoded credentials
 * @return encoded credentials to include with the challenge response
 */
typedef aeron_archive_encoded_credentials_t *(*aeron_archive_credentials_challenge_supplier_func_t)(
    aeron_archive_encoded_credentials_t *encoded_challenge,
    void *clientd);

/**
 * Callback to return encoded credentials so they may be reused or freed.
 *
 * @param credentials to reuse or free
 */
typedef void (*aeron_archive_credentials_free_func_t)(
    aeron_archive_encoded_credentials_t *credentials,
    void *clientd);

/**
 * Callback to allow execution of a delegating invoker to be run.
 */
typedef void (*aeron_archive_delegating_invoker_func_t)(void *clientd);

/**
 * Struct containing the available replay parameters.
 */
typedef struct aeron_archive_replay_params_stct
{
    /**
     * Set the counter id to be used for bounding the replay.
     * Setting this value will trigger the sending of a bounded replay request, instead of a normal replay.
     * By default, a bound will not be applied.
     */
    int32_t bounding_limit_counter_id;

    /**
     * The maximum size of a file operation when reading from the archive to execute the replay.
     */
    int32_t file_io_max_length;

    /**
     * The position at which to start the replay.
     * By default, the stream is replayed from the start.
     */
    int64_t position;

    /**
     * The length of the recorded stream to replay.
     * By default, the whole stream will be replayed.
     * If set to INT64_MAX, it will follow a live recording.
     */
    int64_t length;

    /**
     * The token used for replays when the initiating image is not the one used to create the archive connection/session.
     */
    int64_t replay_token;

    /**
     * The subscription registration id used when doing a start replay using response channels and the response channel is already created.
     */
    int64_t subscription_registration_id;
}
aeron_archive_replay_params_t;

/**
 * Initialize an aeron_archive_replay_params_t with the default values.
 */
int aeron_archive_replay_params_init(aeron_archive_replay_params_t *params);

/**
 * Struct containing the available replication parameters.
 */
typedef struct aeron_archive_replication_params_stct
{
    /**
     * The stop position for the replication.
     * The default of AERON_NULL_VALUE indicates a continuous replication.
     */
    int64_t stop_position;

    /**
     * The recording id of the destination Archive to extend.
     * The default of AERON_NULL_VALUE triggers the creation of a new recording at the destination Archive.
     */
    int64_t dst_recording_id;

    /**
     * Specify the destination for the live stream if a merge is required.
     * The default of an empty string means no merge will occur.
     */
    const char *live_destination;

    /**
     * Specify the channel to use to replicate the recording.
     * The default of an empty string will trigger the use of the context's default replication channel.
     */
    const char *replication_channel;

    /**
     * Specify the control address of the source archive when using response channels during replication.
     */
    const char *src_response_channel;

    /**
     * Specify a tag to apply to the channel used by the Archive's subscription for replication.
     */
    int64_t channel_tag_id;

    /**
     * Specify a subscription tag to apply to the channel used by the Archive's subscription for replication.
     */
    int64_t subscription_tag_id;

    /**
     * Specify the max length for file IO operations used in the replay.
     */
    int32_t file_io_max_length;

    /**
     * Specify session id to be used for the replicated file instead of the session id from the source archive.
     * This is useful in cases where we are replicating the same recording in multiple stages.
     */
    int32_t replication_session_id;

    /**
     * Specify the encoded credentials that will be passed to the source archive for authentication.
     * Currently, only simple authentication (i.e. not challenge/response) is supported for replication.
     */
    aeron_archive_encoded_credentials_t *encoded_credentials;
}
aeron_archive_replication_params_t;

/**
 * Initialize an aeron_archive_replication_params_t with the default values
 */
int aeron_archive_replication_params_init(aeron_archive_replication_params_t *params);

/**
 * Struct containing the details of a recording
 */
typedef struct aeron_archive_recording_descriptor_stct
{
    /// control session id of the request
    int64_t control_session_id;

    /// correlation id of the request
    int64_t correlation_id;

    /// id of the recording
    int64_t recording_id;

    /// timestamp of recording start
    int64_t start_timestamp;

    /// timestamp of recording stop
    int64_t stop_timestamp;

    /// the start position of the recording against the recorded publication
    int64_t start_position;

    /// the highest position reached for this recording
    int64_t stop_position;

    /// the initial term id of the recorded publication
    int32_t initial_term_id;

    /// the segment file length - a multiple of the term_buffer_length
    int32_t segment_file_length;

    /// term buffer length of the publication
    int32_t term_buffer_length;

    /// mtu length of the recorded publication
    int32_t mtu_length;

    /// session id of the recorded publication
    int32_t session_id;

    /// stream id of the recorded publication
    int32_t stream_id;

    /// channel used for recording subscription at the Aeron Archive
    char *stripped_channel;

    /// length of the stripped_channel string
    size_t stripped_channel_length;

    /// channel provided to start the recording request
    char *original_channel;

    /// length of the original_channel string
    size_t original_channel_length;

    /// source identity of the recorded stream
    char *source_identity;

    /// length of the source_identity string
    size_t source_identity_length;
}
aeron_archive_recording_descriptor_t;

/**
 * Callback to return recording descriptors.
 */
typedef void (*aeron_archive_recording_descriptor_consumer_func_t)(
    aeron_archive_recording_descriptor_t *recording_descriptor,
    void *clientd);

/**
 * Struct containing the details of a recording subscription
 */
typedef struct aeron_archive_recording_subscription_descriptor_stct
{
    /// control session id of the request
    int64_t control_session_id;

    /// correlation id of the request
    int64_t correlation_id;

    /// the subscription id - can be used to stop the recording subscription
    int64_t subscription_id;

    /// the stream id the subscription was registered with
    int32_t stream_id;

    /// the channel the subscription was registered with
    char *stripped_channel;

    /// the length of the stripped_channel string
    size_t stripped_channel_length;
}
aeron_archive_recording_subscription_descriptor_t;

/**
 * Callback to return recording subscription descriptors.
 */
typedef void (*aeron_archive_recording_subscription_descriptor_consumer_func_t)(
    aeron_archive_recording_subscription_descriptor_t *recording_subscription_descriptor,
    void *clientd);

typedef enum aeron_archive_client_recording_signal_en
{
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_START = INT32_C(0),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP = INT32_C(1),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND = INT32_C(2),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE = INT32_C(3),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_MERGE = INT32_C(4),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC = INT32_C(5),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE = INT32_C(6),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END = INT32_C(7),
    AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE = INT32_MIN
}
aeron_archive_client_recording_signal_t;

/**
 * Struct containing the details of a recording signal.
 */
typedef struct aeron_archive_recording_signal_stct
{
    /// Control session id of the originating session.
    int64_t control_session_id;

    /// Recording ID of the recording which transitioned.
    int64_t recording_id;

    /// Subscription ID of the subscription which captured the recording.
    int64_t subscription_id;

    /// The position of the recording at the time of transition.
    int64_t position;

    /// Raw code representing the operation the recording has undertaken.
    int32_t recording_signal_code;
}
aeron_archive_recording_signal_t;

/**
 * Callback to return recording signals.
 */
typedef void (*aeron_archive_recording_signal_consumer_func_t)(
    aeron_archive_recording_signal_t *recording_signal,
    void *clientd);

typedef enum aeron_archive_source_location_en
{
    AERON_ARCHIVE_SOURCE_LOCATION_LOCAL = 0,
    AERON_ARCHIVE_SOURCE_LOCATION_REMOTE = 1
}
aeron_archive_source_location_t;

/* context */

/**
 * Create an aeron_archive_context_t struct.
 *
 * @param ctx context to create and initialize
 */
int aeron_archive_context_init(aeron_archive_context_t **ctx);

/**
 * Close and delete the aeron_archive_context_t struct.
 *
 * @param ctx context to delete
 */
int aeron_archive_context_close(aeron_archive_context_t *ctx);

/**
 * Specify the client used for communicating with the local Media Driver.
 * <p>
 * This client will be closed with the aeron_archive_t is closed if aeron_archive_context_set_owns_aeron_client is true.
 */
int aeron_archive_context_set_aeron(aeron_archive_context_t *ctx, aeron_t *aeron);
aeron_t *aeron_archive_context_get_aeron(aeron_archive_context_t *ctx);

/**
 * Specify whether or not this context owns the client and, therefore, takes responsibility for closing it.
 */
int aeron_archive_context_set_owns_aeron_client(aeron_archive_context_t *ctx, bool owns_aeron_client);
bool aeron_archive_context_get_owns_aeron_client(aeron_archive_context_t *ctx);

/**
 * Specify the top level Aeron directory used for communication between the Aeron client and the Media Driver.
 */
int aeron_archive_context_set_aeron_directory_name(aeron_archive_context_t *ctx, const char *aeron_directory_name);
const char *aeron_archive_context_get_aeron_directory_name(aeron_archive_context_t *ctx);

/**
 * Specify the channel used for sending requests to the Aeron Archive.
 */
int aeron_archive_context_set_control_request_channel(aeron_archive_context_t *ctx, const char *control_request_channel);
const char *aeron_archive_context_get_control_request_channel(aeron_archive_context_t *ctx);

/**
 * Specify the stream used for sending requests to the Aeron Archive.
 */
int aeron_archive_context_set_control_request_stream_id(aeron_archive_context_t *ctx, int32_t control_request_stream_id);
int32_t aeron_archive_context_get_control_request_stream_id(aeron_archive_context_t *ctx);

/**
 * Specify the channel used for receiving responses from the Aeron Archive.
 */
int aeron_archive_context_set_control_response_channel(aeron_archive_context_t *ctx, const char *control_response_channel);
const char *aeron_archive_context_get_control_response_channel(aeron_archive_context_t *ctx);

/**
 * Specify the stream used for receiving responses from the Aeron Archive.
 */
int aeron_archive_context_set_control_response_stream_id(aeron_archive_context_t *ctx, int32_t control_response_stream_id);
int32_t aeron_archive_context_get_control_response_stream_id(aeron_archive_context_t *ctx);

/**
 * Specify the channel used for receiving recording events from the Aeron Archive.
 */
int aeron_archive_context_set_recording_events_channel(aeron_archive_context_t *ctx, const char *recording_events_channel);
const char *aeron_archive_context_get_recording_events_channel(aeron_archive_context_t *ctx);

/**
 * Specify the stream id used for recording events channel.
 */
int aeron_archive_context_set_recording_events_stream_id(aeron_archive_context_t *ctx, int32_t recording_events_stream_id);
int32_t aeron_archive_context_get_recording_events_stream_id(aeron_archive_context_t *ctx);

/**
 * Specify the message timeout, in nanoseconds, to wait for sending or receiving a message.
 */
int aeron_archive_context_set_message_timeout_ns(aeron_archive_context_t *ctx, uint64_t message_timeout_ns);
uint64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx);

/**
 * Specify the number of retry attempts when offering messages to the archive.
 */
int aeron_archive_context_set_message_retry_attempts(aeron_archive_context_t *ctx, uint32_t message_retry_attempts);
uint32_t aeron_archive_context_get_message_retry_attempts(aeron_archive_context_t *ctx);

/**
 * Specify the default term buffer length for the control request/response channels.
 */
int aeron_archive_context_set_control_term_buffer_length(aeron_archive_context_t *ctx, size_t control_term_buffer_length);
size_t aeron_archive_context_get_control_term_buffer_length(aeron_archive_context_t *ctx);

/**
 * Specify the default MTU length for the control request/response channels.
 */
int aeron_archive_context_set_control_mtu_length(aeron_archive_context_t *ctx, size_t control_mtu_length);
size_t aeron_archive_context_get_control_mtu_length(aeron_archive_context_t *ctx);

/**
 * Specify the default MTU length for the control request/response channels.
 */
int aeron_archive_context_set_control_term_buffer_sparse(aeron_archive_context_t *ctx, bool control_term_buffer_sparse);
bool aeron_archive_context_get_control_term_buffer_sparse(aeron_archive_context_t *ctx);

/**
 * Specify client name to identify this client on the archive side.
 */
int aeron_archive_context_set_client_name(aeron_archive_context_t *context, const char *value);
const char *aeron_archive_context_get_client_name(aeron_archive_context_t *context);

/**
 * Specify the idle strategy function and associated state used by the client between polling calls.
 */
int aeron_archive_context_set_idle_strategy(
    aeron_archive_context_t *ctx,
    aeron_idle_strategy_func_t idle_strategy_func,
    void *idle_strategy_state);

/**
 * Specify the various credentials callbacks to use when connecting to the Aeron Archive.
 */
int aeron_archive_context_set_credentials_supplier(
    aeron_archive_context_t *ctx,
    aeron_archive_credentials_encoded_credentials_supplier_func_t encoded_credentials,
    aeron_archive_credentials_challenge_supplier_func_t on_challenge,
    aeron_archive_credentials_free_func_t on_free,
    void *clientd);

/**
 * Specify the callback to which recording signals are dispatched while polling for control responses.
 */
int aeron_archive_context_set_recording_signal_consumer(
    aeron_archive_context_t *ctx,
    aeron_archive_recording_signal_consumer_func_t on_recording_signal,
    void *clientd);

/**
 * Specify the callback to which errors are dispatched while executing archive client commands.
 */
int aeron_archive_context_set_error_handler(
    aeron_archive_context_t *ctx,
    aeron_error_handler_t error_handler,
    void *clientd);

/**
 * Specify the callback to be invoked in addition to any invoker used by the Aeron instance.
 * <p>
 * Useful when running in a low thread count environment.
 */
int aeron_archive_context_set_delegating_invoker(
    aeron_archive_context_t *ctx,
    aeron_archive_delegating_invoker_func_t delegating_invoker_func,
    void *clientd);

/* client */

/**
 * Begin an attempt at creating a connection which can be completed by calling aeron_archive_async_connect_poll.
 *
 * @param async aeron_archive_async_connect_t to create and initialize
 * @param ctx aeron_archive_context_t for the archive connection
 */
int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);

/**
 * Poll for a complete connection.
 *
 * @param aeron_archive aeron_archive_t that will be created/initialized upon successful connection
 * @param async aeron_archive_async_connect_t to poll
 * @return -1 for failure, 0 for 'try again', and 1 for success
 * <p>
 * Note that after a return of either -1 or 1, the provided aeron_archive_async_connect_t will have been deleted.
 * <p>
 * Also note that after a return of 1, the aeron_archive pointer will be set to a ready to use aeron_archive_t.
 */
int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async);

/**
 * Connect to an Aeron Archive.
 *
 * @param aeron_archive aeron_archive_t that will be created/initialized upon successful connection
 * @param ctx aeron_archive_context_t for the archive connection
 */
int aeron_archive_connect(aeron_archive_t **aeron_archive, aeron_archive_context_t *ctx);

/**
 * Close the connection to the Aeron Archive and free up associated resources.
 */
int aeron_archive_close(aeron_archive_t *aeron_archive);

/**
 * Retrieve the underlying aeron_archive_context_t used to configure the provided aeron_archive_t.
 */
aeron_archive_context_t *aeron_archive_get_archive_context(aeron_archive_t *aeron_archive);

/**
 * Retrieve the underlying aeron_archive_context_t used to configure the provided aeron_archive_t.
 * <p>
 * Additionally, calling this function transfers ownership of the returned aeron_archive_context_t to the caller.
 * i.e. it is now the the caller's responsibility to close the context.
 * This is useful when wrapping the C library in other, higher level languages.
 */
aeron_archive_context_t *aeron_archive_get_and_own_archive_context(aeron_archive_t *aeron_archive);

/**
 * Retrieve the archive id of the connected Aeron Archive.
 */
int64_t aeron_archive_get_archive_id(aeron_archive_t *aeron_archive);

/**
 * Retrieve the underlying aeron_subscription_t used for reading responses from the connected Aeron Archive.
 */
aeron_subscription_t *aeron_archive_get_control_response_subscription(aeron_archive_t *aeron_archive);

/**
 * Retrieve the underlying aeron_subscription_t used for reading responses from the connected Aeron Archive.
 * <p>
 * Additionally, calling this function transfers ownership of the returned aeron_subscription_t to the caller.
 * i.e. it is now the caller's responsibility to close the subscription.
 * This is useful when wrapping the C library in other, high level languages.
 */
aeron_subscription_t *aeron_archive_get_and_own_control_response_subscription(aeron_archive_t *aeron_archive);

// helpful for testing... not necessarily useful otherwise
int64_t aeron_archive_control_session_id(aeron_archive_t *aeron_archive);

/**
 * Poll for recording signals, dispatching them to the configured aeron_archive_recording_signal_consumer_func_t in the context
 *
 * @param count_p out param that indicates the number of recording signals dispatched.
 * @return 0 for success, -1 for failure.
 */
int aeron_archive_poll_for_recording_signals(int32_t *count_p, aeron_archive_t *aeron_archive);

/**
 * Poll the response stream once for an error.
 * If another message is present then it will be skipped over, so only call when not expecting another response.
 *
 * @return 0 if an error sent from the Aeron Archive is found, in which case, the provided buffer contains the error message.
 * If there was no error, the buffer will be an empty string.
 * <p>
 * -1 if an error occurs while attempting to read from the subscription.
 */
int aeron_archive_poll_for_error_response(aeron_archive_t *aeron_archive, char *buffer, size_t buffer_length);

/**
 * Poll the response stream once for an error.
 *
 * @return 0 if no error is found OR if an error is found but an error handler is specified in the context.
 * <p>
 * -1 if an error is found and no error handler is specified.  The error message can be retrieved by calling aeron_errmsg()
 */
int aeron_archive_check_for_error_response(aeron_archive_t *aeron_archive);

/**
 * Add a publication and set it up to be recorded.
 *
 * @param publication_p out param set to the aeron_publication_t upon success
 * @param aeron_archive the archive client
 * @param channel the channel for the publication
 * @param stream_id the stream id for the publication
 */
int aeron_archive_add_recorded_publication(
    aeron_publication_t **publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

/**
 * Add an exclusive publication and set it up to be recorded.
 *
 * @param publication_p out param set to the aeron_exclusive_publication_t upon success
 * @param aeron_archive the archive client
 * @param channel the channel for the exclusive publication
 * @param stream_id the stream id for the exclusive publication
 * @return 0 for success, -1 for failure
 */
int aeron_archive_add_recorded_exclusive_publication(
    aeron_exclusive_publication_t **exclusive_publication_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

/**
 * Start recording a channel/stream pairing.
 * <p>
 * Channels that include session id parameters are considered different than channels without session ids.
 * If a publication matches both a session id specific channel recording and a non session id specific recording,
 * it will be recorded twice.
 *
 * @param subscription_id_p out param set to the subscription id of the recording
 * @param aeron_archive the archive client
 * @param recording_channel the channel of the publication to be recorded
 * @param recording_stream_id the stream id of the publication to be recorded
 * @param source_location the source location of the publication to be recorded
 * @param auto_stop should the recording be automatically stopped when complete
 * @return 0 for success, -1 for failure
 */
int aeron_archive_start_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop);

/**
 * Fetch the position recorded for the specified recording.
 *
 * @param recording_position_p out param set to the recording position of the specified recording
 * @param aeron_archive the archive client
 * @param recording_id the active recording id
 * @return 0 for success, -1 for failure
 */
int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Fetch the start position for the specified recording.
 *
 * @param start_position_p out param set to the start position of the specified recording
 * @param aeron_archive the archive client
 * @param recording_id the active recording id
 * @return 0 for success, -1 for failure
 */
int aeron_archive_get_start_position(
    int64_t *start_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Fetch the stop position for the specified recording.
 *
 * @param stop_position_p out param set to the stop position of the specified recording
 * @param aeron_archive the archive client
 * @param recording_id the active recording id
 * @return 0 for success, -1 for failure
 */
int aeron_archive_get_stop_position(
    int64_t *stop_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Fetch the stop or active position for the specified recording.
 *
 * @param max_recorded_position_p out param set to the stop or active position of the specified recording
 * @param aeron_archive the archive client
 * @param recording_id the active recording id
 * @return 0 for success, -1 for failure
 */
int aeron_archive_get_max_recorded_position(
    int64_t *max_recorded_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Stop recording for the specified subscription id.
 * This is the subscription id returned from aeron_archive_start_recording or aeron_archive_extend_recording.
 *
 * @param aeron_archive the archive client
 * @param subscription_id the subscription id for the recording in the Aeron Archive
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_recording_subscription(
    aeron_archive_t *aeron_archive,
    int64_t subscription_id);

/**
 * Try to stop a recording for the specified subscription id.
 * This is the subscription id returned from aeron_archive_start_recording or aeron_archive_extend_recording.
 *
 * @param stopped_p out param indicating true if stopped, or false if the subscription is not currently active
 * @param aeron_archive the archive client
 * @param subscription_id the subscription id for the recording in the Aeron Archive
 * @return 0 for success, -1 for failure
 */
int aeron_archive_try_stop_recording_subscription(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t subscription_id);

/**
 * Stop recording for the specified channel and stream.
 * <p>
 * Channels that include session id parameters are considered different than channels without session ids.
 * Stopping a recording on a channel without a session id parameter will not stop the recording of any
 * session id specific recordings that use the same channel and stream id.
 *
 * @param aeron_archive the archive client
 * @param channel the channel of the recording to be stopped
 * @param stream_id the stream id of the recording to be stopped
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_recording_channel_and_stream(
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

/**
 * Try to stop recording for the specified channel and stream.
 * <p>
 * Channels that include session id parameters are considered different than channels without session ids.
 * Stopping a recording on a channel without a session id parameter will not stop the recording of any
 * session id specific recordings that use the same channel and stream id.
 *
 * @param stopped_p out param indicating true if stopped, or false if the channel/stream pair is not currently active
 * @param aeron_archive the archive client
 * @param channel the channel of the recording to be stopped
 * @param stream_id the stream id of the recording to be stopped
 * @return 0 for success, -1 for failure
 */
int aeron_archive_try_stop_recording_channel_and_stream(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    const char *channel,
    int32_t stream_id);

/**
 * Stop recording for the specified recording id.
 *
 * @param stopped_p out param indicating true if stopped, or false if the recording is not currently active
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording to be stopped
 * @return 0 for success, -1 for failure
 */
int aeron_archive_try_stop_recording_by_identity(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Stop recording a session id specific recording that pertains to the given publication.
 *
 * @param aeron_archive the archive client
 * @param publication the publication to stop recording
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_recording_publication(
    aeron_archive_t *aeron_archive,
    aeron_publication_t *publication);

/**
 * Stop recording a session id specific recording that pertains to the given exclusive publication.
 *
 * @param aeron_archive the archive client
 * @param exclusive_publication the exclusive publication to stop recording
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_recording_exclusive_publication(
    aeron_archive_t *aeron_archive,
    aeron_exclusive_publication_t *exclusive_publication);

/**
 * Find the last recording that matches the given criteria.
 *
 * @param recording_id_p out param for the recording id that matches
 * @param aeron_archive the archive client
 * @param min_recording_id the lowest recording id to search back to
 * @param channel_fragment for a 'contains' match on the original channel stored with the Aeron Archive
 * @param stream_id the stream id of the recording
 * @param session_id the session id of the recording
 * @return 0 for success, -1 for failure
 */
int aeron_archive_find_last_matching_recording(
    int64_t *recording_id_p,
    aeron_archive_t *aeron_archive,
    int64_t min_recording_id,
    const char *channel_fragment,
    int32_t stream_id,
    int32_t session_id);

/**
 * List a recording descriptor for a single recording id.
 *
 * @param count_p out param indicating the number of descriptors found
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording
 * @param recording_descriptor_consumer to be called for each descriptor
 * @param recording_descriptor_consumer_clientd to be passed for each descriptor
 * @return 0 for success, -1 for failure
 */
int aeron_archive_list_recording(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

/**
 * List all recording descriptors starting at a particular recording id, with a limit of total descriptors delivered.
 *
 * @param count_p out param indicating the number of descriptors found
 * @param aeron_archive the archive client
 * @param from_recording_id the id at which to begin the listing
 * @param record_count the limit of total descriptors to deliver
 * @param recording_descriptor_consumer to be called for each descriptor
 * @param recording_descriptor_consumer_clientd to be passed for each descriptor
 * @return 0 for success, -1 for failure
 */
int aeron_archive_list_recordings(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

/**
 * List all recording descriptors for a given channel fragment and stream id, starting at a particular recording id, with a limit of total descriptors delivered.
 *
 * @param count_p out param indicating the number of descriptors found
 * @param aeron_archive the archive client
 * @param from_recording_id the id at which to begin the listing
 * @param record_count the limit of total descriptors to deliver
 * @param channel_fragment for a 'contains' match on the original channel stored with the Aeron Archive
 * @param stream_id the stream id of the recording
 * @param recording_descriptor_consumer to be called for each descriptor
 * @param recording_descriptor_consumer_clientd to be passed for each descriptor
 * @return 0 for success, -1 for failure
 */
int aeron_archive_list_recordings_for_uri(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t from_recording_id,
    int32_t record_count,
    const char *channel_fragment,
    int32_t stream_id,
    aeron_archive_recording_descriptor_consumer_func_t recording_descriptor_consumer,
    void *recording_descriptor_consumer_clientd);

/**
 * Start a replay
 * <p>
 * The lower 32-bits of the replay session id contain the session id of the image of the received replay
 * and can be obtained by casting the replay session id to an int32_t.
 * All 64-bits are required to uniquely identify the replay when calling aeron_archive_stop_replay.
 *
 * @param replay_session_id_p out param set to the replay session id
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording
 * @param replay_channel the channel to which the replay should be sent
 * @param replay_stream_id the stream id to which the replay should be sent
 * @param params the aeron_archive_replay_params_t that control the behaviour of the replay
 * @return 0 for success, -1 for failure
 */
int aeron_archive_start_replay(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

/**
 * Start a replay.
 *
 * @param subscription_p out param set to the subscription created for consuming the replay
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording
 * @param replay_channel the channel to which the replay should be sent
 * @param replay_stream_id the stream id to which the replay should be sent
 * @param params the aeron_archive_replay_params_t that control the behaviour of the replay
 * @return 0 for success, -1 for failure
 */
int aeron_archive_replay(
    aeron_subscription_t **subscription_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

/**
 * Truncate a stopped recording to the specified position.
 * The position must be less than the stopped position.
 * The position must be on a fragment boundary.
 * Truncating a recording to the start position effectively deletes the recording.
 *
 * @param count_p out param set to the number of segments deleted
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording
 * @param position the position to which the recording will be truncated
 * @return 0 for success, -1 for failure
 */
int aeron_archive_truncate_recording(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t position);

/**
 * Stop a replay session.
 *
 * @param aeron_archive the archive client
 * @param replay_session_id the replay session id indicating the replay to stop
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_replay(
    aeron_archive_t *aeron_archive,
    int64_t replay_session_id);

/**
 * Stop all replays matching a recording id.
 * If recording_id is AERON_NULL_VALUE then match all replays.
 *
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording for which all replays will be stopped
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_all_replays(
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * List active recording subscriptions in the Aeron Archive.
 * These are the result of calling aeron_archive_start_recording or aeron_archive_extend_recording.
 * The subscription id in the returned descriptor can be used when calling aeron_archive_stop_recording_subscription.
 *
 * @param count_p out param set to the count of matched subscriptions
 * @param aeron_archive the archive client
 * @param pseudo_index the index into the active list at which to begin listing
 * @param subscription_count the limit of total descriptors to deliver
 * @param channel_fragment for a 'contains' match on the original channel stored with the Aeron Archive
 * @param stream_id the stream id of the recording
 * @param apply_stream_id whether or not the stream id should be matched
 * @param recording_subscription_descriptor_consumer to be called for each descriptor
 * @param recording_subscription_descriptor_consumer_clientd to be passed for each descriptor
 * @return 0 for success, -1 for failure
 */
int aeron_archive_list_recording_subscriptions(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int32_t pseudo_index,
    int32_t subscription_count,
    const char *channel_fragment,
    int32_t stream_id,
    bool apply_stream_id,
    aeron_archive_recording_subscription_descriptor_consumer_func_t recording_subscription_descriptor_consumer,
    void *recording_subscription_descriptor_consumer_clientd);

/**
 * Purge a stopped recording.
 * i.e. Mark the recording as INVALID at the Archive and delete the corresponding segment files.
 * The space in the Catalog will be reclaimed upon compaction.
 *
 * @param deleted_segments_count_p out param set to the number of deleted segments
 * @param aeron_archive the archive client
 * @param recording_id the id of the stopped recording to be purged
 * @return 0 for success, -1 for failure
 */
int aeron_archive_purge_recording(
    int64_t *deleted_segments_count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Extend an existing, non-active recording for a channel and stream pairing.
 * <p>
 * The channel must be configured with the initial position from which it will be extended.
 * This can be done with aeron_uri_string_builder_set_initial_position.
 * The details required to initialize can be found by calling aeron_archive_list_recording.
 *
 * @param subscription_id_p out param set to the subscription id of the recording
 * @param aeron_archive the archive client
 * @param recording_id the id of the existing recording
 * @param recording_channel the channel of the publication to be recorded
 * @param recording_stream_id the stream id of the publication to be recorded
 * @param source_location the source location of the publication to be recorded
 * @param auto_stop should the recording be automatically stopped when complete
 * @return 0 for success, -1 for failure
 */
int aeron_archive_extend_recording(
    int64_t *subscription_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *recording_channel,
    int32_t recording_stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop);

/**
 * Replicate a recording from a source Archive to a destination.
 * This can be considered a backup for a primary Archive.
 * The source recording will be replayed via the provided replay channel and use the original stream id.
 * The behavior of the replication will be governed by the values specified in the aeron_archive_replication_params_t.
 * <p>
 * For a source recording that is still active, the replay can merge with the live stream and then follow it directly and no longer require the replay from the source.
 * This would require a multicast live destination.
 * <p>
 * Errors will be reported asynchronously and can be checked for with aeron_archive_check_for_error_response and aeron_archive_poll_for_error_response.
 *
 * @param replication_id_p out param set to the replication id that can be used to stop the replication
 * @param aeron_archive the archive client
 * @param src_recording_id the recording id that must exist at the source Archive
 * @param src_control_channel remote control channel for the source archive on which to instruct the replay
 * @param src_control_stream_id remote control stream id for the source archive on which to instruct the replay
 * @param params optional parameters to configure the behavior of the replication
 * @return 0 for success, -1 for failure
 */
int aeron_archive_replicate(
    int64_t *replication_id_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    const char *src_control_channel,
    int32_t src_control_stream_id,
    aeron_archive_replication_params_t *params);

/**
 * Stop a replication by the replication id.
 *
 * @param aeron_archive the archive client
 * @param replication_id the replication id retrieved when calling aeron_archive_replicate
 * @return 0 for success, -1 for failure
 */
int aeron_archive_stop_replication(
    aeron_archive_t *aeron_archive,
    int64_t replication_id);

/**
 * Try to stop a replication by the replication id.
 *
 * @param stopped_p out param indicating true if stopped, or false if the recording is not currently active
 * @param aeron_archive the archive client
 * @param replication_id the replication id retrieved when calling aeron_archive_replicate
 * @return 0 for success, -1 for failure
 */
int aeron_archive_try_stop_replication(
    bool *stopped_p,
    aeron_archive_t *aeron_archive,
    int64_t replication_id);

/**
 * Detach segments from the beginning of a recording up to the provided new start position.
 * <p>
 * The new start position must be the first byte position of a segment after the existing start position.
 * <p>
 * It is not possible to detach segments which are active for recording or being replayed.
 *
 * @param aeron_archive the archive client
 * @param recording_id the id of an existing recording
 * @param new_start_position the new starting position for the recording after the segments are detached
 * @return 0 for success, -1 for failure
 */
int aeron_archive_detach_segments(
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position);

/**
 * Delete segments which have been previously detached from a recording.
 *
 * @param count_p out param set to the number of segments deleted
 * @param aeron_archive the archive client
 * @param recording_id the id of an existing recording
 * @return 0 for success, -1 for failure
 */
int aeron_archive_delete_detached_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Purge (Detach and delete) segments from the beginning of a recording up to the provided new start position.
 * <p>
 * The new start position must be the first byte position of a segment after the existing start position.
 * <p>
 * It is not possible to detach segments which are active for recording or being replayed.
 *
 * @param count_p out param set to the number of segments deleted
 * @param aeron_archive the archive client
 * @param recording_id the id of an existing recording
 * @param new_start_position the new starting position for the recording after the segments are detached
 * @return 0 for success, -1 for failure
 */
int aeron_archive_purge_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    int64_t new_start_position);

/**
 * Attach segments to the beginning of a recording to restore history that was previously detached.
 * <p>
 * Segment files must match the existing recording and join exactly to the start position of the recording they are being attached to.
 *
 * @param count_p out param set to the number of segments attached
 * @param aeron_archive the archive client
 * @param recording_id the id of an existing recording
 * @return 0 for success, -1 for failure
 */
int aeron_archive_attach_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

/**
 * Migrate segments from a source recording and attach them to the beginning of a destination recording.
 * <p>
 * The source recording must match the destination recording for segment length, term length, mtu length,
 * stream id, plus the stop position and term id of the source must join with the start position of the destination
 * and be on a segment boundary.
 * <p>
 * The source recording will be effectively truncated back to its start position after the migration.
 *
 * @param count_p out param set to the number of segments deleted
 * @param aeron_archive the archive client
 * @param src_recording_id the id of an existing recording from which segments will be migrated
 * @param dst_recording_id the id of an existing recording to which segments will be migrated
 * @return 0 for success, -1 for failure
 */
int aeron_archive_migrate_segments(
    int64_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t src_recording_id,
    int64_t dst_recording_id);

/**
 * Update the channel for a recording, i.e. replace original and stripped channel information in the catalog.
 *
 * @param aeron_archive the archive client
 * @param recording_id the id of the recording.
 * @param new_channel to use in the catalogue.
 * @return 0 for success, -1 for failure
 */
int aeron_archive_update_channel(aeron_archive_t *aeron_archive, int64_t recording_id, const char *new_channel);

/**
 * Position of the recorded stream at the base of a segment file.
 * <p>
 * If a recording starts within a term then the base position can be before the recording started.
 *
 * @param start_position start position of the stream
 * @param position position in the stream to calculate the segment base position from.
 * @param term_buffer_length term buffer length of the stream
 * @param segment_file_length segment file length, which is a multiple of term buffer length
 * @return the position of the recorded stream at the beginning of a segment file
 */
int64_t aeron_archive_segment_file_base_position(
    int64_t start_position,
    int64_t position,
    int32_t term_buffer_length,
    int32_t segment_file_length);

/**
 * Find the active counter id for a stream based on the recording id.
 *
 * @param counters_reader an aeron_counters_reader_t to search within
 * @param recording_id the recording id of an active recording
 * @return the counter id if found, otherwise AERON_NULL_COUNTER_ID
 */
int32_t aeron_archive_recording_pos_find_counter_id_by_recording_id(aeron_counters_reader_t *counters_reader, int64_t recording_id);

/**
 * Find the active counter id for a stream based on the session id.
 *
 * @param counters_reader an aeron_counters_reader_t to search within
 * @param session_id the session id of an active recording
 * @return the counter id if found, otherwise AERON_NULL_COUNTER_ID
 */
int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id);

/**
 * Get the recording id for a given counter id.
 *
 * @param counters_reader an aeron_counters_reader_t to search within
 * @param counter_id the counter id of an active recording
 * @return the recording id if found, otherwise AERON_NULL_COUNTER_ID
 */
int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id);

/**
 * Get the source identity for the recording.
 * <p>
 * See source_identity in aeron_image_constants_t.
 *
 * @param counters_reader an aeron_counters_reader_t to search within
 * @param counter_id the counter id of an active recording
 * @param dst a destination buffer into which the source identity will be written
 * @param len_p a pointer to a size_t that initially indicates the length of the dst buffer.  After the function return successfully, len_p will be set to the length of the source identity string in dst
 * @return 0 for success, -1 for failure
 */
int aeron_archive_recording_pos_get_source_identity(aeron_counters_reader_t *counters_reader, int32_t counter_id, const char *dst, size_t *len_p);

/**
 * Is the recording counter still active?
 *
 * @param is_active out param set to true if the counter is still active
 * @param counters_reader an aeron_counters_reader_t to search within
 * @param counter_id the counter id to search for
 * @param recording_id the recording id to match against
 * @return 0 for success, -1 for failure
 */
int aeron_archive_recording_pos_is_active(bool *is_active, aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t recording_id);

/* replay/merge */

typedef struct aeron_archive_replay_merge_stct aeron_archive_replay_merge_t;

#define REPLAY_MERGE_PROGRESS_TIMEOUT_DEFAULT_MS (5 * 1000)

/**
 * Create an aeron_archive_replay_merge_t to manage the merging of a replayed stream into a live stream.
 *
 * @param replay_merge the aeron_archive_replay_merge_t to create and initialize
 * @param subscription the subscription to use for the replay and live stream.  Must be a multi-destination subscription
 * @param aeron_archive the archive client
 * @param replay_channel the channel to use for the replay
 * @param replay_destination the replay channel to use for the destination added by the subscription
 * @param live_destination the live stream channel to use for the destination added by the subscription
 * @param recording_id the recording id of the archive to replay
 * @param start_position the start position of the replay
 * @param epoch_clock the clock to use for progress checks
 * @param merge_progress_timeout_ms the timeout to use for progress checks
 * @return 0 for success, -1 for failure
 */
int aeron_archive_replay_merge_init(
    aeron_archive_replay_merge_t **replay_merge,
    aeron_subscription_t *subscription,
    aeron_archive_t *aeron_archive,
    const char *replay_channel,
    const char *replay_destination,
    const char *live_destination,
    int64_t recording_id,
    int64_t start_position,
    long long epoch_clock,
    int64_t merge_progress_timeout_ms);

/**
 * Close and delete the aeron_archive_replay_merge_t struct.
 *
 * @param replay_merge the aeron_archive_replay_merge_t to close and delete
 * @return 0 for success, -1 for failure
 */
int aeron_archive_replay_merge_close(aeron_archive_replay_merge_t *replay_merge);

/**
 * Process the operation of the merge.  Do not call the processing of fragments on the subscription.
 *
 * @param work_count_p an indicator of work done
 * @param replay_merge the replay_merge to process
 * @return 0 for success, -1 for failure
 */
int aeron_archive_replay_merge_do_work(int *work_count_p, aeron_archive_replay_merge_t *replay_merge);

/**
 * Poll the image used for the merging replay and live stream.
 * The aeron_archive_replay_merge_do_work will be called before the poll so that processing of the merge can be done.
 *
 * @param replay_merge the replay_merge to process/poll
 * @param handler the handler to call for incoming fragments
 * @param clientd the clientd to provide to the handler
 * @param fragment_limit the max number of fragments to process before returning
 * @return >= 0 indicates the number of fragments processed, -1 for failure
 */
int aeron_archive_replay_merge_poll(
    aeron_archive_replay_merge_t *replay_merge,
    aeron_fragment_handler_t handler,
    void *clientd,
    int fragment_limit);

/**
 * The image used for the replay and live stream.
 *
 * @param replay_merge the replay_merge that owns the image.
 * @return the aeron_image_t
 */
aeron_image_t *aeron_archive_replay_merge_image(aeron_archive_replay_merge_t *replay_merge);

/**
 * Is the live stream merged and the replay stopped?
 *
 * @param replay_merge the replay_merge to check
 * @return true if merged, false otherwise
 */
bool aeron_archive_replay_merge_is_merged(aeron_archive_replay_merge_t *replay_merge);

/**
 * Has the replay_merge failed due to an error?
 *
 * @param replay_merge the replay_merge to check
 * @return true if an error occurred
 */
bool aeron_archive_replay_merge_has_failed(aeron_archive_replay_merge_t *replay_merge);

/**
 * Is the live destination added to the subscription?
 *
 * @param replay_merge the replay_merge to check
 * @return true if the live destination is added to the subscription
 */
bool aeron_archive_replay_merge_is_live_added(aeron_archive_replay_merge_t *replay_merge);

/* persistent subscription */

#define AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START (INT64_C(-1))
#define AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE  (INT64_C(-2))


typedef struct aeron_archive_persistent_subscription_context_stct aeron_archive_persistent_subscription_context_t;
typedef struct aeron_archive_persistent_subscription_stct aeron_archive_persistent_subscription_t;

/**
 * Listener for events from a persistent subscription.
 * <p>
 * All callbacks are invoked from the thread that calls aeron_archive_persistent_subscription_poll
 * or aeron_archive_persistent_subscription_controlled_poll.
 */
typedef struct aeron_archive_persistent_subscription_listener_stct
{
    /**
     * Invoked when the persistent subscription transitions to the live state.
     *
     * @param clientd the clientd set on this listener.
     */
    void (*on_live_joined)(void *clientd);

    /**
     * Invoked when the persistent subscription transitions from the live state.
     *
     * @param clientd the clientd set on this listener.
     */
    void (*on_live_left)(void *clientd);

    /**
     * Invoked when the persistent subscription encounters an error.
     * <p>
     * The message pointer refers to a stack-allocated buffer owned by the caller and is only valid
     * for the duration of this callback.
     *
     * @param clientd the clientd set on this listener.
     * @param errcode the error code describing the failure.
     * @param message a human-readable error message. Valid only for the duration of this callback;
     *                copy if it must be retained.
     */
    void (*on_error)(void *clientd, int errcode, const char *message);

    /**
     * Opaque user data passed to each of the callbacks above. The persistent subscription does not
     * dereference, copy, or take ownership of this pointer. The caller is responsible for ensuring
     * it remains valid for the lifetime of the persistent subscription.
     */
    void *clientd;
}
aeron_archive_persistent_subscription_listener_t;

/**
 * Create and initialize a persistent subscription context.
 *
 * @param context to set if completed successfully.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_init(
    aeron_archive_persistent_subscription_context_t **context);

/**
 * Close and dispose of all resources held by the persistent subscription context.
 * <p>
 * If the context created its own Aeron client (i.e. none was set via
 * aeron_archive_persistent_subscription_context_set_aeron), that client will be closed here.
 *
 * @param context to close.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_close(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the Aeron client that will be used by the persistent subscription.
 * <p>
 * If not set, the persistent subscription will create and own its own Aeron client when
 * aeron_archive_persistent_subscription_create is called. In that case, the client will be
 * closed when the context is closed via aeron_archive_persistent_subscription_context_close.
 *
 * @param context to configure.
 * @param aeron the Aeron client to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_aeron(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_t *aeron);

/**
 * Set the Aeron directory name to use when the persistent subscription creates its own Aeron client.
 * Has no effect if an Aeron client is set via aeron_archive_persistent_subscription_context_set_aeron.
 * <p>
 * The directory name is copied into the context. The caller retains ownership of the supplied string.
 *
 * @param context to configure.
 * @param aeron_directory_name the Aeron directory name.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_aeron_directory_name(
    aeron_archive_persistent_subscription_context_t *context,
    const char *aeron_directory_name);

/**
 * Set the Aeron Archive client context that will be used by the persistent subscription.
 *
 * @param context to configure.
 * @param archive_context the Aeron Archive client context to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_archive_context(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_archive_context_t *archive_context);

/**
 * Set the id of the live stream recording that will be used by the persistent subscription to catch up.
 *
 * @param context to configure.
 * @param recording_id the recording id to use.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_recording_id(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t recording_id);

/**
 * Get the id of the live stream recording that will be used by the persistent subscription to catch up.
 *
 * @param context to query.
 * @return the recording id.
 * @see aeron_archive_persistent_subscription_context_set_recording_id
 */
int64_t aeron_archive_persistent_subscription_context_get_recording_id(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the live channel.
 *
 * @param context to configure.
 * @param live_channel the live channel which will be copied.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *live_channel);

/**
 * Get the live channel.
 *
 * @param context to query.
 * @return the live channel.
 * @see aeron_archive_persistent_subscription_context_set_live_channel
 */
const char *aeron_archive_persistent_subscription_context_get_live_channel(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the id of the live stream.
 *
 * @param context to configure.
 * @param live_stream_id the live stream id.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t live_stream_id);

/**
 * Get the id of the live stream.
 *
 * @param context to query.
 * @return the live stream id.
 * @see aeron_archive_persistent_subscription_context_set_live_stream_id
 */
int32_t aeron_archive_persistent_subscription_context_get_live_stream_id(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the channel used for replays.
 *
 * @param context to configure.
 * @param replay_channel the channel to use for replays which will be copied.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_replay_channel(
    aeron_archive_persistent_subscription_context_t *context,
    const char *replay_channel);

/**
 * Get the channel used for replays.
 *
 * @param context to query.
 * @return the replay channel.
 * @see aeron_archive_persistent_subscription_context_set_replay_channel
 */
const char *aeron_archive_persistent_subscription_context_get_replay_channel(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the id of the stream used for replays.
 *
 * @param context to configure.
 * @param replay_stream_id the stream id to use for replays.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context,
    int32_t replay_stream_id);

/**
 * Get the id of the stream used for replays.
 *
 * @param context to query.
 * @return the replay stream id.
 * @see aeron_archive_persistent_subscription_context_set_replay_stream_id
 */
int32_t aeron_archive_persistent_subscription_context_get_replay_stream_id(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the position to start the subscription from, can be an actual position or
 * AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_START or AERON_ARCHIVE_PERSISTENT_SUBSCRIPTION_FROM_LIVE.
 *
 * @param context to configure.
 * @param start_position the position to start the subscription from.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_start_position(
    aeron_archive_persistent_subscription_context_t *context,
    int64_t start_position);

/**
 * Get the position to start the subscription from.
 *
 * @param context to query.
 * @return the start position.
 * @see aeron_archive_persistent_subscription_context_set_start_position
 */
int64_t aeron_archive_persistent_subscription_context_get_start_position(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the listener for events from the persistent subscription.
 *
 * @param context to configure.
 * @param listener the listener to set. The content of the listener struct is copied by value.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_listener(
    aeron_archive_persistent_subscription_context_t *context,
    const aeron_archive_persistent_subscription_listener_t *listener);

/**
 * Set the counter for tracking the current state of the persistent subscription.
 *
 * @param context to configure.
 * @param counter the state counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_state_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the current state of the persistent subscription.
 *
 * @param context to query.
 * @return the state counter.
 * @see aeron_archive_persistent_subscription_context_set_state_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_state_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the join difference of the persistent subscription.
 * The join difference is the difference between the live position and the replay position when
 * transitioning. When not live, the value is INT64_MIN.
 *
 * @param context to configure.
 * @param counter the join difference counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the join difference of the persistent subscription.
 *
 * @param context to query.
 * @return the join difference counter.
 * @see aeron_archive_persistent_subscription_context_set_join_difference_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_join_difference_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the number of times the live stream has been left.
 *
 * @param context to configure.
 * @param counter the live left counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the number of times the live stream has been left.
 *
 * @param context to query.
 * @return the live left counter.
 * @see aeron_archive_persistent_subscription_context_set_live_left_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_left_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Set the counter for tracking the number of times live has been joined.
 *
 * @param context to configure.
 * @param counter the live joined counter.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_context_set_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context,
    aeron_counter_t *counter);

/**
 * Get the counter for tracking the number of times live has been joined.
 *
 * @param context to query.
 * @return the live joined counter.
 * @see aeron_archive_persistent_subscription_context_set_live_joined_counter
 */
aeron_counter_t *aeron_archive_persistent_subscription_context_get_live_joined_counter(
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Create a persistent subscription.
 * <p>
 * If creating a subscription succeeds, then the subscription will own the context. And closing the
 * subscription, will close the context.
 * <p>
 * If no Aeron client is set on the context, one will be created and owned by the context,
 * and will be closed when the context is closed via aeron_archive_persistent_subscription_context_close.
 *
 * @param persistent_subscription to set if completed successfully.
 * @param context with the configuration of a persistent subscription to be created.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_create(
    aeron_archive_persistent_subscription_t **persistent_subscription,
    aeron_archive_persistent_subscription_context_t *context);

/**
 * Close a persistent subscription and dispose of all resources and memory held by it.
 *
 * @param persistent_subscription to close.
 * @return 0 on success, -1 on error.
 */
int aeron_archive_persistent_subscription_close(
    aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Poll a persistent subscription for available messages.
 * <p>
 * Delivers assembled messages to the handler, so the handler shouldn't be a fragment assembler.
 *
 * @param persistent_subscription to poll.
 * @param handler for handling read messages.
 * @param clientd to pass to the handler.
 * @param fragment_limit maximum number of message fragments to read when polling.
 * @return positive number if fragments have been read or the persistent subscription has done other work,
 * 0 if no fragments have been read and no work has been done, negative on error.
 */
int aeron_archive_persistent_subscription_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);

/**
 * Poll in a controlled manner a persistent subscription for available messages.
 * <p>
 * Delivers assembled messages to the handler, so the handler shouldn't be a fragment assembler.
 *
 * @param persistent_subscription to poll.
 * @param handler for handling read messages.
 * @param clientd to pass to the handler.
 * @param fragment_limit maximum number of message fragments to read when polling.
 * @return positive number if fragments have been read or the persistent subscription has done other work,
 * 0 if no fragments have been read and no work has been done, negative on error.
 */
int aeron_archive_persistent_subscription_controlled_poll(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    aeron_controlled_fragment_handler_t handler,
    void *clientd,
    size_t fragment_limit);

/**
 * Check if persistent subscription is live, i.e. reading messages from the live subscription without having a replay
 * subscription.
 *
 * @param persistent_subscription to check.
 * @return true if live, false otherwise.
 */
bool aeron_archive_persistent_subscription_is_live(
    aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Indicates if the persistent subscription is replaying from a recording.
 *
 * @param persistent_subscription to check.
 * @return true if replaying, false otherwise.
 */
bool aeron_archive_persistent_subscription_is_replaying(
    aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Indicates if the persistent subscription has failed.
 * <p>
 * The listener will be notified of any terminal errors that can cause the persistent subscription to fail.
 *
 * @param persistent_subscription to check.
 * @return true if failed, false otherwise.
 * @see aeron_archive_persistent_subscription_context_set_listener
 */
bool aeron_archive_persistent_subscription_has_failed(
    aeron_archive_persistent_subscription_t *persistent_subscription);

/**
 * Get the terminal error that caused the persistent subscription to fail.
 * Only meaningful when aeron_archive_persistent_subscription_has_failed returns true.
 *
 * @param persistent_subscription to query.
 * @param out_errcode optional pointer to receive the error code, may be NULL.
 * @param out_message optional pointer to receive the error message, may be NULL.
 * @return true if in the failed state, false otherwise.
 * @see aeron_archive_persistent_subscription_has_failed
 */
bool aeron_archive_persistent_subscription_failure_reason(
    aeron_archive_persistent_subscription_t *persistent_subscription,
    int *out_errcode,
    const char **out_message);

#ifdef __cplusplus
}
#endif

#endif //AERON_ARCHIVE_H
