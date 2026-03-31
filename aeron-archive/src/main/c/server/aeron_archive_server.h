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

#ifndef AERON_ARCHIVE_SERVER_H
#define AERON_ARCHIVE_SERVER_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeronc.h"
#include "aeron_archive_conductor.h"
#include "aeron_archive_mark_file.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * Archive threading mode (matches Java ArchiveThreadingMode)
 * ----------------------------------------------------------------------- */
typedef enum aeron_archive_threading_mode_en
{
    /** Conductor, recorder, and replayer on dedicated threads. */
    AERON_ARCHIVE_THREADING_MODE_DEDICATED = 0,
    /** All on a single shared thread. */
    AERON_ARCHIVE_THREADING_MODE_SHARED = 1,
    /** Caller invokes do_work manually (e.g. inside ArchivingMediaDriver). */
    AERON_ARCHIVE_THREADING_MODE_INVOKER = 2
}
aeron_archive_threading_mode_t;

/* -----------------------------------------------------------------------
 * Configuration defaults (matching Java Archive.Configuration)
 * ----------------------------------------------------------------------- */

/** Default archive directory name. */
#define AERON_ARCHIVE_SERVER_DIR_DEFAULT                      "aeron-archive"

/** Default segment file length: 128 MiB. */
#define AERON_ARCHIVE_SERVER_SEGMENT_FILE_LENGTH_DEFAULT      (128 * 1024 * 1024)

/** Default file IO max length: 1 MiB (must be power of 2). */
#define AERON_ARCHIVE_SERVER_FILE_IO_MAX_LENGTH_DEFAULT       (1024 * 1024)

/** Default file sync level: 0 = normal writes. */
#define AERON_ARCHIVE_SERVER_FILE_SYNC_LEVEL_DEFAULT          0

/** Default catalog file sync level: same as file sync level. */
#define AERON_ARCHIVE_SERVER_CATALOG_FILE_SYNC_LEVEL_DEFAULT  0

/** Default max concurrent recordings. */
#define AERON_ARCHIVE_SERVER_MAX_CONCURRENT_RECORDINGS_DEFAULT 20

/** Default max concurrent replays. */
#define AERON_ARCHIVE_SERVER_MAX_CONCURRENT_REPLAYS_DEFAULT   20

/** Default catalog capacity in bytes. */
#define AERON_ARCHIVE_SERVER_CATALOG_CAPACITY_DEFAULT         (8LL * 1024 * 1024)

/** Default low storage space threshold (= segment file length). */
#define AERON_ARCHIVE_SERVER_LOW_STORAGE_THRESHOLD_DEFAULT    \
    AERON_ARCHIVE_SERVER_SEGMENT_FILE_LENGTH_DEFAULT

/** Default connect timeout: 5 seconds in nanoseconds. */
#define AERON_ARCHIVE_SERVER_CONNECT_TIMEOUT_DEFAULT_NS       (5LL * 1000 * 1000 * 1000)

/** Default session liveness check interval: 1 second in nanoseconds. */
#define AERON_ARCHIVE_SERVER_SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS \
    (1LL * 1000 * 1000 * 1000)

/** Default replay linger timeout: 5 seconds in nanoseconds. */
#define AERON_ARCHIVE_SERVER_REPLAY_LINGER_TIMEOUT_DEFAULT_NS (5LL * 1000 * 1000 * 1000)

/** Default error buffer length: 1 MiB. */
#define AERON_ARCHIVE_SERVER_ERROR_BUFFER_LENGTH_DEFAULT      (1024 * 1024)

/** Default control stream id (matches AeronArchive.Configuration). */
#define AERON_ARCHIVE_SERVER_CONTROL_STREAM_ID_DEFAULT        10

/** Default local control stream id. */
#define AERON_ARCHIVE_SERVER_LOCAL_CONTROL_STREAM_ID_DEFAULT  10

/** Default recording events stream id. */
#define AERON_ARCHIVE_SERVER_RECORDING_EVENTS_STREAM_ID_DEFAULT 30

/** Default control MTU length. */
#define AERON_ARCHIVE_SERVER_CONTROL_MTU_LENGTH_DEFAULT       1408

/** Default control term buffer length: 64 KiB. */
#define AERON_ARCHIVE_SERVER_CONTROL_TERM_BUFFER_LENGTH_DEFAULT (64 * 1024)

/** Minimum term buffer length. */
#define AERON_ARCHIVE_SERVER_TERM_MIN_LENGTH                  (64 * 1024)

/** Maximum term buffer length. */
#define AERON_ARCHIVE_SERVER_TERM_MAX_LENGTH                  (1024 * 1024 * 1024)

/* -----------------------------------------------------------------------
 * Archive Server Context
 *
 * Corresponds to Archive.Context in Java. Holds all configuration for
 * creating and running an archive server instance.
 * ----------------------------------------------------------------------- */
typedef struct aeron_archive_server_context_stct
{
    /* Aeron client */
    aeron_t *aeron;
    char     aeron_directory_name[4096];
    bool     owns_aeron_client;

    /* Directory configuration */
    char     archive_dir[4096];
    char     mark_file_dir[4096];
    bool     delete_archive_on_start;

    /* Channels and stream IDs */
    char    *control_channel;
    int32_t  control_stream_id;
    char    *local_control_channel;
    int32_t  local_control_stream_id;
    char    *recording_events_channel;
    int32_t  recording_events_stream_id;
    char    *replication_channel;

    /* Control stream parameters */
    bool     control_channel_enabled;
    bool     recording_events_enabled;
    bool     control_term_buffer_sparse;
    int32_t  control_mtu_length;
    int32_t  control_term_buffer_length;

    /* Recording parameters */
    int32_t  segment_file_length;
    int32_t  file_io_max_length;
    int32_t  file_sync_level;
    int32_t  catalog_file_sync_level;

    /* Session limits */
    int32_t  max_concurrent_recordings;
    int32_t  max_concurrent_replays;

    /* Capacity / thresholds */
    int64_t  catalog_capacity;
    int64_t  low_storage_space_threshold;

    /* Timeouts (nanoseconds) */
    int64_t  connect_timeout_ns;
    int64_t  session_liveness_check_interval_ns;
    int64_t  replay_linger_timeout_ns;

    /* Archive identity */
    int64_t  archive_id;

    /* Error buffer */
    int32_t  error_buffer_length;

    /* Error handling */
    aeron_error_handler_t error_handler;
    void                 *error_handler_clientd;

    /* Threading */
    aeron_archive_threading_mode_t threading_mode;

    /* Mark file (created during conclude) */
    aeron_archive_mark_file_t *mark_file;
    bool                       owns_mark_file;

    /* Conductor context (populated during conclude) */
    aeron_archive_conductor_context_t conductor_ctx;

    /* Concluded flag */
    bool is_concluded;
}
aeron_archive_server_context_t;

/* -----------------------------------------------------------------------
 * Archive Server
 *
 * Corresponds to Archive in Java. Wraps a conductor and manages its
 * lifecycle (start, do_work, close).
 * ----------------------------------------------------------------------- */
typedef struct aeron_archive_server_stct
{
    aeron_archive_server_context_t *ctx;
    aeron_archive_conductor_t      *conductor;

    /* For SHARED/INVOKER modes, the caller drives the do_work loop. */
    bool is_running;
    bool is_closed;
}
aeron_archive_server_t;

/* -----------------------------------------------------------------------
 * Context lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Initialise the archive server context with default values.
 *
 * @param ctx  out parameter for the allocated context.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_context_init(aeron_archive_server_context_t **ctx);

/**
 * Conclude (validate and finalise) the archive server context.
 *
 * Creates the archive directory, mark file, and conductor context.
 * This is equivalent to Java Archive.Context.conclude().
 *
 * @param ctx  the context to conclude.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_context_conclude(aeron_archive_server_context_t *ctx);

/**
 * Close the archive server context and release owned resources.
 *
 * @param ctx  the context to close (may be NULL).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_context_close(aeron_archive_server_context_t *ctx);

/* -----------------------------------------------------------------------
 * Archive Server lifecycle
 * ----------------------------------------------------------------------- */

/**
 * Create an archive server with the given context.
 * The context must have been concluded before calling this function.
 *
 * @param archive  out parameter for the allocated archive server.
 * @param ctx      the concluded archive server context (ownership transferred).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_create(
    aeron_archive_server_t **archive,
    aeron_archive_server_context_t *ctx);

/**
 * Convenience: allocate default context, conclude, create, and start the archive.
 * Equivalent to Java Archive.launch(ctx).
 *
 * @param archive  out parameter for the launched archive server.
 * @param ctx      the context (will be concluded if not already). May be NULL for defaults.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_launch(
    aeron_archive_server_t **archive,
    aeron_archive_server_context_t *ctx);

/**
 * Perform a single unit of work on the archive conductor.
 * Used in INVOKER or SHARED threading modes.
 *
 * @param archive  the archive server.
 * @return the amount of work done, or -1 on terminal error.
 */
int aeron_archive_server_do_work(aeron_archive_server_t *archive);

/**
 * Close the archive server and release all resources.
 *
 * @param archive  the archive server to close (may be NULL).
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_server_close(aeron_archive_server_t *archive);

/**
 * Get the archive server context.
 *
 * @param archive  the archive server.
 * @return the context.
 */
aeron_archive_server_context_t *aeron_archive_server_context(aeron_archive_server_t *archive);

/**
 * Check if the archive server is closed.
 *
 * @param archive  the archive server.
 * @return true if closed.
 */
bool aeron_archive_server_is_closed(const aeron_archive_server_t *archive);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_SERVER_H */
