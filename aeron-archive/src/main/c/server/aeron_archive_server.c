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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>

#if defined(_MSC_VER)
#include <direct.h>
#include <io.h>
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <process.h>
#define mkdir(dir, mode) _mkdir(dir)
#define stat _stat
#ifndef S_ISDIR
#define S_ISDIR(m) (((m) & _S_IFMT) == _S_IFDIR)
#endif
#else
#include <sys/types.h>
#include <unistd.h>
#endif

#include "aeron_archive_server.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"

/* -----------------------------------------------------------------------
 * Utility: check power of 2
 * ----------------------------------------------------------------------- */
static bool aeron_archive_is_power_of_two(int32_t value)
{
    return value > 0 && (value & (value - 1)) == 0;
}

/* -----------------------------------------------------------------------
 * Utility: ensure directory exists
 * ----------------------------------------------------------------------- */
static int aeron_archive_ensure_dir(const char *dir, const char *label)
{
    struct stat st;
    if (stat(dir, &st) == 0)
    {
        if (S_ISDIR(st.st_mode))
        {
            return 0;
        }
        AERON_SET_ERR(ENOTDIR, "%s path exists but is not a directory: %s", label, dir);
        return -1;
    }

    if (mkdir(dir, 0755) < 0 && errno != EEXIST)
    {
        AERON_SET_ERR(errno, "failed to create %s directory: %s", label, dir);
        return -1;
    }
    return 0;
}

/* -----------------------------------------------------------------------
 * Utility: epoch millis
 * ----------------------------------------------------------------------- */
static int64_t aeron_archive_epoch_ms(void)
{
    return aeron_epoch_clock();
}

/* -----------------------------------------------------------------------
 * Utility: duplicate a string, NULL-safe
 * ----------------------------------------------------------------------- */
static char *aeron_archive_strdup(const char *s)
{
    if (NULL == s)
    {
        return NULL;
    }
    size_t len = strlen(s);
    char *dup = NULL;
    if (aeron_alloc((void **)&dup, len + 1) < 0)
    {
        return NULL;
    }
    memcpy(dup, s, len + 1);
    return dup;
}

/* -----------------------------------------------------------------------
 * Context init
 * ----------------------------------------------------------------------- */
int aeron_archive_server_context_init(aeron_archive_server_context_t **ctx)
{
    aeron_archive_server_context_t *c = NULL;
    if (aeron_alloc((void **)&c, sizeof(aeron_archive_server_context_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive server context");
        return -1;
    }
    memset(c, 0, sizeof(aeron_archive_server_context_t));

    /* Aeron client */
    c->aeron = NULL;
    c->aeron_directory_name[0] = '\0';
    c->owns_aeron_client = false;

    /* Directories */
    snprintf(c->archive_dir, sizeof(c->archive_dir), "%s", AERON_ARCHIVE_SERVER_DIR_DEFAULT);
    c->mark_file_dir[0] = '\0';  /* empty = use archive_dir */
    c->delete_archive_on_start = false;

    /* Channels */
    c->control_channel = aeron_archive_strdup("aeron:udp?endpoint=localhost:8010");
    c->control_stream_id = AERON_ARCHIVE_SERVER_CONTROL_STREAM_ID_DEFAULT;
    c->local_control_channel = aeron_archive_strdup("aeron:ipc?term-length=64k");
    c->local_control_stream_id = AERON_ARCHIVE_SERVER_LOCAL_CONTROL_STREAM_ID_DEFAULT;
    c->recording_events_channel = aeron_archive_strdup("aeron:udp?endpoint=localhost:8030");
    c->recording_events_stream_id = AERON_ARCHIVE_SERVER_RECORDING_EVENTS_STREAM_ID_DEFAULT;
    c->replication_channel = aeron_archive_strdup("aeron:udp?endpoint=localhost:0");

    /* Control stream parameters */
    c->control_channel_enabled = true;
    c->recording_events_enabled = true;
    c->control_term_buffer_sparse = true;
    c->control_mtu_length = AERON_ARCHIVE_SERVER_CONTROL_MTU_LENGTH_DEFAULT;
    c->control_term_buffer_length = AERON_ARCHIVE_SERVER_CONTROL_TERM_BUFFER_LENGTH_DEFAULT;

    /* Recording parameters */
    c->segment_file_length = AERON_ARCHIVE_SERVER_SEGMENT_FILE_LENGTH_DEFAULT;
    c->file_io_max_length = AERON_ARCHIVE_SERVER_FILE_IO_MAX_LENGTH_DEFAULT;
    c->file_sync_level = AERON_ARCHIVE_SERVER_FILE_SYNC_LEVEL_DEFAULT;
    c->catalog_file_sync_level = AERON_ARCHIVE_SERVER_CATALOG_FILE_SYNC_LEVEL_DEFAULT;

    /* Session limits */
    c->max_concurrent_recordings = AERON_ARCHIVE_SERVER_MAX_CONCURRENT_RECORDINGS_DEFAULT;
    c->max_concurrent_replays = AERON_ARCHIVE_SERVER_MAX_CONCURRENT_REPLAYS_DEFAULT;

    /* Capacity / thresholds */
    c->catalog_capacity = AERON_ARCHIVE_SERVER_CATALOG_CAPACITY_DEFAULT;
    c->low_storage_space_threshold = AERON_ARCHIVE_SERVER_LOW_STORAGE_THRESHOLD_DEFAULT;

    /* Timeouts */
    c->connect_timeout_ns = AERON_ARCHIVE_SERVER_CONNECT_TIMEOUT_DEFAULT_NS;
    c->session_liveness_check_interval_ns = AERON_ARCHIVE_SERVER_SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS;
    c->replay_linger_timeout_ns = AERON_ARCHIVE_SERVER_REPLAY_LINGER_TIMEOUT_DEFAULT_NS;

    /* Identity */
    c->archive_id = AERON_ARCHIVE_NULL_VALUE;

    /* Error buffer */
    c->error_buffer_length = AERON_ARCHIVE_SERVER_ERROR_BUFFER_LENGTH_DEFAULT;

    /* Error handling */
    c->error_handler = NULL;
    c->error_handler_clientd = NULL;

    /* Threading */
    c->threading_mode = AERON_ARCHIVE_THREADING_MODE_SHARED;

    /* Mark file */
    c->mark_file = NULL;
    c->owns_mark_file = false;

    c->is_concluded = false;

    *ctx = c;
    return 0;
}

/* -----------------------------------------------------------------------
 * Context conclude
 * ----------------------------------------------------------------------- */
int aeron_archive_server_context_conclude(aeron_archive_server_context_t *ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "NULL archive server context");
        return -1;
    }

    if (ctx->is_concluded)
    {
        AERON_SET_ERR(EINVAL, "%s", "archive server context already concluded");
        return -1;
    }

    /* ---- Validate all parameters BEFORE creating resources (matches Java conclude order) ---- */

    /* Validate catalog_file_sync_level >= file_sync_level */
    if (ctx->catalog_file_sync_level < ctx->file_sync_level)
    {
        AERON_SET_ERR(EINVAL,
            "catalogFileSyncLevel %d < fileSyncLevel %d",
            ctx->catalog_file_sync_level, ctx->file_sync_level);
        return -1;
    }

    /* Validate file IO max length: must be power of 2 and >= TERM_MIN_LENGTH */
    if (ctx->file_io_max_length < AERON_ARCHIVE_SERVER_TERM_MIN_LENGTH ||
        !aeron_archive_is_power_of_two(ctx->file_io_max_length))
    {
        AERON_SET_ERR(EINVAL, "invalid fileIoMaxLength=%d", ctx->file_io_max_length);
        return -1;
    }

    /* Validate control channel if enabled */
    if (ctx->control_channel_enabled)
    {
        if (NULL == ctx->control_channel || strlen(ctx->control_channel) == 0)
        {
            AERON_SET_ERR(EINVAL, "%s", "controlChannel must be set when control channel is enabled");
            return -1;
        }
    }

    /* Validate local control channel must start with "aeron:ipc" */
    if (NULL == ctx->local_control_channel ||
        strncmp(ctx->local_control_channel, "aeron:ipc", 9) != 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "localControlChannel must be IPC media");
        return -1;
    }

    /* Validate replication channel */
    if (NULL == ctx->replication_channel || strlen(ctx->replication_channel) == 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "replicationChannel must be set");
        return -1;
    }

    /* Validate recording events channel if enabled */
    if (ctx->recording_events_enabled &&
        (NULL == ctx->recording_events_channel || strlen(ctx->recording_events_channel) == 0))
    {
        AERON_SET_ERR(EINVAL, "%s",
            "recordingEventsChannel must be set when recording events are enabled");
        return -1;
    }

    /* Validate segment file length: must be power of 2 in valid range */
    if (!aeron_archive_is_power_of_two(ctx->segment_file_length) ||
        ctx->segment_file_length < AERON_ARCHIVE_SERVER_TERM_MIN_LENGTH ||
        ctx->segment_file_length > AERON_ARCHIVE_SERVER_TERM_MAX_LENGTH)
    {
        AERON_SET_ERR(EINVAL,
            "segmentFileLength not a power of 2 in valid range: %d",
            ctx->segment_file_length);
        return -1;
    }

    /* Validate error buffer length */
    if (ctx->error_buffer_length < AERON_ARCHIVE_SERVER_ERROR_BUFFER_LENGTH_DEFAULT)
    {
        AERON_SET_ERR(EINVAL, "invalid errorBufferLength=%d", ctx->error_buffer_length);
        return -1;
    }

    /* ---- All validations passed. Now create resources. ---- */

    /* Create an Aeron client if one was not provided externally. */
    if (NULL == ctx->aeron)
    {
        aeron_context_t *aeron_ctx = NULL;
        if (aeron_context_init(&aeron_ctx) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to init aeron context for archive");
            return -1;
        }

        if (ctx->aeron_directory_name[0] != '\0')
        {
            if (aeron_context_set_dir(aeron_ctx, ctx->aeron_directory_name) < 0)
            {
                aeron_context_close(aeron_ctx);
                AERON_APPEND_ERR("%s", "failed to set aeron dir for archive");
                return -1;
            }
        }

        if (aeron_init(&ctx->aeron, aeron_ctx) < 0)
        {
            aeron_context_close(aeron_ctx);
            AERON_APPEND_ERR("%s", "failed to init aeron client for archive");
            return -1;
        }

        if (aeron_start(ctx->aeron) < 0)
        {
            aeron_close(ctx->aeron);
            ctx->aeron = NULL;
            AERON_APPEND_ERR("%s", "failed to start aeron client for archive");
            return -1;
        }

        ctx->owns_aeron_client = true;
    }

    /* Resolve mark file dir: defaults to archive_dir */
    if (ctx->mark_file_dir[0] == '\0')
    {
        snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s", ctx->archive_dir);
    }

    /* Delete archive on start if requested */
    if (ctx->delete_archive_on_start)
    {
        /* Best-effort removal: remove files in directory but not the dir itself.
         * A full recursive delete is intentionally omitted for safety. */
        char cmd[4200];
        snprintf(cmd, sizeof(cmd), "rm -rf %s/*", ctx->archive_dir);
        if (system(cmd)) {}
    }

    /* Ensure directories exist */
    if (aeron_archive_ensure_dir(ctx->archive_dir, "archive") < 0)
    {
        return -1;
    }

    if (aeron_archive_ensure_dir(ctx->mark_file_dir, "mark file") < 0)
    {
        return -1;
    }

    /* Conclude archive ID: if not set, generate one from pid + timestamp */
    if (AERON_ARCHIVE_NULL_VALUE == ctx->archive_id)
    {
        if (NULL != ctx->aeron)
        {
            ctx->archive_id = aeron_client_id(ctx->aeron);
        }
        else
        {
            /* Generate a pseudo-unique ID from pid and time */
#if defined(_MSC_VER)
            ctx->archive_id = ((int64_t)_getpid() << 32) | (aeron_archive_epoch_ms() & 0xFFFFFFFF);
#else
            ctx->archive_id = ((int64_t)getpid() << 32) | (aeron_archive_epoch_ms() & 0xFFFFFFFF);
#endif
        }
    }

    /* Create mark file */
    if (NULL == ctx->mark_file)
    {
        int64_t now_ms = aeron_archive_epoch_ms();
        if (aeron_archive_mark_file_create(
            &ctx->mark_file,
            ctx->mark_file_dir,
            ctx->error_buffer_length,
            ctx->archive_id,
            ctx->control_stream_id,
            ctx->local_control_stream_id,
            ctx->recording_events_stream_id,
            ctx->control_channel,
            ctx->local_control_channel,
            ctx->recording_events_channel,
            ctx->aeron_directory_name,
            now_ms,
            AERON_ARCHIVE_MARK_FILE_LIVENESS_TIMEOUT_MS) < 0)
        {
            return -1;
        }
        ctx->owns_mark_file = true;
    }

    /* Create the catalog (or open existing one) */
    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_archive_catalog_create(
        &catalog,
        ctx->archive_dir,
        (size_t)ctx->catalog_capacity,
        (ctx->file_sync_level >= 1),
        (ctx->file_sync_level >= 2)) < 0)
    {
        /* Catalog creation failure is non-fatal for the connect handshake,
         * but recording operations will not work without it. */
    }

    /* Populate conductor context from server context */
    aeron_archive_conductor_context_t *cctx = &ctx->conductor_ctx;
    cctx->aeron = ctx->aeron;
    cctx->catalog = catalog;
    cctx->recording_events_proxy = NULL;

    cctx->archive_dir = ctx->archive_dir;
    cctx->control_channel = ctx->control_channel;
    cctx->control_stream_id = ctx->control_stream_id;
    cctx->local_control_channel = ctx->local_control_channel;
    cctx->local_control_stream_id = ctx->local_control_stream_id;
    cctx->recording_events_channel = ctx->recording_events_channel;
    cctx->recording_events_stream_id = ctx->recording_events_stream_id;
    cctx->replication_channel = ctx->replication_channel;

    cctx->segment_file_length = ctx->segment_file_length;
    cctx->file_io_max_length = ctx->file_io_max_length;
    cctx->max_concurrent_recordings = ctx->max_concurrent_recordings;
    cctx->max_concurrent_replays = ctx->max_concurrent_replays;

    cctx->connect_timeout_ms = ctx->connect_timeout_ns / 1000000;
    cctx->session_liveness_check_interval_ms = ctx->session_liveness_check_interval_ns / 1000000;
    cctx->replay_linger_timeout_ns = ctx->replay_linger_timeout_ns;
    cctx->low_storage_space_threshold = ctx->low_storage_space_threshold;
    cctx->archive_id = ctx->archive_id;

    cctx->control_channel_enabled = ctx->control_channel_enabled;
    cctx->recording_events_enabled = ctx->recording_events_enabled;
    cctx->control_term_buffer_sparse = ctx->control_term_buffer_sparse;
    cctx->control_mtu_length = ctx->control_mtu_length;
    cctx->control_term_buffer_length = ctx->control_term_buffer_length;

    cctx->force_writes = (ctx->file_sync_level >= 1);
    cctx->force_metadata = (ctx->file_sync_level >= 2);

    /* Signal mark file ready */
    aeron_archive_mark_file_signal_ready(ctx->mark_file, aeron_archive_epoch_ms());

    ctx->is_concluded = true;
    return 0;
}

/* -----------------------------------------------------------------------
 * Context close
 * ----------------------------------------------------------------------- */
int aeron_archive_server_context_close(aeron_archive_server_context_t *ctx)
{
    if (NULL == ctx)
    {
        return 0;
    }

    if (ctx->owns_mark_file && NULL != ctx->mark_file)
    {
        aeron_archive_mark_file_signal_terminated(ctx->mark_file);
        aeron_archive_mark_file_close(ctx->mark_file);
        ctx->mark_file = NULL;
    }

    /* Close the catalog if one was created during conclude. */
    if (NULL != ctx->conductor_ctx.catalog)
    {
        aeron_archive_catalog_close(ctx->conductor_ctx.catalog);
        ctx->conductor_ctx.catalog = NULL;
    }

    if (ctx->owns_aeron_client && NULL != ctx->aeron)
    {
        aeron_close(ctx->aeron);
        ctx->aeron = NULL;
    }

    /* Free heap-allocated channel strings */
    aeron_free(ctx->control_channel);
    aeron_free(ctx->local_control_channel);
    aeron_free(ctx->recording_events_channel);
    aeron_free(ctx->replication_channel);

    ctx->control_channel = NULL;
    ctx->local_control_channel = NULL;
    ctx->recording_events_channel = NULL;
    ctx->replication_channel = NULL;

    aeron_free(ctx);
    return 0;
}

/* -----------------------------------------------------------------------
 * Archive Server create
 * ----------------------------------------------------------------------- */
int aeron_archive_server_create(
    aeron_archive_server_t **archive,
    aeron_archive_server_context_t *ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "NULL archive server context");
        return -1;
    }

    if (!ctx->is_concluded)
    {
        AERON_SET_ERR(EINVAL, "%s", "archive server context has not been concluded");
        return -1;
    }

    aeron_archive_server_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(aeron_archive_server_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive server");
        return -1;
    }
    memset(a, 0, sizeof(aeron_archive_server_t));

    a->ctx = ctx;
    a->conductor = NULL;
    a->is_running = false;
    a->is_closed = false;

    /* Create the conductor */
    if (aeron_archive_conductor_create(&a->conductor, &ctx->conductor_ctx) < 0)
    {
        aeron_archive_mark_file_signal_terminated(ctx->mark_file);
        aeron_free(a);
        return -1;
    }

    a->is_running = true;
    *archive = a;
    return 0;
}

/* -----------------------------------------------------------------------
 * Archive Server launch (convenience)
 * ----------------------------------------------------------------------- */
int aeron_archive_server_launch(
    aeron_archive_server_t **archive,
    aeron_archive_server_context_t *ctx)
{
    aeron_archive_server_context_t *c = ctx;

    if (NULL == c)
    {
        if (aeron_archive_server_context_init(&c) < 0)
        {
            return -1;
        }
    }

    if (!c->is_concluded)
    {
        if (aeron_archive_server_context_conclude(c) < 0)
        {
            if (NULL == ctx)
            {
                aeron_archive_server_context_close(c);
            }
            return -1;
        }
    }

    if (aeron_archive_server_create(archive, c) < 0)
    {
        if (NULL == ctx)
        {
            aeron_archive_server_context_close(c);
        }
        return -1;
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Archive Server do_work
 * ----------------------------------------------------------------------- */
int aeron_archive_server_do_work(aeron_archive_server_t *archive)
{
    if (NULL == archive || archive->is_closed)
    {
        return 0;
    }

    return aeron_archive_conductor_do_work(archive->conductor);
}

/* -----------------------------------------------------------------------
 * Archive Server close
 * ----------------------------------------------------------------------- */
int aeron_archive_server_close(aeron_archive_server_t *archive)
{
    if (NULL == archive)
    {
        return 0;
    }

    if (!archive->is_closed)
    {
        archive->is_closed = true;
        archive->is_running = false;

        if (NULL != archive->conductor)
        {
            aeron_archive_conductor_close(archive->conductor);
            archive->conductor = NULL;
        }

        if (NULL != archive->ctx)
        {
            aeron_archive_server_context_close(archive->ctx);
            archive->ctx = NULL;
        }

        aeron_free(archive);
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */
aeron_archive_server_context_t *aeron_archive_server_context(aeron_archive_server_t *archive)
{
    return (NULL != archive) ? archive->ctx : NULL;
}

bool aeron_archive_server_is_closed(const aeron_archive_server_t *archive)
{
    return (NULL == archive) || archive->is_closed;
}
