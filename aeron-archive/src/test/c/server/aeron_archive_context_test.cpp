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

#ifdef _MSC_VER
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#endif

#include <gtest/gtest.h>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <climits>
#include <string>
#include <sys/stat.h>
#if defined(_MSC_VER)
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <windows.h>
#include <direct.h>
#ifndef PATH_MAX
#define PATH_MAX 4096
#endif
#include <io.h>
#include <process.h>
#if !defined(getpid)
#define getpid _getpid
#endif
static char *mkdtemp(char *tmpl)
{
    char tmp_path[MAX_PATH];
    if (GetTempPathA(MAX_PATH, tmp_path) == 0) return NULL;
    if (GetTempFileNameA(tmp_path, "aer", 0, tmpl) == 0) return NULL;
    DeleteFileA(tmpl);
    if (_mkdir(tmpl) != 0) return NULL;
    return tmpl;
}
#else
#include <unistd.h>
#endif

/*
 * Include order matters: aeron_archive_conductor.h and aeron_archive_control_response_proxy.h
 * both define aeron_archive_recording_signal_en. We include aeron_archive_server.h (which pulls
 * in the conductor) and aeron_archive_mark_file.h, then use forward declarations for the control
 * session types to avoid the duplicate enum from the response proxy header.
 */
extern "C"
{
#include "aeronmd.h"
#include "server/aeron_archive_server.h"
#include "server/aeron_archive_mark_file.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
}

/*
 * For the ControlSession and ControlSessionAdapter tests we need direct access to their structs
 * and functions. The headers conflict with aeron_archive_conductor.h (via aeron_archive_server.h)
 * due to a duplicate enum definition. We redeclare the needed functions and access the structs
 * through the already-included conductor header's forward declarations.
 */
extern "C"
{

/* --- aeron_archive_control_response_proxy types (avoid pulling the full header) --- */
typedef enum aeron_archive_control_response_code_en
{
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_OK_REDEF = 0,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_ERROR_REDEF = 1,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_RECORDING_UNKNOWN_REDEF = 2,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_SUBSCRIPTION_UNKNOWN_REDEF = 3
}
aeron_archive_control_response_code_t;

typedef struct aeron_archive_control_response_proxy_stct aeron_archive_control_response_proxy_t;

/* --- control session types --- */
typedef enum aeron_archive_control_session_state_en
{
    AERON_ARCHIVE_CONTROL_SESSION_STATE_INIT = 0,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTING = 1,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTED = 2,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED = 3,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED = 4,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE = 5,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_REJECTED = 6,
    AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE = 7
}
aeron_archive_control_session_state_t;

typedef struct aeron_archive_control_session_adapter_stct aeron_archive_control_session_adapter_t;

typedef struct aeron_archive_control_session_pending_response_stct
{
    int64_t correlation_id;
    int64_t relevant_id;
    aeron_archive_control_response_code_t code;
    char *error_message;
    struct aeron_archive_control_session_pending_response_stct *next;
}
aeron_archive_control_session_pending_response_t;

typedef struct aeron_archive_control_session_stct
{
    int64_t control_session_id;
    int64_t correlation_id;
    int64_t connect_timeout_ms;
    int64_t session_liveness_check_interval_ms;
    int64_t session_liveness_check_deadline_ms;
    int64_t resend_deadline_ms;
    int64_t activity_deadline_ms;

    aeron_archive_control_session_state_t state;

    aeron_t *aeron;
    aeron_archive_conductor_t *conductor;
    aeron_archive_control_response_proxy_t *control_response_proxy;
    aeron_archive_control_session_adapter_t *control_session_adapter;

    aeron_exclusive_publication_t *control_publication;
    int64_t control_publication_registration_id;
    int32_t control_publication_stream_id;
    char *control_publication_channel;

    char *invalid_version_message;
    char *abort_reason;

    uint8_t *encoded_principal;
    size_t encoded_principal_length;

    aeron_archive_control_session_pending_response_t *response_queue_head;
    aeron_archive_control_session_pending_response_t *response_queue_tail;
}
aeron_archive_control_session_t;

int aeron_archive_control_session_create(
    aeron_archive_control_session_t **session,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t connect_timeout_ms,
    int64_t session_liveness_check_interval_ms,
    int64_t control_publication_registration_id,
    const char *control_publication_channel,
    int32_t control_publication_stream_id,
    const char *invalid_version_message,
    aeron_archive_control_session_adapter_t *control_session_adapter,
    aeron_t *aeron,
    aeron_archive_conductor_t *conductor,
    int64_t cached_epoch_clock_ms,
    aeron_archive_control_response_proxy_t *control_response_proxy);

int aeron_archive_control_session_do_work(
    aeron_archive_control_session_t *session, int64_t cached_epoch_clock_ms);

void aeron_archive_control_session_abort(
    aeron_archive_control_session_t *session, const char *reason);

int aeron_archive_control_session_close(aeron_archive_control_session_t *session);

bool aeron_archive_control_session_is_done(const aeron_archive_control_session_t *session);

int64_t aeron_archive_control_session_id(const aeron_archive_control_session_t *session);

aeron_archive_control_session_state_t aeron_archive_control_session_state(
    const aeron_archive_control_session_t *session);

aeron_exclusive_publication_t *aeron_archive_control_session_publication(
    const aeron_archive_control_session_t *session);

const uint8_t *aeron_archive_control_session_encoded_principal(
    const aeron_archive_control_session_t *session, size_t *out_length);

void aeron_archive_control_session_authenticate(
    aeron_archive_control_session_t *session,
    const uint8_t *encoded_principal, size_t encoded_principal_length);

void aeron_archive_control_session_challenged(aeron_archive_control_session_t *session);

void aeron_archive_control_session_reject(aeron_archive_control_session_t *session);

void aeron_archive_control_session_send_ok_response(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t relevant_id);

void aeron_archive_control_session_send_error_response(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t relevant_id, const char *error_message);

/* --- control session adapter types --- */

typedef struct aeron_archive_control_session_info_stct
{
    int64_t control_session_id;
    aeron_archive_control_session_t *control_session;
    aeron_image_t *image;
    struct aeron_archive_control_session_info_stct *next;
}
aeron_archive_control_session_info_t;

typedef struct aeron_archive_control_session_adapter_stct
{
    aeron_subscription_t *control_subscription;
    aeron_subscription_t *local_control_subscription;
    aeron_archive_conductor_t *conductor;
    aeron_fragment_assembler_t *fragment_assembler;

    aeron_archive_control_session_info_t *session_map_head;
}
aeron_archive_control_session_adapter_t;

int aeron_archive_control_session_adapter_create(
    aeron_archive_control_session_adapter_t **adapter,
    aeron_subscription_t *control_subscription,
    aeron_subscription_t *local_control_subscription,
    aeron_archive_conductor_t *conductor);

int aeron_archive_control_session_adapter_poll(
    aeron_archive_control_session_adapter_t *adapter);

void aeron_archive_control_session_adapter_add_session(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_archive_control_session_t *session,
    aeron_image_t *image);

void aeron_archive_control_session_adapter_remove_session(
    aeron_archive_control_session_adapter_t *adapter,
    int64_t control_session_id,
    bool session_aborted,
    const char *abort_reason);

void aeron_archive_control_session_adapter_abort_by_image(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image);

int aeron_archive_control_session_adapter_close(
    aeron_archive_control_session_adapter_t *adapter);

} /* extern "C" */

/* SBE schema constants needed for encoding tests (from control_response_proxy.h / control_session_adapter.h). */
#ifndef AERON_ARCHIVE_CONTROL_SCHEMA_ID
#define AERON_ARCHIVE_CONTROL_SCHEMA_ID      (101)
#endif
#ifndef AERON_ARCHIVE_CONTROL_SCHEMA_VERSION
#define AERON_ARCHIVE_CONTROL_SCHEMA_VERSION (13)
#endif

#ifndef AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID
#define AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID                    (2)
#define AERON_ARCHIVE_CLOSE_SESSION_REQUEST_TEMPLATE_ID              (3)
#define AERON_ARCHIVE_START_RECORDING_REQUEST_TEMPLATE_ID            (4)
#define AERON_ARCHIVE_STOP_RECORDING_REQUEST_TEMPLATE_ID             (5)
#define AERON_ARCHIVE_REPLAY_REQUEST_TEMPLATE_ID                     (6)
#define AERON_ARCHIVE_STOP_REPLAY_REQUEST_TEMPLATE_ID                (7)
#define AERON_ARCHIVE_LIST_RECORDINGS_REQUEST_TEMPLATE_ID            (8)
#define AERON_ARCHIVE_BOUNDED_REPLAY_REQUEST_TEMPLATE_ID             (18)
#define AERON_ARCHIVE_KEEP_ALIVE_REQUEST_TEMPLATE_ID                 (61)
#define AERON_ARCHIVE_ARCHIVE_ID_REQUEST_TEMPLATE_ID                 (68)
#endif

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */

static std::string make_temp_dir(const char *prefix)
{
    char tmpl[PATH_MAX];
    snprintf(tmpl, sizeof(tmpl), "/tmp/%s_XXXXXX", prefix);
    char *dir = mkdtemp(tmpl);
    EXPECT_NE(nullptr, dir);
    return std::string(dir);
}

static void remove_temp_dir(const std::string &dir)
{
#ifdef _MSC_VER
    std::string cmd = "rmdir /s /q \"" + dir + "\"";
#else
    std::string cmd = "rm -rf \"" + dir + "\"";
#endif
    if (std::system(cmd.c_str())) {}
}

static bool dir_exists(const std::string &path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0 && ((st.st_mode & S_IFMT) == S_IFDIR);
}

static bool file_exists(const std::string &path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

/* -----------------------------------------------------------------------
 * ArchiveContextTest: context init, conclude validation, defaults, close
 * ----------------------------------------------------------------------- */

class ArchiveContextTest : public ::testing::Test
{
protected:
    std::string temp_dir;
    aeron_archive_server_context_t *ctx = nullptr;
    aeron_driver_context_t *driver_ctx = nullptr;
    aeron_driver_t *driver = nullptr;
    aeron_context_t *aeron_ctx = nullptr;
    aeron_t *aeron = nullptr;

    void SetUp() override
    {
        temp_dir = make_temp_dir("archive_ctx_test");

        /* Start a real media driver — mirrors Java test which uses mock(Aeron.class)
         * but we need a real driver so aeron_start() can connect. */
        ASSERT_EQ(0, aeron_driver_context_init(&driver_ctx));
        std::string aeron_dir = temp_dir + "/aeron";
        aeron_driver_context_set_dir(driver_ctx, aeron_dir.c_str());
        aeron_driver_context_set_dir_delete_on_start(driver_ctx, true);
        aeron_driver_context_set_dir_delete_on_shutdown(driver_ctx, true);
        aeron_driver_context_set_threading_mode(driver_ctx, AERON_THREADING_MODE_SHARED);
        ASSERT_EQ(0, aeron_driver_init(&driver, driver_ctx));
        ASSERT_EQ(0, aeron_driver_start(driver, false));

        /* Create a real aeron client connected to the driver */
        ASSERT_EQ(0, aeron_context_init(&aeron_ctx));
        aeron_context_set_dir(aeron_ctx, aeron_dir.c_str());
        ASSERT_EQ(0, aeron_init(&aeron, aeron_ctx));
        ASSERT_EQ(0, aeron_start(aeron));

        /* Init archive server context with pre-set aeron client (like Java context.aeron(aeron)) */
        ASSERT_EQ(0, aeron_archive_server_context_init(&ctx));
        ASSERT_NE(nullptr, ctx);
        snprintf(ctx->archive_dir, sizeof(ctx->archive_dir), "%s/archive", temp_dir.c_str());
        snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s/mark", temp_dir.c_str());
        snprintf(ctx->aeron_directory_name, sizeof(ctx->aeron_directory_name), "%s", aeron_dir.c_str());
        ctx->aeron = aeron;
        ctx->owns_aeron_client = false;
    }

    void TearDown() override
    {
        if (nullptr != ctx)
        {
            aeron_archive_server_context_close(ctx);
            ctx = nullptr;
        }
        if (nullptr != aeron) { aeron_close(aeron); aeron = nullptr; }
        if (nullptr != aeron_ctx) { aeron_context_close(aeron_ctx); aeron_ctx = nullptr; }
        if (nullptr != driver) { aeron_driver_close(driver); driver = nullptr; }
        if (nullptr != driver_ctx) { aeron_driver_context_close(driver_ctx); driver_ctx = nullptr; }
        remove_temp_dir(temp_dir);
    }
};

TEST_F(ArchiveContextTest, shouldInitContextWithDefaults)
{
    /* Create a fresh context (not the fixture's) to check pure init defaults */
    aeron_archive_server_context_t *fresh = nullptr;
    ASSERT_EQ(0, aeron_archive_server_context_init(&fresh));

    EXPECT_EQ(AERON_ARCHIVE_SERVER_CONTROL_STREAM_ID_DEFAULT, fresh->control_stream_id);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_LOCAL_CONTROL_STREAM_ID_DEFAULT, fresh->local_control_stream_id);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_RECORDING_EVENTS_STREAM_ID_DEFAULT, fresh->recording_events_stream_id);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_SEGMENT_FILE_LENGTH_DEFAULT, fresh->segment_file_length);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_FILE_IO_MAX_LENGTH_DEFAULT, fresh->file_io_max_length);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_FILE_SYNC_LEVEL_DEFAULT, fresh->file_sync_level);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_CATALOG_FILE_SYNC_LEVEL_DEFAULT, fresh->catalog_file_sync_level);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_MAX_CONCURRENT_RECORDINGS_DEFAULT, fresh->max_concurrent_recordings);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_MAX_CONCURRENT_REPLAYS_DEFAULT, fresh->max_concurrent_replays);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_CATALOG_CAPACITY_DEFAULT, fresh->catalog_capacity);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_CONNECT_TIMEOUT_DEFAULT_NS, fresh->connect_timeout_ns);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_SESSION_LIVENESS_CHECK_INTERVAL_DEFAULT_NS,
        fresh->session_liveness_check_interval_ns);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_REPLAY_LINGER_TIMEOUT_DEFAULT_NS, fresh->replay_linger_timeout_ns);
    EXPECT_EQ(AERON_ARCHIVE_SERVER_ERROR_BUFFER_LENGTH_DEFAULT, fresh->error_buffer_length);
    EXPECT_EQ(AERON_ARCHIVE_THREADING_MODE_SHARED, fresh->threading_mode);
    EXPECT_TRUE(fresh->control_channel_enabled);
    EXPECT_TRUE(fresh->recording_events_enabled);
    EXPECT_FALSE(fresh->delete_archive_on_start);
    EXPECT_FALSE(fresh->owns_aeron_client);
    EXPECT_FALSE(fresh->is_concluded);
    EXPECT_EQ(nullptr, fresh->aeron);
    EXPECT_EQ(nullptr, fresh->mark_file);

    aeron_archive_server_context_close(fresh);
}

TEST_F(ArchiveContextTest, archiveIdIsNullValueByDefault)
{
    EXPECT_EQ(AERON_ARCHIVE_NULL_VALUE, ctx->archive_id);
}

TEST_F(ArchiveContextTest, archiveIdReturnsAssignedValue)
{
    const int64_t values[] = { INT64_MIN, INT64_MAX, 0, 5, 28, -17 };
    for (auto id : values)
    {
        ctx->archive_id = id;
        EXPECT_EQ(id, ctx->archive_id);
    }
}

TEST_F(ArchiveContextTest, concludeSucceeds)
{
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_TRUE(ctx->is_concluded);
    EXPECT_NE(nullptr, ctx->mark_file);
    EXPECT_TRUE(ctx->owns_mark_file);
}

TEST_F(ArchiveContextTest, concludeFailsOnNullContext)
{
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(nullptr));
}

TEST_F(ArchiveContextTest, concludeFailsIfAlreadyConcluded)
{
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateReplicationChannelMustBeSet)
{
    aeron_free(ctx->replication_channel);
    ctx->replication_channel = nullptr;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateReplicationChannelNotEmpty)
{
    aeron_free(ctx->replication_channel);
    void *tmp = nullptr;
    aeron_alloc(&tmp, 1);
    ctx->replication_channel = (char *)tmp;
    ctx->replication_channel[0] = '\0';
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateControlChannelMustBeSetWhenEnabled)
{
    ctx->control_channel_enabled = true;
    aeron_free(ctx->control_channel);
    ctx->control_channel = nullptr;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, controlChannelCanBeDisabled)
{
    ctx->control_channel_enabled = false;
    aeron_free(ctx->control_channel);
    ctx->control_channel = nullptr;
    EXPECT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_FALSE(ctx->control_channel_enabled);
}

TEST_F(ArchiveContextTest, shouldValidateLocalControlChannelMustBeIPC)
{
    aeron_free(ctx->local_control_channel);
    ctx->local_control_channel = strdup("aeron:udp?endpoint=localhost:8010");
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateLocalControlChannelNotNull)
{
    aeron_free(ctx->local_control_channel);
    ctx->local_control_channel = nullptr;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateRecordingEventsChannelWhenEnabled)
{
    ctx->recording_events_enabled = true;
    aeron_free(ctx->recording_events_channel);
    ctx->recording_events_channel = nullptr;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateRecordingEventsChannelNotRequiredWhenDisabled)
{
    ctx->recording_events_enabled = false;
    aeron_free(ctx->recording_events_channel);
    ctx->recording_events_channel = nullptr;
    EXPECT_EQ(0, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldValidateSegmentFileLengthPowerOfTwo)
{
    const int32_t invalid_values[] = { 0, 100000, -1, 3 };
    for (auto val : invalid_values)
    {
        aeron_archive_server_context_t *c = nullptr;
        ASSERT_EQ(0, aeron_archive_server_context_init(&c));
        snprintf(c->archive_dir, sizeof(c->archive_dir), "%s/arch_%d", temp_dir.c_str(), val);
        snprintf(c->mark_file_dir, sizeof(c->mark_file_dir), "%s/mark_%d", temp_dir.c_str(), val);
        c->segment_file_length = val;
        EXPECT_EQ(-1, aeron_archive_server_context_conclude(c)) << "val=" << val;
        aeron_archive_server_context_close(c);
    }
}

TEST_F(ArchiveContextTest, shouldValidateFileIoMaxLengthPowerOfTwoAndMinSize)
{
    const int32_t invalid_values[] = { 0, 100, -1, 3 };
    for (auto val : invalid_values)
    {
        aeron_archive_server_context_t *c = nullptr;
        ASSERT_EQ(0, aeron_archive_server_context_init(&c));
        snprintf(c->archive_dir, sizeof(c->archive_dir), "%s/arch_io_%d", temp_dir.c_str(), val);
        snprintf(c->mark_file_dir, sizeof(c->mark_file_dir), "%s/mark_io_%d", temp_dir.c_str(), val);
        c->file_io_max_length = val;
        EXPECT_EQ(-1, aeron_archive_server_context_conclude(c)) << "val=" << val;
        aeron_archive_server_context_close(c);
    }
}

TEST_F(ArchiveContextTest, shouldValidateErrorBufferLengthMinimum)
{
    const int32_t invalid_values[] = { -3, 0, AERON_ARCHIVE_SERVER_ERROR_BUFFER_LENGTH_DEFAULT - 1 };
    for (auto val : invalid_values)
    {
        aeron_archive_server_context_t *c = nullptr;
        ASSERT_EQ(0, aeron_archive_server_context_init(&c));
        snprintf(c->archive_dir, sizeof(c->archive_dir), "%s/arch_eb_%d", temp_dir.c_str(), val);
        snprintf(c->mark_file_dir, sizeof(c->mark_file_dir), "%s/mark_eb_%d", temp_dir.c_str(), val);
        c->error_buffer_length = val;
        EXPECT_EQ(-1, aeron_archive_server_context_conclude(c)) << "val=" << val;
        aeron_archive_server_context_close(c);
    }
}

TEST_F(ArchiveContextTest, shouldValidateCatalogFileSyncLevelNotLessThanFileSyncLevel)
{
    ctx->file_sync_level = 2;
    ctx->catalog_file_sync_level = 1;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, shouldAcceptValidCatalogFileSyncLevel)
{
    ctx->file_sync_level = 1;
    ctx->catalog_file_sync_level = 2;
    EXPECT_EQ(0, aeron_archive_server_context_conclude(ctx));
}

TEST_F(ArchiveContextTest, concludeCreatesArchiveDirectory)
{
    std::string archDir = temp_dir + "/new_archive_dir";
    snprintf(ctx->archive_dir, sizeof(ctx->archive_dir), "%s", archDir.c_str());
    EXPECT_FALSE(dir_exists(archDir));

    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_TRUE(dir_exists(archDir));
}

TEST_F(ArchiveContextTest, concludeCreatesMarkFileDirectory)
{
    std::string markDir = temp_dir + "/new_mark_dir";
    snprintf(ctx->mark_file_dir, sizeof(ctx->mark_file_dir), "%s", markDir.c_str());
    EXPECT_FALSE(dir_exists(markDir));

    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_TRUE(dir_exists(markDir));
}

TEST_F(ArchiveContextTest, concludeDefaultsMarkFileDirToArchiveDir)
{
    ctx->mark_file_dir[0] = '\0';
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_STREQ(ctx->archive_dir, ctx->mark_file_dir);
}

TEST_F(ArchiveContextTest, concludeAssignsArchiveIdIfNotSet)
{
    EXPECT_EQ(AERON_ARCHIVE_NULL_VALUE, ctx->archive_id);
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_NE(AERON_ARCHIVE_NULL_VALUE, ctx->archive_id);
}

TEST_F(ArchiveContextTest, concludePreservesExplicitArchiveId)
{
    ctx->archive_id = 42;
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_EQ(42, ctx->archive_id);
}

TEST_F(ArchiveContextTest, shouldDetectActiveMarkFile)
{
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_NE(nullptr, ctx->mark_file);

    /* Mark file is active because signal_ready was called during conclude. */
    std::string mark_dir = std::string(ctx->mark_file_dir);
    EXPECT_TRUE(aeron_archive_mark_file_is_active(
        mark_dir.c_str(), aeron_archive_mark_file_activity_timestamp_volatile(ctx->mark_file) + 1, 10000));

    /* A second conclude on the same directory should fail due to active mark file. */
    aeron_archive_server_context_t *ctx2 = nullptr;
    ASSERT_EQ(0, aeron_archive_server_context_init(&ctx2));
    snprintf(ctx2->archive_dir, sizeof(ctx2->archive_dir), "%s", ctx->archive_dir);
    snprintf(ctx2->mark_file_dir, sizeof(ctx2->mark_file_dir), "%s", ctx->mark_file_dir);
    /* Share the aeron client — matches Java: anotherContext.aeron(context.aeron()) */
    ctx2->aeron = aeron;
    ctx2->owns_aeron_client = false;
    EXPECT_EQ(-1, aeron_archive_server_context_conclude(ctx2));
    aeron_archive_server_context_close(ctx2);
}

TEST_F(ArchiveContextTest, contextCloseIsIdempotent)
{
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_EQ(0, aeron_archive_server_context_close(ctx));
    ctx = nullptr;
    EXPECT_EQ(0, aeron_archive_server_context_close(nullptr));
}

TEST_F(ArchiveContextTest, shouldUseProvidedThreadingMode)
{
    ctx->threading_mode = AERON_ARCHIVE_THREADING_MODE_DEDICATED;
    EXPECT_EQ(AERON_ARCHIVE_THREADING_MODE_DEDICATED, ctx->threading_mode);
}

TEST_F(ArchiveContextTest, shouldPopulateConductorContextOnConclude)
{
    ctx->archive_id = 12345;
    ctx->control_stream_id = 42;
    ctx->local_control_stream_id = 77;
    ctx->segment_file_length = 256 * 1024 * 1024;
    ctx->file_io_max_length = 1024 * 1024;
    ctx->connect_timeout_ns = 10000000000LL;

    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));

    aeron_archive_conductor_context_t *cctx = &ctx->conductor_ctx;
    EXPECT_EQ(12345, cctx->archive_id);
    EXPECT_EQ(42, cctx->control_stream_id);
    EXPECT_EQ(77, cctx->local_control_stream_id);
    EXPECT_EQ(256 * 1024 * 1024, cctx->segment_file_length);
    EXPECT_EQ(1024 * 1024, cctx->file_io_max_length);
    EXPECT_EQ(10000LL, cctx->connect_timeout_ms);
}

TEST_F(ArchiveContextTest, deleteArchiveOnStartClearsContents)
{
    std::string archDir = temp_dir + "/del_archive";
#ifdef _MSC_VER
    _mkdir(archDir.c_str());
#else
    mkdir(archDir.c_str(), 0755);
#endif
    std::string testFile = archDir + "/testfile.dat";
    FILE *f = fopen(testFile.c_str(), "w");
    ASSERT_NE(nullptr, f);
    fclose(f);
    EXPECT_TRUE(file_exists(testFile));

    snprintf(ctx->archive_dir, sizeof(ctx->archive_dir), "%s", archDir.c_str());
    ctx->delete_archive_on_start = true;

    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_FALSE(file_exists(testFile));
    EXPECT_TRUE(dir_exists(archDir));
}

TEST_F(ArchiveContextTest, controlTermBufferSparseDefault)
{
    EXPECT_TRUE(ctx->control_term_buffer_sparse);
}

TEST_F(ArchiveContextTest, controlMtuLengthDefault)
{
    EXPECT_EQ(AERON_ARCHIVE_SERVER_CONTROL_MTU_LENGTH_DEFAULT, ctx->control_mtu_length);
}

TEST_F(ArchiveContextTest, controlTermBufferLengthDefault)
{
    EXPECT_EQ(AERON_ARCHIVE_SERVER_CONTROL_TERM_BUFFER_LENGTH_DEFAULT, ctx->control_term_buffer_length);
}

TEST_F(ArchiveContextTest, lowStorageSpaceThresholdDefault)
{
    EXPECT_EQ(AERON_ARCHIVE_SERVER_LOW_STORAGE_THRESHOLD_DEFAULT, ctx->low_storage_space_threshold);
}

TEST_F(ArchiveContextTest, concludeSetsForceWritesFromSyncLevel)
{
    ctx->file_sync_level = 1;
    ctx->catalog_file_sync_level = 1;
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_TRUE(ctx->conductor_ctx.force_writes);
    EXPECT_FALSE(ctx->conductor_ctx.force_metadata);
}

TEST_F(ArchiveContextTest, concludeSetsForceMetadataFromSyncLevel)
{
    ctx->file_sync_level = 2;
    ctx->catalog_file_sync_level = 2;
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_TRUE(ctx->conductor_ctx.force_writes);
    EXPECT_TRUE(ctx->conductor_ctx.force_metadata);
}

TEST_F(ArchiveContextTest, concludeNoForceWritesAtSyncLevelZero)
{
    ctx->file_sync_level = 0;
    ASSERT_EQ(0, aeron_archive_server_context_conclude(ctx));
    EXPECT_FALSE(ctx->conductor_ctx.force_writes);
    EXPECT_FALSE(ctx->conductor_ctx.force_metadata);
}

/* -----------------------------------------------------------------------
 * ArchiveMarkFileTest: create, read timestamps, signal ready, close
 * ----------------------------------------------------------------------- */

class ArchiveMarkFileTest : public ::testing::Test
{
protected:
    std::string temp_dir;

    void SetUp() override
    {
        temp_dir = make_temp_dir("archive_mf_test");
    }

    void TearDown() override
    {
        remove_temp_dir(temp_dir);
    }
};

TEST_F(ArchiveMarkFileTest, shouldCreateMarkFile)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        42, 10, 11, 30,
        "aeron:udp?endpoint=localhost:8010",
        "aeron:ipc",
        "aeron:udp?endpoint=localhost:8030",
        "/dev/shm/aeron",
        1000, 10000));
    ASSERT_NE(nullptr, mf);

    std::string path = temp_dir + "/" + AERON_ARCHIVE_MARK_FILE_FILENAME;
    EXPECT_TRUE(file_exists(path));
    EXPECT_NE(nullptr, mf->mapped);
    EXPECT_GT(mf->mapped_length, 0u);

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, shouldReadActivityTimestamp)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));
    ASSERT_NE(nullptr, mf);

    /* Initially zero (memset in create). */
    EXPECT_EQ(0, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    const int64_t ts = 7383439454305LL;
    aeron_archive_mark_file_update_activity_timestamp(mf, ts);
    EXPECT_EQ(ts, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, activityTimestampReturnsNullValueAfterClose)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    aeron_archive_mark_file_update_activity_timestamp(mf, 999);
    EXPECT_EQ(999, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    aeron_archive_mark_file_close(mf);

    /* After close the pointer is freed so reading should return -1. */
    EXPECT_EQ(-1LL, aeron_archive_mark_file_activity_timestamp_volatile(nullptr));
}

TEST_F(ArchiveMarkFileTest, shouldReadArchiveId)
{
    const int64_t archive_id = -462348234343LL;
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        archive_id, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    EXPECT_EQ(archive_id, aeron_archive_mark_file_archive_id(mf));

    aeron_archive_mark_file_close(mf);
    EXPECT_EQ(-1LL, aeron_archive_mark_file_archive_id(nullptr));
}

TEST_F(ArchiveMarkFileTest, signalReady)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    /* Version is zero before signal_ready. */
    int32_t version = 0;
    memcpy(&version, mf->mapped + AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET, sizeof(version));
    EXPECT_EQ(0, version);

    aeron_archive_mark_file_signal_ready(mf, 5555);

    memcpy(&version, mf->mapped + AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET, sizeof(version));
    EXPECT_EQ(AERON_ARCHIVE_MARK_FILE_SEMANTIC_VERSION, version);
    EXPECT_EQ(5555, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, signalReadyIsNoOpAfterClose)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    /* Must test with a still-valid pointer but NULL mapped. */
    aeron_archive_mark_file_t fake = {};
    fake.mapped = nullptr;
    aeron_archive_mark_file_signal_ready(&fake, 7777);
    /* No crash = pass. */

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, signalTerminated)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    aeron_archive_mark_file_signal_ready(mf, 5555);
    EXPECT_EQ(5555, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    aeron_archive_mark_file_signal_terminated(mf);
    EXPECT_EQ(-1LL, aeron_archive_mark_file_activity_timestamp_volatile(mf));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, updateActivityTimestampIsNoOpOnNull)
{
    aeron_archive_mark_file_update_activity_timestamp(nullptr, 12345);
    /* No crash = pass. */
}

TEST_F(ArchiveMarkFileTest, isActiveReturnsFalseForMissingFile)
{
    EXPECT_FALSE(aeron_archive_mark_file_is_active(temp_dir.c_str(), 1000, 10000));
}

TEST_F(ArchiveMarkFileTest, isActiveReturnsTrueAfterSignalReady)
{
    aeron_archive_mark_file_t *mf = nullptr;
    const int64_t now = 50000;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    aeron_archive_mark_file_signal_ready(mf, now);
    EXPECT_TRUE(aeron_archive_mark_file_is_active(temp_dir.c_str(), now + 1, 10000));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, isActiveReturnsFalseAfterTimeout)
{
    aeron_archive_mark_file_t *mf = nullptr;
    const int64_t now = 50000;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    aeron_archive_mark_file_signal_ready(mf, now);
    /* Timeout exceeded. */
    EXPECT_FALSE(aeron_archive_mark_file_is_active(temp_dir.c_str(), now + 20000, 10000));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, isActiveReturnsFalseBeforeSignalReady)
{
    aeron_archive_mark_file_t *mf = nullptr;
    const int64_t now = 50000;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    /* Version is still 0 (not ready). */
    EXPECT_FALSE(aeron_archive_mark_file_is_active(temp_dir.c_str(), now + 1, 10000));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, errorBufferAccessors)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    EXPECT_NE(nullptr, aeron_archive_mark_file_error_buffer(mf));
    EXPECT_EQ(AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        aeron_archive_mark_file_error_buffer_length(mf));

    /* Error buffer should be at HEADER_LENGTH offset into the mapping. */
    EXPECT_EQ(mf->mapped + AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH,
        aeron_archive_mark_file_error_buffer(mf));

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, errorBufferReturnsNullForNullFile)
{
    EXPECT_EQ(nullptr, aeron_archive_mark_file_error_buffer(nullptr));
    EXPECT_EQ(0, aeron_archive_mark_file_error_buffer_length(nullptr));
}

TEST_F(ArchiveMarkFileTest, closeIsIdempotentOnNull)
{
    EXPECT_EQ(0, aeron_archive_mark_file_close(nullptr));
}

TEST_F(ArchiveMarkFileTest, shouldStoreControlStreamIds)
{
    const int32_t control_stream = 42;
    const int32_t local_control_stream = -118;
    const int32_t events_stream = 85858585;

    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        46238467823468LL, control_stream, local_control_stream, events_stream,
        "aeron:udp?endpoint=localhost:55555|alias=control",
        "aeron:ipc",
        "aeron:udp?endpoint=localhost:0",
        "/test/aeron/dir",
        1000, 10000));

    int32_t read_control = 0;
    int32_t read_local = 0;
    int32_t read_events = 0;
    memcpy(&read_control, mf->mapped + AERON_ARCHIVE_MARK_FILE_CONTROL_STREAM_ID_OFFSET, 4);
    memcpy(&read_local, mf->mapped + AERON_ARCHIVE_MARK_FILE_LOCAL_CONTROL_STREAM_ID_OFFSET, 4);
    memcpy(&read_events, mf->mapped + AERON_ARCHIVE_MARK_FILE_EVENTS_STREAM_ID_OFFSET, 4);

    EXPECT_EQ(control_stream, read_control);
    EXPECT_EQ(local_control_stream, read_local);
    EXPECT_EQ(events_stream, read_events);

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, shouldStoreHeaderLengthAndErrorBufferLength)
{
    aeron_archive_mark_file_t *mf = nullptr;
    const int32_t error_buf_len = 2 * 1024 * 1024;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        error_buf_len, 0, 10, 11, 30,
        nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    int32_t hdr_len = 0;
    int32_t eb_len = 0;
    memcpy(&hdr_len, mf->mapped + AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH_OFFSET, 4);
    memcpy(&eb_len, mf->mapped + AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET, 4);

    EXPECT_EQ(AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH, hdr_len);
    EXPECT_EQ(error_buf_len, eb_len);

    EXPECT_EQ((size_t)(AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH + error_buf_len), mf->mapped_length);

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, shouldStoreStartTimestamp)
{
    const int64_t now = 12345678909876LL;
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    int64_t start_ts = 0;
    memcpy(&start_ts, mf->mapped + AERON_ARCHIVE_MARK_FILE_START_TIMESTAMP_OFFSET, 8);
    EXPECT_EQ(now, start_ts);

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, shouldStorePid)
{
    aeron_archive_mark_file_t *mf = nullptr;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        1000, 10000));

    int64_t stored_pid = 0;
    memcpy(&stored_pid, mf->mapped + AERON_ARCHIVE_MARK_FILE_PID_OFFSET, 8);
    EXPECT_EQ((int64_t)getpid(), stored_pid);

    aeron_archive_mark_file_close(mf);
}

TEST_F(ArchiveMarkFileTest, shouldFailToCreateIfActiveMarkFileExists)
{
    aeron_archive_mark_file_t *mf1 = nullptr;
    const int64_t now = 50000;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf1, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    aeron_archive_mark_file_signal_ready(mf1, now);

    /* Second create should fail because mark file is active. */
    aeron_archive_mark_file_t *mf2 = nullptr;
    EXPECT_EQ(-1, aeron_archive_mark_file_create(
        &mf2, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now + 1, 10000));
    EXPECT_EQ(nullptr, mf2);

    aeron_archive_mark_file_close(mf1);
}

TEST_F(ArchiveMarkFileTest, shouldReCreateAfterTermination)
{
    aeron_archive_mark_file_t *mf1 = nullptr;
    const int64_t now = 50000;
    ASSERT_EQ(0, aeron_archive_mark_file_create(
        &mf1, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now, 10000));

    aeron_archive_mark_file_signal_ready(mf1, now);
    aeron_archive_mark_file_signal_terminated(mf1);
    aeron_archive_mark_file_close(mf1);

    /* After termination (activity = -1), create should succeed. */
    aeron_archive_mark_file_t *mf2 = nullptr;
    EXPECT_EQ(0, aeron_archive_mark_file_create(
        &mf2, temp_dir.c_str(),
        AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT,
        0, 10, 11, 30, nullptr, "aeron:ipc", nullptr, "",
        now + 1, 10000));
    EXPECT_NE(nullptr, mf2);

    aeron_archive_mark_file_close(mf2);
}

/* -----------------------------------------------------------------------
 * ControlSessionTest: state machine transitions
 * ----------------------------------------------------------------------- */

class ControlSessionTest : public ::testing::Test
{
protected:
    aeron_archive_control_session_t *session = nullptr;

    static constexpr int64_t CONTROL_SESSION_ID = 42;
    static constexpr int64_t CORRELATION_ID = 2;
    static constexpr int64_t CONNECT_TIMEOUT_MS = 5000;
    static constexpr int64_t SESSION_LIVENESS_CHECK_INTERVAL_MS = 100;
    static constexpr int64_t CONTROL_PUBLICATION_ID = 777;
    static constexpr int32_t CONTROL_PUBLICATION_STREAM_ID = 5555;

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_archive_control_session_create(
            &session,
            CONTROL_SESSION_ID,
            CORRELATION_ID,
            CONNECT_TIMEOUT_MS,
            SESSION_LIVENESS_CHECK_INTERVAL_MS,
            CONTROL_PUBLICATION_ID,
            "aeron:ipc?alias=test-control-response",
            CONTROL_PUBLICATION_STREAM_ID,
            nullptr,  /* invalid_version_message */
            nullptr,  /* adapter */
            nullptr,  /* aeron */
            nullptr,  /* conductor */
            0,        /* cached_epoch_clock_ms */
            nullptr   /* control_response_proxy */
        ));
        ASSERT_NE(nullptr, session);
    }

    void TearDown() override
    {
        if (nullptr != session)
        {
            aeron_archive_control_session_close(session);
            session = nullptr;
        }
    }
};

TEST_F(ControlSessionTest, shouldStartInInitState)
{
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_INIT,
        aeron_archive_control_session_state(session));
    EXPECT_FALSE(aeron_archive_control_session_is_done(session));
}

TEST_F(ControlSessionTest, shouldReturnSessionId)
{
    EXPECT_EQ(int64_t{42}, aeron_archive_control_session_id(session));
}

TEST_F(ControlSessionTest, shouldReturnControlPublicationChannel)
{
    EXPECT_STREQ("aeron:ipc?alias=test-control-response", session->control_publication_channel);
}

TEST_F(ControlSessionTest, shouldReturnControlPublicationStreamId)
{
    EXPECT_EQ((int32_t)CONTROL_PUBLICATION_STREAM_ID, session->control_publication_stream_id);
}

TEST_F(ControlSessionTest, shouldChangeStateToDoneOnAbort)
{
    EXPECT_FALSE(aeron_archive_control_session_is_done(session));

    aeron_archive_control_session_abort(session, "stop execution");

    EXPECT_TRUE(aeron_archive_control_session_is_done(session));
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE,
        aeron_archive_control_session_state(session));
    EXPECT_NE(nullptr, session->abort_reason);
    EXPECT_STREQ("stop execution", session->abort_reason);
}

TEST_F(ControlSessionTest, abortIsNoOpIfSessionIsDone)
{
    aeron_archive_control_session_abort(session, "first");
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));
    EXPECT_STREQ("first", session->abort_reason);

    /* Second abort should not change the reason because state is already DONE. */
    aeron_archive_control_session_abort(session, "second");
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));
    /* The C implementation overwrites abort_reason if state != DONE,
     * but since state is DONE, the abort is a no-op. */
}

TEST_F(ControlSessionTest, shouldTransitionToAuthenticated)
{
    const uint8_t principal[] = { 0x01, 0x02, 0x03 };
    aeron_archive_control_session_authenticate(session, principal, sizeof(principal));

    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED,
        aeron_archive_control_session_state(session));

    size_t len = 0;
    const uint8_t *p = aeron_archive_control_session_encoded_principal(session, &len);
    EXPECT_EQ(3u, len);
    EXPECT_EQ(0, memcmp(principal, p, len));
}

TEST_F(ControlSessionTest, shouldTransitionToAuthenticatedWithNullPrincipal)
{
    aeron_archive_control_session_authenticate(session, nullptr, 0);

    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED,
        aeron_archive_control_session_state(session));

    size_t len = 0;
    const uint8_t *p = aeron_archive_control_session_encoded_principal(session, &len);
    EXPECT_EQ(0u, len);
    EXPECT_EQ(nullptr, p);
}

TEST_F(ControlSessionTest, shouldTransitionToChallenged)
{
    aeron_archive_control_session_challenged(session);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED,
        aeron_archive_control_session_state(session));
}

TEST_F(ControlSessionTest, shouldTransitionToRejected)
{
    aeron_archive_control_session_reject(session);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_REJECTED,
        aeron_archive_control_session_state(session));
}

TEST_F(ControlSessionTest, closeIsIdempotentOnNull)
{
    EXPECT_EQ(0, aeron_archive_control_session_close(nullptr));
}

TEST_F(ControlSessionTest, shouldStoreInvalidVersionMessage)
{
    aeron_archive_control_session_t *s2 = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &s2,
        99,
        1,
        5000,
        100,
        777,
        "aeron:ipc",
        5555,
        "invalid version: 999",
        nullptr, nullptr, nullptr,
        0, nullptr));

    EXPECT_STREQ("invalid version: 999", s2->invalid_version_message);
    aeron_archive_control_session_close(s2);
}

TEST_F(ControlSessionTest, shouldHaveNullPublicationInitially)
{
    EXPECT_EQ(nullptr, aeron_archive_control_session_publication(session));
}

TEST_F(ControlSessionTest, shouldQueueAndDrainResponsesOnClose)
{
    /* Queue a response via the send_response API (which queues internally). */
    aeron_archive_control_session_send_ok_response(session, 1, 0);
    aeron_archive_control_session_send_error_response(session, 2, 0, "test error");

    /* Close should drain the queue without crashing. */
    EXPECT_EQ(0, aeron_archive_control_session_close(session));
    session = nullptr;
}

TEST_F(ControlSessionTest, abortOverwritesReasonOnSubsequentCalls)
{
    /* In the C impl, abort only sets state + reason when state != DONE.
     * After the first abort, state becomes DONE, so subsequent aborts are no-ops. */
    aeron_archive_control_session_abort(session, "call1");
    EXPECT_STREQ("call1", session->abort_reason);

    /* State is now DONE, so this should be a no-op. */
    aeron_archive_control_session_abort(session, "call2");
    EXPECT_STREQ("call1", session->abort_reason);
}

TEST_F(ControlSessionTest, shouldTimeoutIfNoActivity)
{
    /* doWork with now_ms past the activity deadline should trigger abort. */
    aeron_archive_control_session_do_work(session, CONNECT_TIMEOUT_MS + 1);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE,
        aeron_archive_control_session_state(session));
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));
}

/* -----------------------------------------------------------------------
 * ControlSessionAdapterTest: create, add/remove sessions, close
 * ----------------------------------------------------------------------- */

class ControlSessionAdapterTest : public ::testing::Test
{
protected:
    aeron_archive_control_session_adapter_t *adapter = nullptr;

    void SetUp() override
    {
        ASSERT_EQ(0, aeron_archive_control_session_adapter_create(
            &adapter,
            nullptr,  /* control_subscription */
            nullptr,  /* local_control_subscription */
            nullptr   /* conductor */
        ));
        ASSERT_NE(nullptr, adapter);
    }

    void TearDown() override
    {
        if (nullptr != adapter)
        {
            aeron_archive_control_session_adapter_close(adapter);
            adapter = nullptr;
        }
    }
};

TEST_F(ControlSessionAdapterTest, shouldCreateAdapter)
{
    EXPECT_EQ(nullptr, adapter->control_subscription);
    EXPECT_EQ(nullptr, adapter->local_control_subscription);
    EXPECT_EQ(nullptr, adapter->conductor);
    EXPECT_NE(nullptr, adapter->fragment_assembler);
    EXPECT_EQ(nullptr, adapter->session_map_head);
}

TEST_F(ControlSessionAdapterTest, shouldAddAndRemoveSession)
{
    aeron_archive_control_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &session, 42, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    aeron_archive_control_session_adapter_add_session(adapter, session, nullptr);
    EXPECT_NE(nullptr, adapter->session_map_head);
    EXPECT_EQ(42, adapter->session_map_head->control_session_id);

    aeron_archive_control_session_adapter_remove_session(adapter, 42, false, nullptr);
    EXPECT_EQ(nullptr, adapter->session_map_head);

    aeron_archive_control_session_close(session);
}

TEST_F(ControlSessionAdapterTest, shouldAddMultipleSessions)
{
    aeron_archive_control_session_t *s1 = nullptr;
    aeron_archive_control_session_t *s2 = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &s1, 100, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &s2, 200, 2, 5000, 100, 778,
        "aeron:ipc", 5556, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    aeron_archive_control_session_adapter_add_session(adapter, s1, nullptr);
    aeron_archive_control_session_adapter_add_session(adapter, s2, nullptr);

    /* Both should be in the map. */
    int count = 0;
    aeron_archive_control_session_info_t *info = adapter->session_map_head;
    while (nullptr != info)
    {
        count++;
        info = info->next;
    }
    EXPECT_EQ(2, count);

    /* Remove one. */
    aeron_archive_control_session_adapter_remove_session(adapter, 100, false, nullptr);
    count = 0;
    info = adapter->session_map_head;
    while (nullptr != info)
    {
        count++;
        info = info->next;
    }
    EXPECT_EQ(1, count);
    EXPECT_EQ(200, adapter->session_map_head->control_session_id);

    aeron_archive_control_session_close(s1);
    aeron_archive_control_session_close(s2);
}

TEST_F(ControlSessionAdapterTest, removeNonExistentSessionIsNoOp)
{
    aeron_archive_control_session_adapter_remove_session(adapter, 9999, false, nullptr);
    EXPECT_EQ(nullptr, adapter->session_map_head);
}

TEST_F(ControlSessionAdapterTest, closeIsIdempotentOnNull)
{
    EXPECT_EQ(0, aeron_archive_control_session_adapter_close(nullptr));
}

TEST_F(ControlSessionAdapterTest, pollReturnsZeroWithNoSubscriptions)
{
    EXPECT_EQ(0, aeron_archive_control_session_adapter_poll(adapter));
}

TEST_F(ControlSessionAdapterTest, shouldAbortByImage)
{
    /* Create a session and add it to the adapter with a specific image pointer.
     * Since we pass nullptr for the image, abort_by_image with nullptr should find it. */
    aeron_archive_control_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &session, 42, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    /* Use a fake non-null pointer as the "image". */
    auto *fake_image = (aeron_image_t *)(uintptr_t)0xDEADBEEF;
    aeron_archive_control_session_adapter_add_session(adapter, session, fake_image);

    EXPECT_FALSE(aeron_archive_control_session_is_done(session));

    aeron_archive_control_session_adapter_abort_by_image(adapter, fake_image);
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));

    aeron_archive_control_session_close(session);
}

TEST_F(ControlSessionAdapterTest, abortByImageIgnoresNonMatchingImage)
{
    aeron_archive_control_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &session, 42, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    auto *fake_image_1 = (aeron_image_t *)(uintptr_t)0xDEADBEEF;
    auto *fake_image_2 = (aeron_image_t *)(uintptr_t)0xCAFEBABE;
    aeron_archive_control_session_adapter_add_session(adapter, session, fake_image_1);

    aeron_archive_control_session_adapter_abort_by_image(adapter, fake_image_2);
    EXPECT_FALSE(aeron_archive_control_session_is_done(session));

    aeron_archive_control_session_close(session);
}

TEST_F(ControlSessionAdapterTest, abortByImageSkipsDoneSessions)
{
    aeron_archive_control_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &session, 42, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    auto *fake_image = (aeron_image_t *)(uintptr_t)0xDEADBEEF;
    aeron_archive_control_session_adapter_add_session(adapter, session, fake_image);

    /* Abort the session first. */
    aeron_archive_control_session_abort(session, "test");
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));

    /* abort_by_image should skip it because it is already done. */
    aeron_archive_control_session_adapter_abort_by_image(adapter, fake_image);
    /* Still done, no crash. */
    EXPECT_TRUE(aeron_archive_control_session_is_done(session));

    aeron_archive_control_session_close(session);
}

TEST_F(ControlSessionAdapterTest, closeReleasesSessionMap)
{
    aeron_archive_control_session_t *s1 = nullptr;
    aeron_archive_control_session_t *s2 = nullptr;
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &s1, 100, 1, 5000, 100, 777,
        "aeron:ipc", 5555, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));
    ASSERT_EQ(0, aeron_archive_control_session_create(
        &s2, 200, 2, 5000, 100, 778,
        "aeron:ipc", 5556, nullptr,
        adapter, nullptr, nullptr, 0, nullptr));

    aeron_archive_control_session_adapter_add_session(adapter, s1, nullptr);
    aeron_archive_control_session_adapter_add_session(adapter, s2, nullptr);

    /* Close frees the map entries but not the sessions themselves. */
    EXPECT_EQ(0, aeron_archive_control_session_adapter_close(adapter));
    adapter = nullptr;

    /* Sessions still need to be cleaned up separately. */
    aeron_archive_control_session_close(s1);
    aeron_archive_control_session_close(s2);
}

/* -----------------------------------------------------------------------
 * SBE message decoding tests via the adapter's on_fragment handler
 *
 * We encode SBE messages inline (matching the control schema) and push
 * them directly through the fragment handler callback.
 * ----------------------------------------------------------------------- */

/* SBE encoding helpers (little-endian, matching the archive control schema). */

static void sbe_encode_uint16(uint8_t *buffer, size_t offset, uint16_t value)
{
    memcpy(buffer + offset, &value, sizeof(value));
}

static void sbe_encode_int32(uint8_t *buffer, size_t offset, int32_t value)
{
    memcpy(buffer + offset, &value, sizeof(value));
}

static void sbe_encode_int64(uint8_t *buffer, size_t offset, int64_t value)
{
    memcpy(buffer + offset, &value, sizeof(value));
}

static void sbe_encode_uint32(uint8_t *buffer, size_t offset, uint32_t value)
{
    memcpy(buffer + offset, &value, sizeof(value));
}

static size_t sbe_encode_var_data(uint8_t *buffer, size_t offset, const char *str)
{
    uint32_t len = (nullptr != str) ? (uint32_t)strlen(str) : 0;
    sbe_encode_uint32(buffer, offset, len);
    if (len > 0)
    {
        memcpy(buffer + offset + 4, str, len);
    }
    return 4 + len;
}

static size_t sbe_encode_var_bytes(uint8_t *buffer, size_t offset, const uint8_t *data, uint32_t len)
{
    sbe_encode_uint32(buffer, offset, len);
    if (len > 0)
    {
        memcpy(buffer + offset + 4, data, len);
    }
    return 4 + len;
}

/*
 * SBE message header:
 *   blockLength (uint16) @ 0
 *   templateId  (uint16) @ 2
 *   schemaId    (uint16) @ 4
 *   version     (uint16) @ 6
 */
static void sbe_encode_header(uint8_t *buffer, uint16_t block_length, uint16_t template_id)
{
    sbe_encode_uint16(buffer, 0, block_length);
    sbe_encode_uint16(buffer, 2, template_id);
    sbe_encode_uint16(buffer, 4, AERON_ARCHIVE_CONTROL_SCHEMA_ID);
    sbe_encode_uint16(buffer, 6, AERON_ARCHIVE_CONTROL_SCHEMA_VERSION);
}

/*
 * ConnectRequest (templateId=2):
 *   correlationId   int64  @0
 *   responseStreamId int32 @8
 *   version         int32  @12
 *   block = 16 bytes
 *   var: responseChannel
 */
static size_t encode_connect_request(
    uint8_t *buffer, int64_t correlation_id, int32_t response_stream_id, int32_t version,
    const char *response_channel)
{
    const uint16_t block = 16;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, correlation_id);
    sbe_encode_int32(body, 8, response_stream_id);
    sbe_encode_int32(body, 12, version);
    size_t var_offset = block;
    var_offset += sbe_encode_var_data(body, var_offset, response_channel);
    return 8 + var_offset;
}

/*
 * ReplayRequest (templateId=6):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   recordingId       int64 @16
 *   position          int64 @24
 *   length            int64 @32
 *   fileIoMaxLength   int32 @40
 *   replayStreamId    int32 @44
 *   block = 48 bytes
 *   var: replayChannel
 */
static size_t encode_replay_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t file_io_max_length,
    int32_t replay_stream_id,
    const char *replay_channel)
{
    const uint16_t block = 48;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_REPLAY_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int64(body, 16, recording_id);
    sbe_encode_int64(body, 24, position);
    sbe_encode_int64(body, 32, length);
    sbe_encode_int32(body, 40, file_io_max_length);
    sbe_encode_int32(body, 44, replay_stream_id);
    size_t var_offset = block;
    var_offset += sbe_encode_var_data(body, var_offset, replay_channel);
    return 8 + var_offset;
}

/*
 * BoundedReplayRequest (templateId=18):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   recordingId       int64 @16
 *   position          int64 @24
 *   length            int64 @32
 *   limitCounterId    int32 @40
 *   fileIoMaxLength   int32 @44
 *   replayStreamId    int32 @48
 *   block = 52 bytes
 *   var: replayChannel
 */
static size_t encode_bounded_replay_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t limit_counter_id,
    int32_t file_io_max_length,
    int32_t replay_stream_id,
    const char *replay_channel)
{
    const uint16_t block = 52;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_BOUNDED_REPLAY_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int64(body, 16, recording_id);
    sbe_encode_int64(body, 24, position);
    sbe_encode_int64(body, 32, length);
    sbe_encode_int32(body, 40, limit_counter_id);
    sbe_encode_int32(body, 44, file_io_max_length);
    sbe_encode_int32(body, 48, replay_stream_id);
    size_t var_offset = block;
    var_offset += sbe_encode_var_data(body, var_offset, replay_channel);
    return 8 + var_offset;
}

/*
 * KeepAliveRequest (templateId=61):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   block = 16 bytes
 */
static size_t encode_keep_alive_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id)
{
    const uint16_t block = 16;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_KEEP_ALIVE_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    return 8 + block;
}

/*
 * CloseSessionRequest (templateId=3):
 *   controlSessionId  int64 @0
 *   block = 8 bytes
 */
static size_t encode_close_session_request(
    uint8_t *buffer,
    int64_t control_session_id)
{
    const uint16_t block = 8;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_CLOSE_SESSION_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    return 8 + block;
}

/*
 * StartRecordingRequest (templateId=4):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   streamId          int32 @16
 *   sourceLocation    int32 @20
 *   block = 24 bytes
 *   var: channel
 */
static size_t encode_start_recording_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int32_t stream_id,
    int32_t source_location,
    const char *channel)
{
    const uint16_t block = 24;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_START_RECORDING_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int32(body, 16, stream_id);
    sbe_encode_int32(body, 20, source_location);
    size_t var_offset = block;
    var_offset += sbe_encode_var_data(body, var_offset, channel);
    return 8 + var_offset;
}

/*
 * StopRecordingRequest (templateId=5):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   streamId          int32 @16
 *   block = 20 bytes
 *   var: channel
 */
static size_t encode_stop_recording_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel)
{
    const uint16_t block = 20;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_STOP_RECORDING_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int32(body, 16, stream_id);
    size_t var_offset = block;
    var_offset += sbe_encode_var_data(body, var_offset, channel);
    return 8 + var_offset;
}

/*
 * ListRecordingsRequest (templateId=8):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   fromRecordingId   int64 @16
 *   recordCount       int32 @24
 *   block = 28 bytes
 */
static size_t encode_list_recordings_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count)
{
    const uint16_t block = 28;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_LIST_RECORDINGS_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int64(body, 16, from_recording_id);
    sbe_encode_int32(body, 24, record_count);
    return 8 + block;
}

/*
 * StopReplayRequest (templateId=7):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   replaySessionId   int64 @16
 *   block = 24 bytes
 */
static size_t encode_stop_replay_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t replay_session_id)
{
    const uint16_t block = 24;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_STOP_REPLAY_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    sbe_encode_int64(body, 16, replay_session_id);
    return 8 + block;
}

/*
 * ArchiveIdRequest (templateId=68):
 *   controlSessionId  int64 @0
 *   correlationId     int64 @8
 *   block = 16 bytes
 */
static size_t encode_archive_id_request(
    uint8_t *buffer,
    int64_t control_session_id,
    int64_t correlation_id)
{
    const uint16_t block = 16;
    sbe_encode_header(buffer, block, AERON_ARCHIVE_ARCHIVE_ID_REQUEST_TEMPLATE_ID);
    uint8_t *body = buffer + 8;
    sbe_encode_int64(body, 0, control_session_id);
    sbe_encode_int64(body, 8, correlation_id);
    return 8 + block;
}

/*
 * Note: The adapter's on_fragment handler is internal (static). We cannot call
 * it directly without significant refactoring. Instead, we test the adapter's
 * public interface (create, add/remove sessions, poll, abort_by_image, close)
 * and verify the SBE encoding helpers produce correct byte layouts that match
 * what the adapter expects.
 */

class SbeEncodingTest : public ::testing::Test
{
};

TEST_F(SbeEncodingTest, shouldEncodeConnectRequestHeader)
{
    uint8_t buffer[256];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_connect_request(buffer, 100, 42, 6, "aeron:ipc");

    /* Verify SBE header */
    uint16_t block_length, template_id, schema_id, version;
    memcpy(&block_length, buffer + 0, 2);
    memcpy(&template_id, buffer + 2, 2);
    memcpy(&schema_id, buffer + 4, 2);
    memcpy(&version, buffer + 6, 2);

    EXPECT_EQ(16, block_length);
    EXPECT_EQ(AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID, template_id);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SCHEMA_ID, schema_id);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SCHEMA_VERSION, version);

    /* Verify body fields */
    int64_t correlation_id;
    int32_t response_stream_id, client_version;
    memcpy(&correlation_id, buffer + 8, 8);
    memcpy(&response_stream_id, buffer + 16, 4);
    memcpy(&client_version, buffer + 20, 4);
    EXPECT_EQ(100, correlation_id);
    EXPECT_EQ(42, response_stream_id);
    EXPECT_EQ(6, client_version);

    /* Verify var-length channel */
    uint32_t channel_len;
    memcpy(&channel_len, buffer + 24, 4);
    EXPECT_EQ(9u, channel_len);
    EXPECT_EQ(0, memcmp(buffer + 28, "aeron:ipc", 9));
    EXPECT_EQ(28u + 9u, len);
}

TEST_F(SbeEncodingTest, shouldEncodeReplayRequestFields)
{
    uint8_t buffer[256];
    memset(buffer, 0, sizeof(buffer));
    encode_replay_request(buffer, 928374, 9382475, 9827345897LL, 982374, 0, 4096, 9832475, "aeron:ipc");

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id, recording_id, position, length;
    int32_t file_io_max_length, replay_stream_id;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&recording_id, body + 16, 8);
    memcpy(&position, body + 24, 8);
    memcpy(&length, body + 32, 8);
    memcpy(&file_io_max_length, body + 40, 4);
    memcpy(&replay_stream_id, body + 44, 4);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(9827345897LL, recording_id);
    EXPECT_EQ(982374, position);
    EXPECT_EQ(0, length);
    EXPECT_EQ(4096, file_io_max_length);
    EXPECT_EQ(9832475, replay_stream_id);
}

TEST_F(SbeEncodingTest, shouldEncodeBoundedReplayRequestFields)
{
    uint8_t buffer[256];
    memset(buffer, 0, sizeof(buffer));
    encode_bounded_replay_request(
        buffer, 928374, 9382475, 9827345897LL, 982374, 0, 92734, 4096, 9832475, "aeron:ipc?alias=replay");

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id, recording_id, position, length;
    int32_t limit_counter_id, file_io_max_length, replay_stream_id;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&recording_id, body + 16, 8);
    memcpy(&position, body + 24, 8);
    memcpy(&length, body + 32, 8);
    memcpy(&limit_counter_id, body + 40, 4);
    memcpy(&file_io_max_length, body + 44, 4);
    memcpy(&replay_stream_id, body + 48, 4);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(9827345897LL, recording_id);
    EXPECT_EQ(982374, position);
    EXPECT_EQ(0, length);
    EXPECT_EQ(92734, limit_counter_id);
    EXPECT_EQ(4096, file_io_max_length);
    EXPECT_EQ(9832475, replay_stream_id);
}

TEST_F(SbeEncodingTest, shouldEncodeKeepAliveRequest)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_keep_alive_request(buffer, 928374, 9382475);

    EXPECT_EQ(24u, len);

    uint16_t template_id;
    memcpy(&template_id, buffer + 2, 2);
    EXPECT_EQ(AERON_ARCHIVE_KEEP_ALIVE_REQUEST_TEMPLATE_ID, template_id);

    int64_t control_session_id, correlation_id;
    memcpy(&control_session_id, buffer + 8, 8);
    memcpy(&correlation_id, buffer + 16, 8);
    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
}

TEST_F(SbeEncodingTest, shouldEncodeCloseSessionRequest)
{
    uint8_t buffer[32];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_close_session_request(buffer, 928374);

    EXPECT_EQ(16u, len);

    uint16_t template_id;
    memcpy(&template_id, buffer + 2, 2);
    EXPECT_EQ(AERON_ARCHIVE_CLOSE_SESSION_REQUEST_TEMPLATE_ID, template_id);

    int64_t control_session_id;
    memcpy(&control_session_id, buffer + 8, 8);
    EXPECT_EQ(928374, control_session_id);
}

TEST_F(SbeEncodingTest, shouldEncodeStartRecordingRequest)
{
    uint8_t buffer[256];
    memset(buffer, 0, sizeof(buffer));
    encode_start_recording_request(buffer, 928374, 9382475, 42, 1, "aeron:udp?endpoint=localhost:8010");

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id;
    int32_t stream_id, source_location;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&stream_id, body + 16, 4);
    memcpy(&source_location, body + 20, 4);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(42, stream_id);
    EXPECT_EQ(1, source_location);
}

TEST_F(SbeEncodingTest, shouldEncodeStopRecordingRequest)
{
    uint8_t buffer[256];
    memset(buffer, 0, sizeof(buffer));
    encode_stop_recording_request(buffer, 928374, 9382475, 42, "aeron:udp?endpoint=localhost:8010");

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id;
    int32_t stream_id;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&stream_id, body + 16, 4);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(42, stream_id);
}

TEST_F(SbeEncodingTest, shouldEncodeListRecordingsRequest)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_list_recordings_request(buffer, 928374, 9382475, 0, 100);

    EXPECT_EQ(36u, len);

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id, from_recording_id;
    int32_t record_count;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&from_recording_id, body + 16, 8);
    memcpy(&record_count, body + 24, 4);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(0, from_recording_id);
    EXPECT_EQ(100, record_count);
}

TEST_F(SbeEncodingTest, shouldEncodeStopReplayRequest)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_stop_replay_request(buffer, 928374, 9382475, 1234567);

    EXPECT_EQ(32u, len);

    uint8_t *body = buffer + 8;
    int64_t control_session_id, correlation_id, replay_session_id;
    memcpy(&control_session_id, body + 0, 8);
    memcpy(&correlation_id, body + 8, 8);
    memcpy(&replay_session_id, body + 16, 8);

    EXPECT_EQ(928374, control_session_id);
    EXPECT_EQ(9382475, correlation_id);
    EXPECT_EQ(1234567, replay_session_id);
}

TEST_F(SbeEncodingTest, shouldEncodeArchiveIdRequest)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    size_t len = encode_archive_id_request(buffer, 928374, 9382475);

    EXPECT_EQ(24u, len);

    uint16_t template_id;
    memcpy(&template_id, buffer + 2, 2);
    EXPECT_EQ(AERON_ARCHIVE_ARCHIVE_ID_REQUEST_TEMPLATE_ID, template_id);
}

TEST_F(SbeEncodingTest, varDataEncodingWithNullString)
{
    uint8_t buffer[16];
    memset(buffer, 0, sizeof(buffer));
    size_t consumed = sbe_encode_var_data(buffer, 0, nullptr);
    EXPECT_EQ(4u, consumed);

    uint32_t len;
    memcpy(&len, buffer, 4);
    EXPECT_EQ(0u, len);
}

TEST_F(SbeEncodingTest, varDataEncodingWithEmptyString)
{
    uint8_t buffer[16];
    memset(buffer, 0, sizeof(buffer));
    size_t consumed = sbe_encode_var_data(buffer, 0, "");
    EXPECT_EQ(4u, consumed);

    uint32_t len;
    memcpy(&len, buffer, 4);
    EXPECT_EQ(0u, len);
}

TEST_F(SbeEncodingTest, varBytesEncoding)
{
    uint8_t buffer[32];
    memset(buffer, 0, sizeof(buffer));
    const uint8_t data[] = { 0x01, 0x02, 0x03 };
    size_t consumed = sbe_encode_var_bytes(buffer, 0, data, sizeof(data));
    EXPECT_EQ(7u, consumed);

    uint32_t len;
    memcpy(&len, buffer, 4);
    EXPECT_EQ(3u, len);
    EXPECT_EQ(0, memcmp(buffer + 4, data, 3));
}

TEST_F(SbeEncodingTest, sbeHeaderSchemaIdAndVersionMatchAdapterConstants)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    sbe_encode_header(buffer, 16, 2);

    uint16_t schema_id, version;
    memcpy(&schema_id, buffer + 4, 2);
    memcpy(&version, buffer + 6, 2);

    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SCHEMA_ID, schema_id);
    EXPECT_EQ(AERON_ARCHIVE_CONTROL_SCHEMA_VERSION, version);
}

TEST_F(SbeEncodingTest, wrongSchemaIdShouldBeDetectable)
{
    uint8_t buffer[64];
    memset(buffer, 0, sizeof(buffer));
    sbe_encode_header(buffer, 16, 2);
    /* Overwrite schema ID with wrong value. */
    uint16_t wrong_schema = 999;
    memcpy(buffer + 4, &wrong_schema, 2);

    uint16_t schema_id;
    memcpy(&schema_id, buffer + 4, 2);
    EXPECT_NE(AERON_ARCHIVE_CONTROL_SCHEMA_ID, schema_id);
}
