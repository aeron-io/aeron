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

extern "C"
{
#include <inttypes.h>
#include <sys/stat.h>
#if defined(_MSC_VER)
#include <windows.h>
#include <direct.h>
#include <io.h>
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
#include <fcntl.h>

#include "aeron_alloc.h"
#include "util/aeron_bitutil.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"

#include "server/aeron_archive_replay_session.h"
#include "server/aeron_archive_recording_reader.h"
#include "server/aeron_archive_recording_writer.h"
#include "server/aeron_archive_catalog.h"
#include "server/aeron_archive_list_recordings_session.h"
}

/* ============================================================================
 * Constants mirroring the Java ReplaySessionTest
 * ============================================================================ */

static const int64_t RECORDING_ID = 0;
static const int32_t TERM_BUFFER_LENGTH = AERON_LOGBUFFER_TERM_MIN_LENGTH;  /* 64 KiB */
static const int32_t SEGMENT_LENGTH = TERM_BUFFER_LENGTH;
static const int32_t INITIAL_TERM_ID = 8231773;
static const int32_t INITIAL_TERM_OFFSET = 1024;
static const int64_t REPLAY_ID = 1;
static const int64_t START_POSITION = INITIAL_TERM_OFFSET;
static const int64_t JOIN_POSITION = START_POSITION;
static const int64_t RECORDING_POSITION = INITIAL_TERM_OFFSET;
static const int64_t TIME = 0;
static const int64_t CONNECT_TIMEOUT_MS = 5000;
static const int32_t FRAME_LENGTH = 1024;
static const int32_t SESSION_ID = 1;
static const int32_t STREAM_ID = 1001;

/* ============================================================================
 * Utility: create a temp directory for archive data
 * ============================================================================ */

static std::string make_temp_dir(const char *prefix)
{
    char tmpl[260];
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

/* ============================================================================
 * Utility: build an Aeron data frame in a buffer
 * ============================================================================ */

static void build_data_frame(
    uint8_t *buffer,
    int32_t offset,
    int32_t frame_length,
    int32_t term_id,
    int32_t term_offset,
    int32_t session_id,
    int32_t stream_id,
    int64_t reserved_value,
    uint8_t flags,
    int16_t frame_type,
    uint8_t fill_byte)
{
    aeron_data_header_t *hdr = (aeron_data_header_t *)(buffer + offset);
    memset(hdr, 0, sizeof(aeron_data_header_t));
    hdr->frame_header.frame_length = frame_length;
    hdr->frame_header.version = AERON_FRAME_HEADER_VERSION;
    hdr->frame_header.flags = flags;
    hdr->frame_header.type = frame_type;
    hdr->term_offset = term_offset;
    hdr->session_id = session_id;
    hdr->stream_id = stream_id;
    hdr->term_id = term_id;
    hdr->reserved_value = reserved_value;

    /* Fill the payload after the header */
    memset(
        buffer + offset + AERON_DATA_HEADER_LENGTH,
        fill_byte,
        (size_t)(AERON_ALIGN(frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT) - AERON_DATA_HEADER_LENGTH));
}

/* ============================================================================
 * Utility: write a segment file containing pre-recorded frames
 * using the C recording writer
 * ============================================================================ */

static void write_recording_frames(
    const std::string &archive_dir,
    int64_t recording_id,
    int64_t start_position)
{
    aeron_archive_recording_writer_t *writer = nullptr;

    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer,
        recording_id,
        start_position,
        JOIN_POSITION,
        TERM_BUFFER_LENGTH,
        SEGMENT_LENGTH,
        archive_dir.c_str(),
        false,
        false));

    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    uint8_t buffer[TERM_BUFFER_LENGTH];
    memset(buffer, 0, sizeof(buffer));

    /* Frame 0: UNFRAGMENTED DATA */
    build_data_frame(
        buffer, 0, FRAME_LENGTH, INITIAL_TERM_ID, INITIAL_TERM_OFFSET,
        SESSION_ID, STREAM_ID, 0, AERON_DATA_HEADER_UNFRAGMENTED, AERON_HDR_TYPE_DATA, 0);

    int32_t aligned = (int32_t)AERON_ALIGN(FRAME_LENGTH, AERON_LOGBUFFER_FRAME_ALIGNMENT);
    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buffer, (size_t)aligned, false));

    /* Frame 1: BEGIN_FRAG DATA */
    memset(buffer, 0, sizeof(buffer));
    build_data_frame(
        buffer, 0, FRAME_LENGTH, INITIAL_TERM_ID, INITIAL_TERM_OFFSET + FRAME_LENGTH,
        SESSION_ID, STREAM_ID, 1, AERON_DATA_HEADER_BEGIN_FLAG, AERON_HDR_TYPE_DATA, 1);

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buffer, (size_t)aligned, false));

    /* Frame 2: END_FRAG DATA */
    memset(buffer, 0, sizeof(buffer));
    build_data_frame(
        buffer, 0, FRAME_LENGTH, INITIAL_TERM_ID, INITIAL_TERM_OFFSET + FRAME_LENGTH * 2,
        SESSION_ID, STREAM_ID, 2, AERON_DATA_HEADER_END_FLAG, AERON_HDR_TYPE_DATA, 2);

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buffer, (size_t)aligned, false));

    /* Frame 3: UNFRAGMENTED PAD */
    memset(buffer, 0, sizeof(buffer));
    build_data_frame(
        buffer, 0, FRAME_LENGTH, INITIAL_TERM_ID, INITIAL_TERM_OFFSET + FRAME_LENGTH * 3,
        SESSION_ID, STREAM_ID, 3, AERON_DATA_HEADER_UNFRAGMENTED, AERON_HDR_TYPE_PAD, 3);

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buffer, (size_t)aligned, true));

    aeron_archive_recording_writer_close(writer);
}

/* ============================================================================
 * RecordingReader Tests
 * ============================================================================ */

class RecordingReaderTest : public ::testing::Test
{
protected:
    std::string archive_dir_;

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_reader_test");
        write_recording_frames(archive_dir_, RECORDING_ID, START_POSITION);
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir_);
    }
};

struct fragment_data
{
    int32_t offset;
    int32_t length;
    int16_t frame_type;
    uint8_t flags;
    int64_t reserved_value;
};

static void recording_fragment_handler(
    const uint8_t *buffer,
    size_t length,
    int16_t frame_type,
    uint8_t flags,
    int64_t reserved_value,
    void *clientd)
{
    auto *data = static_cast<fragment_data *>(clientd);
    data->length = (int32_t)length;
    data->frame_type = frame_type;
    data->flags = flags;
    data->reserved_value = reserved_value;
}

TEST_F(RecordingReaderTest, verifyRecordingFile)
{
    const int64_t stop_position = START_POSITION + 4 * FRAME_LENGTH;

    aeron_archive_recording_reader_t *reader = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_reader_create(
        &reader,
        RECORDING_ID,
        START_POSITION,
        stop_position,
        AERON_ARCHIVE_NULL_POSITION,
        AERON_ARCHIVE_NULL_LENGTH,
        TERM_BUFFER_LENGTH,
        SEGMENT_LENGTH,
        INITIAL_TERM_ID,
        STREAM_ID,
        archive_dir_.c_str()));

    /* Fragment 0: UNFRAGMENTED DATA */
    fragment_data fd = {};
    int fragments = aeron_archive_recording_reader_poll(reader, recording_fragment_handler, &fd, 1);
    ASSERT_EQ(1, fragments);
    EXPECT_EQ(FRAME_LENGTH - (int32_t)AERON_DATA_HEADER_LENGTH, fd.length);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, fd.frame_type);
    EXPECT_EQ(AERON_DATA_HEADER_UNFRAGMENTED, fd.flags);

    /* Fragment 1: BEGIN_FRAG DATA */
    fd = {};
    fragments = aeron_archive_recording_reader_poll(reader, recording_fragment_handler, &fd, 1);
    ASSERT_EQ(1, fragments);
    EXPECT_EQ(FRAME_LENGTH - (int32_t)AERON_DATA_HEADER_LENGTH, fd.length);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, fd.frame_type);
    EXPECT_EQ(AERON_DATA_HEADER_BEGIN_FLAG, fd.flags);

    /* Fragment 2: END_FRAG DATA */
    fd = {};
    fragments = aeron_archive_recording_reader_poll(reader, recording_fragment_handler, &fd, 1);
    ASSERT_EQ(1, fragments);
    EXPECT_EQ(FRAME_LENGTH - (int32_t)AERON_DATA_HEADER_LENGTH, fd.length);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, fd.frame_type);
    EXPECT_EQ(AERON_DATA_HEADER_END_FLAG, fd.flags);

    /* Fragment 3: UNFRAGMENTED PAD */
    fd = {};
    fragments = aeron_archive_recording_reader_poll(reader, recording_fragment_handler, &fd, 1);
    ASSERT_EQ(1, fragments);
    EXPECT_EQ(FRAME_LENGTH - (int32_t)AERON_DATA_HEADER_LENGTH, fd.length);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, fd.frame_type);
    EXPECT_EQ(AERON_DATA_HEADER_UNFRAGMENTED, fd.flags);

    EXPECT_TRUE(aeron_archive_recording_reader_is_done(reader));
    aeron_archive_recording_reader_close(reader);
}

/* ============================================================================
 * ReplaySession Tests
 *
 * Note: The C replay session interacts with real aeron_exclusive_publication_t
 * which requires a running media driver. These tests validate the state machine
 * by directly driving the session and checking state transitions.
 * We test what we can without mocking the publication -- primarily the INIT
 * state transitions, timeout, and error paths.
 * ============================================================================ */

class ReplaySessionTest : public ::testing::Test
{
protected:
    std::string archive_dir_;
    int64_t stop_position_ = 0;

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_replay_test");
        write_recording_frames(archive_dir_, RECORDING_ID, START_POSITION);
        stop_position_ = START_POSITION + 4 * FRAME_LENGTH;
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir_);
    }

    aeron_archive_replay_session_t *create_replay_session(
        int64_t replay_position,
        int64_t replay_length,
        int64_t correlation_id,
        int64_t current_time_ms,
        aeron_exclusive_publication_t *pub,
        aeron_counter_t *limit_position,
        int64_t stop_pos)
    {
        aeron_archive_replay_session_t *session = nullptr;
        int rc = aeron_archive_replay_session_create(
            &session,
            correlation_id,
            RECORDING_ID,
            replay_position,
            replay_length,
            START_POSITION,
            stop_pos,
            SEGMENT_LENGTH,
            TERM_BUFFER_LENGTH,
            STREAM_ID,
            REPLAY_ID,
            CONNECT_TIMEOUT_MS,
            current_time_ms,
            pub,
            limit_position,
            archive_dir_.c_str(),
            (size_t)TERM_BUFFER_LENGTH);
        EXPECT_EQ(0, rc);
        return session;
    }
};

TEST_F(ReplaySessionTest, shouldCreateReplaySessionWithCorrectState)
{
    aeron_archive_replay_session_t *session = create_replay_session(
        RECORDING_POSITION, FRAME_LENGTH, 1, TIME, nullptr, nullptr, stop_position_);

    ASSERT_NE(nullptr, session);
    EXPECT_EQ(AERON_ARCHIVE_REPLAY_SESSION_STATE_INIT, aeron_archive_replay_session_state(session));
    EXPECT_EQ(RECORDING_ID, aeron_archive_replay_session_recording_id(session));
    EXPECT_EQ(REPLAY_ID, aeron_archive_replay_session_session_id(session));
    EXPECT_FALSE(aeron_archive_replay_session_is_done(session));

    aeron_archive_replay_session_close(session);
}

TEST_F(ReplaySessionTest, shouldStayInInitWhenPublicationIsNull)
{
    aeron_archive_replay_session_t *session = create_replay_session(
        RECORDING_POSITION, FRAME_LENGTH, 1, TIME, nullptr, nullptr, stop_position_);

    ASSERT_NE(nullptr, session);

    /* With a NULL publication, the session should stay in INIT (waiting for connection). */
    /* The C implementation calls aeron_exclusive_publication_is_connected on NULL which
     * would segfault, so for a NULL-pub test we verify the initial state only. */
    EXPECT_EQ(AERON_ARCHIVE_REPLAY_SESSION_STATE_INIT, aeron_archive_replay_session_state(session));

    aeron_archive_replay_session_close(session);
}

TEST_F(ReplaySessionTest, shouldAbortReplaySession)
{
    aeron_archive_replay_session_t *session = create_replay_session(
        RECORDING_POSITION, FRAME_LENGTH, 1, TIME, nullptr, nullptr, stop_position_);

    ASSERT_NE(nullptr, session);
    EXPECT_FALSE(aeron_archive_replay_session_is_done(session));

    aeron_archive_replay_session_abort(session, "test abort");

    /* After abort, the next doWork should transition to DONE, but since we
     * cannot call doWork with a NULL publication, verify the abort flag. */
    EXPECT_TRUE(session->is_aborted);

    aeron_archive_replay_session_close(session);
}

TEST_F(ReplaySessionTest, shouldComputeCorrectSegmentFileBasePosition)
{
    aeron_archive_replay_session_t *session = create_replay_session(
        RECORDING_POSITION, FRAME_LENGTH, 1, TIME, nullptr, nullptr, stop_position_);

    ASSERT_NE(nullptr, session);

    /* The segment base position should be 0 since start_position=1024 is within
     * the first segment (term_length == segment_length == 64K). */
    EXPECT_EQ(0, aeron_archive_replay_session_segment_file_base_position(session));

    aeron_archive_replay_session_close(session);
}

TEST_F(ReplaySessionTest, shouldHaveNoErrorMessageOnCreate)
{
    aeron_archive_replay_session_t *session = create_replay_session(
        RECORDING_POSITION, FRAME_LENGTH, 1, TIME, nullptr, nullptr, stop_position_);

    ASSERT_NE(nullptr, session);
    EXPECT_EQ(nullptr, aeron_archive_replay_session_error_message(session));

    aeron_archive_replay_session_close(session);
}

TEST_F(ReplaySessionTest, shouldCloseCleanlyWithNullSession)
{
    EXPECT_EQ(0, aeron_archive_replay_session_close(nullptr));
}

/* ============================================================================
 * ListRecordingsSession Tests (port of Java ListRecordingsSessionTest)
 * ============================================================================ */

static const int64_t CATALOG_CAPACITY = 1024 * 1024;
static const int32_t SEGMENT_FILE_SIZE = 128 * 1024 * 1024;

struct list_recordings_test_context
{
    int64_t correlation_id;
    int32_t descriptor_count;
    int64_t last_recording_id;
    int32_t unknown_count;
    int64_t unknown_recording_id;
    int64_t expected_recording_ids[16];
    int32_t expected_index;
};

static bool test_send_descriptor(
    int64_t correlation_id,
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    auto *ctx = static_cast<list_recordings_test_context *>(clientd);
    ctx->descriptor_count++;
    ctx->last_recording_id = descriptor->recording_id;

    /* Verify descriptor matches expected recording id if tracking */
    if (ctx->expected_index >= 0 && ctx->expected_index < 16)
    {
        EXPECT_EQ(ctx->expected_recording_ids[ctx->expected_index], descriptor->recording_id);
        ctx->expected_index++;
    }

    return true;
}

static void test_send_unknown(
    int64_t correlation_id,
    int64_t recording_id,
    void *clientd)
{
    auto *ctx = static_cast<list_recordings_test_context *>(clientd);
    ctx->unknown_count++;
    ctx->unknown_recording_id = recording_id;
}

static bool test_send_descriptor_fail(
    int64_t correlation_id,
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    auto *ctx = static_cast<list_recordings_test_context *>(clientd);

    if (ctx->descriptor_count < 3)
    {
        ctx->descriptor_count++;
        return false;  /* simulate send failure / back-pressure */
    }

    ctx->descriptor_count++;
    return true;
}

class ListRecordingsSessionTest : public ::testing::Test
{
protected:
    std::string archive_dir_;
    aeron_archive_catalog_t *catalog_ = nullptr;
    int64_t recording_ids_[3] = {};

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_list_test");

        ASSERT_EQ(0, aeron_archive_catalog_create(
            &catalog_, archive_dir_.c_str(), (size_t)CATALOG_CAPACITY, false, false));

        recording_ids_[0] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1,
            "channelG", "channelG?tag=f", "sourceA");
        ASSERT_GE(recording_ids_[0], 0);

        recording_ids_[1] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 2,
            "channelH", "channelH?tag=f", "sourceV");
        ASSERT_GE(recording_ids_[1], 0);

        recording_ids_[2] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 3,
            "channelK", "channelK?tag=f", "sourceB");
        ASSERT_GE(recording_ids_[2], 0);
    }

    void TearDown() override
    {
        aeron_archive_catalog_close(catalog_);
        remove_temp_dir(archive_dir_);
    }
};

TEST_F(ListRecordingsSessionTest, shouldSendAllDescriptors)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = recording_ids_[0];
    ctx.expected_recording_ids[1] = recording_ids_[1];
    ctx.expected_recording_ids[2] = recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_session_create(
        &session, 1, 0, 3, catalog_, test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(3, ctx.descriptor_count);
    EXPECT_EQ(0, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsSessionTest, shouldSend2Descriptors)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = recording_ids_[1];
    ctx.expected_recording_ids[1] = recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_session_create(
        &session, 1, 1, 2, catalog_, test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(2, ctx.descriptor_count);
    EXPECT_EQ(0, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsSessionTest, shouldSendTwoDescriptorsThenRecordingUnknown)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = recording_ids_[1];
    ctx.expected_recording_ids[1] = recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_session_create(
        &session, 1, 1, 3, catalog_, test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(2, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(3, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsSessionTest, shouldSendRecordingUnknownOnFirst)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_session_create(
        &session, 1, 3, 3, catalog_, test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(0, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(3, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

/* ============================================================================
 * ListRecordingsForUriSession Tests
 * (port of Java ListRecordingsForUriSessionTest)
 * ============================================================================ */

class ListRecordingsForUriSessionTest : public ::testing::Test
{
protected:
    std::string archive_dir_;
    aeron_archive_catalog_t *catalog_ = nullptr;
    int64_t matching_recording_ids_[3] = {};

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_listuri_test");

        ASSERT_EQ(0, aeron_archive_catalog_create(
            &catalog_, archive_dir_.c_str(), (size_t)CATALOG_CAPACITY, false, false));

        /* recording 0: localhost, stream 1 -- matches */
        matching_recording_ids_[0] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1,
            "localhost", "localhost?tag=f", "sourceA");
        ASSERT_GE(matching_recording_ids_[0], 0);

        /* recording 1: channelA, stream 1 -- does NOT match */
        int64_t non_match1 = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 1,
            "channelA", "channel?tag=f", "sourceV");
        ASSERT_GE(non_match1, 0);

        /* recording 2: localhost, stream 1 -- matches */
        matching_recording_ids_[1] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1,
            "localhost", "localhost?tag=f", "sourceB");
        ASSERT_GE(matching_recording_ids_[1], 0);

        /* recording 3: channelB, stream 1 -- does NOT match */
        int64_t non_match2 = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1,
            "channelB", "channelB?tag=f", "sourceB");
        ASSERT_GE(non_match2, 0);

        /* recording 4: localhost, stream 1 -- matches */
        matching_recording_ids_[2] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 1,
            "localhost", "localhost?tag=f", "sourceB");
        ASSERT_GE(matching_recording_ids_[2], 0);
    }

    void TearDown() override
    {
        aeron_archive_catalog_close(catalog_);
        remove_temp_dir(archive_dir_);
    }
};

TEST_F(ListRecordingsForUriSessionTest, shouldSendAllDescriptors)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = matching_recording_ids_[0];
    ctx.expected_recording_ids[1] = matching_recording_ids_[1];
    ctx.expected_recording_ids[2] = matching_recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_for_uri_session_create(
        &session, 1, 0, 3, "localhost", 1, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(3, ctx.descriptor_count);
    EXPECT_EQ(0, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsForUriSessionTest, shouldSend2Descriptors)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = matching_recording_ids_[1];
    ctx.expected_recording_ids[1] = matching_recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_for_uri_session_create(
        &session, 1, 1, 2, "localhost", 1, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(2, ctx.descriptor_count);
    EXPECT_EQ(0, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsForUriSessionTest, shouldSend2DescriptorsAndRecordingUnknown)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;
    ctx.expected_recording_ids[0] = matching_recording_ids_[1];
    ctx.expected_recording_ids[1] = matching_recording_ids_[2];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_for_uri_session_create(
        &session, 1, 1, 5, "localhost", 1, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(2, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(5, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsForUriSessionTest, shouldSendRecordingUnknown)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_for_uri_session_create(
        &session, 1, 1, 3, "notChannel", 1, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(0, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingsForUriSessionTest, shouldSendUnknownOnFirst)
{
    list_recordings_test_context ctx = {};
    ctx.correlation_id = 1;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recordings_for_uri_session_create(
        &session, 1, 5, 3, "localhost", 1, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(0, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(5, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

/* ============================================================================
 * ListRecordingByIdSession Tests
 * (port of Java ListRecordingByIdSessionTest)
 * ============================================================================ */

class ListRecordingByIdSessionTest : public ::testing::Test
{
protected:
    std::string archive_dir_;
    aeron_archive_catalog_t *catalog_ = nullptr;
    int64_t recording_ids_[3] = {};

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_listbyid_test");

        ASSERT_EQ(0, aeron_archive_catalog_create(
            &catalog_, archive_dir_.c_str(), (size_t)CATALOG_CAPACITY, false, false));

        recording_ids_[0] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1,
            "channelG", "channelG?tag=f", "sourceA");
        ASSERT_GE(recording_ids_[0], 0);

        recording_ids_[1] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 2,
            "channelH", "channelH?tag=f", "sourceV");
        ASSERT_GE(recording_ids_[1], 0);

        recording_ids_[2] = aeron_archive_catalog_add_recording(
            catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 8, 3,
            "channelK", "channelK?tag=f", "sourceB");
        ASSERT_GE(recording_ids_[2], 0);
    }

    void TearDown() override
    {
        aeron_archive_catalog_close(catalog_);
        remove_temp_dir(archive_dir_);
    }
};

TEST_F(ListRecordingByIdSessionTest, shouldSendDescriptor)
{
    const int64_t correlation_id = -53465834;

    list_recordings_test_context ctx = {};
    ctx.correlation_id = correlation_id;
    ctx.expected_recording_ids[0] = recording_ids_[1];

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, correlation_id, recording_ids_[1], catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(1, ctx.descriptor_count);
    EXPECT_EQ(0, ctx.unknown_count);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingByIdSessionTest, shouldSendRecordingUnknownOnFirst)
{
    const int64_t correlation_id = 42;
    const int64_t unknown_recording_id = 17777;

    list_recordings_test_context ctx = {};
    ctx.correlation_id = correlation_id;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, correlation_id, unknown_recording_id, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(0, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(unknown_recording_id, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingByIdSessionTest, shouldSendRecordingUnknownOnInvalidRecordingId)
{
    const int64_t correlation_id = 42;
    const int64_t invalidated_recording_id = recording_ids_[1];

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog_, invalidated_recording_id));

    list_recordings_test_context ctx = {};
    ctx.correlation_id = correlation_id;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, correlation_id, invalidated_recording_id, catalog_,
        test_send_descriptor, test_send_unknown, &ctx));

    aeron_archive_list_recordings_session_do_work(session);

    EXPECT_EQ(0, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(invalidated_recording_id, ctx.unknown_recording_id);
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingByIdSessionTest, shouldRetrySendingDescriptorUntilSuccess)
{
    const int64_t correlation_id = 19;
    const int64_t recording_id = recording_ids_[2];

    list_recordings_test_context ctx = {};
    ctx.correlation_id = correlation_id;

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, correlation_id, recording_id, catalog_,
        test_send_descriptor_fail, test_send_unknown, &ctx));

    /* The send_descriptor_fail callback returns false for the first 3 calls,
     * then true on the 4th. Each doWork call should do 1 unit of work until
     * the descriptor is successfully sent. */
    while (!aeron_archive_list_recordings_session_is_done(session))
    {
        int work = aeron_archive_list_recordings_session_do_work(session);
        EXPECT_EQ(1, work);
    }

    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));
    EXPECT_EQ(4, ctx.descriptor_count);

    /* After done, doWork returns 0 */
    EXPECT_EQ(0, aeron_archive_list_recordings_session_do_work(session));

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingByIdSessionTest, shouldSendRecordingUnknownIfInvalidatedBetweenRetries)
{
    const int64_t correlation_id = 19;
    const int64_t recording_id = recording_ids_[2];

    /* Use a callback that always fails (returns false) */
    list_recordings_test_context ctx = {};
    ctx.correlation_id = correlation_id;

    /* We need a special callback that always returns false */
    auto always_fail = [](int64_t cid,
        const aeron_archive_catalog_recording_descriptor_t *desc,
        void *clientd) -> bool
    {
        auto *c = static_cast<list_recordings_test_context *>(clientd);
        c->descriptor_count++;
        return false;
    };

    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, correlation_id, recording_id, catalog_,
        always_fail, test_send_unknown, &ctx));

    /* First doWork: tries to send, fails */
    EXPECT_EQ(1, aeron_archive_list_recordings_session_do_work(session));
    EXPECT_FALSE(aeron_archive_list_recordings_session_is_done(session));

    /* Invalidate the recording between retries */
    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog_, recording_id));

    /* Second doWork: recording is now invalid, should send unknown */
    EXPECT_EQ(1, aeron_archive_list_recordings_session_do_work(session));
    EXPECT_TRUE(aeron_archive_list_recordings_session_is_done(session));

    EXPECT_EQ(1, ctx.descriptor_count);
    EXPECT_EQ(1, ctx.unknown_count);
    EXPECT_EQ(recording_id, ctx.unknown_recording_id);

    aeron_archive_list_recordings_session_close(session);
}

TEST_F(ListRecordingByIdSessionTest, shouldCloseSession)
{
    aeron_archive_list_recordings_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_list_recording_by_id_session_create(
        &session, 1, 111, catalog_,
        test_send_descriptor, test_send_unknown, nullptr));

    /* Close should not crash and should succeed */
    EXPECT_EQ(0, aeron_archive_list_recordings_session_close(session));
}

TEST_F(ListRecordingByIdSessionTest, shouldCloseNullSession)
{
    EXPECT_EQ(0, aeron_archive_list_recordings_session_close(nullptr));
}

/* ============================================================================
 * Additional catalog-level test: verify recording count and IDs
 * ============================================================================ */

class CatalogTest : public ::testing::Test
{
protected:
    std::string archive_dir_;
    aeron_archive_catalog_t *catalog_ = nullptr;

    void SetUp() override
    {
        archive_dir_ = make_temp_dir("archive_catalog_test");
        ASSERT_EQ(0, aeron_archive_catalog_create(
            &catalog_, archive_dir_.c_str(), (size_t)CATALOG_CAPACITY, false, false));
    }

    void TearDown() override
    {
        aeron_archive_catalog_close(catalog_);
        remove_temp_dir(archive_dir_);
    }
};

TEST_F(CatalogTest, shouldAddAndFindRecordings)
{
    int64_t id0 = aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 6, 1,
        "channelA", "channelA?tag=f", "sourceA");
    ASSERT_GE(id0, 0);

    int64_t id1 = aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 7, 2,
        "channelB", "channelB?tag=f", "sourceB");
    ASSERT_GE(id1, 0);

    EXPECT_NE(id0, id1);

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog_, id0, &desc));
    EXPECT_EQ(id0, desc.recording_id);
    EXPECT_EQ(6, desc.session_id);
    EXPECT_EQ(1, desc.stream_id);

    desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog_, id1, &desc));
    EXPECT_EQ(id1, desc.recording_id);
    EXPECT_EQ(7, desc.session_id);
    EXPECT_EQ(2, desc.stream_id);

    /* Non-existent recording */
    EXPECT_NE(0, aeron_archive_catalog_find_recording(catalog_, 999, &desc));
}

TEST_F(CatalogTest, shouldInvalidateRecording)
{
    int64_t id = aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 1, 1,
        "ch", "ch", "src");
    ASSERT_GE(id, 0);

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog_, id, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog_, id));

    /* After invalidation, find should fail */
    EXPECT_NE(0, aeron_archive_catalog_find_recording(catalog_, id, &desc));
}

TEST_F(CatalogTest, shouldIterateAllRecordings)
{
    aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 1, 1, "a", "a", "s1");
    aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 2, 2, "b", "b", "s2");
    aeron_archive_catalog_add_recording(
        catalog_, 0, 0, 0, 0, 0, SEGMENT_FILE_SIZE, 4096, 1024, 3, 3, "c", "c", "s3");

    int32_t count = 0;
    auto counter = [](const aeron_archive_catalog_recording_descriptor_t *desc, void *clientd)
    {
        int32_t *cnt = static_cast<int32_t *>(clientd);
        (*cnt)++;
    };

    int32_t iterated = aeron_archive_catalog_for_each(catalog_, counter, &count);
    EXPECT_EQ(3, count);
    EXPECT_EQ(3, iterated);
}
