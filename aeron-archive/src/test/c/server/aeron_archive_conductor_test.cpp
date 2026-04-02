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

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>

#ifdef _MSC_VER
#include <direct.h>
#include <io.h>
#define MKDIR(dir) _mkdir(dir)
static char *mkdtemp(char *tmpl)
{
    if (_mktemp_s(tmpl, strlen(tmpl) + 1) != 0) return NULL;
    if (_mkdir(tmpl) != 0) return NULL;
    return tmpl;
}
#else
#include <sys/stat.h>
#define MKDIR(dir) mkdir((dir), 0755)
#endif

extern "C"
{
#include <inttypes.h>
#include "server/aeron_archive_conductor.h"
#include "server/aeron_archive_delete_segments_session.h"
}

/*
 * ==========================================================================
 * ArchiveConductor Tests
 *
 * Port of Java ArchiveConductorTest. The Java test verifies strippedChannelBuilder
 * which has no C equivalent. Instead we test the C conductor lifecycle:
 * create, initial state, session counts, and close.
 * ==========================================================================
 */

class ArchiveConductorTest : public ::testing::Test
{
protected:
    aeron_archive_conductor_context_t ctx = {};
    aeron_archive_conductor_t *conductor = nullptr;

    void SetUp() override
    {
        memset(&ctx, 0, sizeof(ctx));
        ctx.archive_dir = "/tmp";
        ctx.control_channel = "aeron:udp?endpoint=localhost:8010";
        ctx.control_stream_id = 10;
        ctx.segment_file_length = 1024 * 1024;
        ctx.max_concurrent_recordings = 16;
        ctx.max_concurrent_replays = 16;
    }

    void TearDown() override
    {
        if (nullptr != conductor)
        {
            aeron_archive_conductor_close(conductor);
            conductor = nullptr;
        }
    }
};

TEST_F(ArchiveConductorTest, shouldCreateAndClose)
{
    ASSERT_EQ(0, aeron_archive_conductor_create(&conductor, &ctx));
    ASSERT_NE(nullptr, conductor);
    EXPECT_FALSE(aeron_archive_conductor_is_closed(conductor));

    ASSERT_EQ(0, aeron_archive_conductor_close(conductor));
    conductor = nullptr;
}

TEST_F(ArchiveConductorTest, shouldReportClosedAfterClose)
{
    ASSERT_EQ(0, aeron_archive_conductor_create(&conductor, &ctx));
    ASSERT_FALSE(aeron_archive_conductor_is_closed(conductor));

    ASSERT_EQ(0, aeron_archive_conductor_close(conductor));
    /* After close the pointer is freed, so is_closed(NULL) should be true */
    EXPECT_TRUE(aeron_archive_conductor_is_closed(nullptr));
    conductor = nullptr;
}

TEST_F(ArchiveConductorTest, shouldHaveZeroSessionCountsInitially)
{
    ASSERT_EQ(0, aeron_archive_conductor_create(&conductor, &ctx));
    EXPECT_EQ(0, aeron_archive_conductor_recording_session_count(conductor));
    EXPECT_EQ(0, aeron_archive_conductor_replay_session_count(conductor));
}

TEST_F(ArchiveConductorTest, shouldReturnErrorOnNullArgs)
{
    EXPECT_EQ(-1, aeron_archive_conductor_create(nullptr, &ctx));
    EXPECT_EQ(-1, aeron_archive_conductor_create(&conductor, nullptr));
}

TEST_F(ArchiveConductorTest, shouldCloseNullSafely)
{
    EXPECT_EQ(0, aeron_archive_conductor_close(nullptr));
}

/*
 * ==========================================================================
 * DeleteSegmentsSession Tests
 *
 * Port of Java DeleteSegmentsSessionTest.
 * ==========================================================================
 */

/* Segment file name format: "<recordingId>-<position>.rec" */
static std::string segment_file_name(int64_t recording_id, int64_t position)
{
    char buf[256];
    snprintf(buf, sizeof(buf), "%" PRId64 "-%" PRId64 ".rec", recording_id, position);
    return std::string(buf);
}

/* Tracking structures for mock callbacks */
struct SignalRecord
{
    int64_t correlation_id;
    int64_t recording_id;
    int64_t subscription_id;
    int64_t position;
};

struct RemoveRecord
{
    void *session;
};

static std::vector<SignalRecord> g_signal_records;
static std::vector<RemoveRecord> g_remove_records;

static void mock_send_signal(
    int64_t correlation_id,
    int64_t recording_id,
    int64_t subscription_id,
    int64_t position,
    void *clientd)
{
    g_signal_records.push_back({correlation_id, recording_id, subscription_id, position});
}

static void mock_send_error(
    int64_t correlation_id,
    int32_t error_code,
    const char *error_message,
    void *clientd)
{
    /* no-op for these tests */
}

static void mock_remove_session(void *session, void *clientd)
{
    g_remove_records.push_back({session});
}

static void mock_error_handler(const char *error_message, void *clientd)
{
    /* no-op for these tests */
}

class DeleteSegmentsSessionTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        g_signal_records.clear();
        g_remove_records.clear();
    }
};

TEST_F(DeleteSegmentsSessionTest, shouldComputeMaxDeletePosition)
{
    const int64_t recording_id = 18;
    const int64_t correlation_id = 42;

    aeron_archive_delete_segments_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_delete_segments_session_create(
        &session,
        recording_id,
        correlation_id,
        mock_send_signal,
        mock_send_error,
        mock_remove_session,
        mock_error_handler,
        nullptr,
        nullptr));
    ASSERT_NE(nullptr, session);

    /* Add files matching the Java test:
     * segmentFileName(18, 1048576)           -> "18-1048576.rec"
     * segmentFileName(18, 0) + ".del"        -> "18-0.rec.del"
     * segmentFileName(18, 42)                -> "18-42.rec"
     * segmentFileName(18, -9999999999)       -> "18--9999999999.rec"
     * segmentFileName(18, 12345000000) + ".del" -> "18-12345000000.rec.del"
     * segmentFileName(18, 56678)             -> "18-56678.rec"
     */
    std::string f1 = segment_file_name(recording_id, 1024 * 1024);
    std::string f2 = segment_file_name(recording_id, 0) + ".del";
    std::string f3 = segment_file_name(recording_id, correlation_id);
    std::string f4 = segment_file_name(recording_id, -9999999999LL);
    std::string f5 = segment_file_name(recording_id, 12345000000LL) + ".del";
    std::string f6 = segment_file_name(recording_id, 56678);

    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f1.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f2.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f3.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f4.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f5.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, f6.c_str()));

    aeron_archive_delete_segments_session_finalise(session);

    EXPECT_EQ(12345000000LL, aeron_archive_delete_segments_session_max_delete_position(session));

    aeron_archive_delete_segments_session_close(session);
}

TEST_F(DeleteSegmentsSessionTest, shouldRemoveSessionUponClose)
{
    const int64_t recording_id = 0;
    const int64_t correlation_id = -4732948723LL;

    aeron_archive_delete_segments_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_delete_segments_session_create(
        &session,
        recording_id,
        correlation_id,
        mock_send_signal,
        mock_send_error,
        mock_remove_session,
        mock_error_handler,
        nullptr,
        nullptr));
    ASSERT_NE(nullptr, session);

    /* No files added - empty queue */
    aeron_archive_delete_segments_session_finalise(session);

    void *session_ptr = session;
    aeron_archive_delete_segments_session_close(session);

    /* Verify remove_session was called with the session pointer */
    ASSERT_EQ(1u, g_remove_records.size());
    EXPECT_EQ(session_ptr, g_remove_records[0].session);

    /* Verify send_signal was called with correct args:
     * correlation_id, recording_id, AERON_ARCHIVE_NULL_VALUE, AERON_ARCHIVE_NULL_VALUE */
    ASSERT_EQ(1u, g_signal_records.size());
    EXPECT_EQ(correlation_id, g_signal_records[0].correlation_id);
    EXPECT_EQ(recording_id, g_signal_records[0].recording_id);
    EXPECT_EQ(AERON_ARCHIVE_NULL_VALUE, g_signal_records[0].subscription_id);
    EXPECT_EQ(AERON_ARCHIVE_NULL_VALUE, g_signal_records[0].position);
}

TEST_F(DeleteSegmentsSessionTest, shouldDeleteSegmentFiles)
{
    const int64_t recording_id = 7;
    const int64_t correlation_id = 100;

    /* Create a temp directory for segment files */
    char temp_dir[] = "/tmp/aeron_del_seg_test_XXXXXX";
    ASSERT_NE(nullptr, mkdtemp(temp_dir));

    std::string dir(temp_dir);

    /* Create segment files on disk */
    std::string file1 = dir + "/" + segment_file_name(recording_id, 0);
    std::string file2 = dir + "/" + segment_file_name(recording_id, 1048576);
    std::string file3 = dir + "/" + segment_file_name(recording_id, 2097152);

    for (const auto &path : {file1, file2, file3})
    {
        std::ofstream ofs(path);
        ASSERT_TRUE(ofs.is_open()) << "Failed to create: " << path;
        ofs << "segment data";
        ofs.close();
    }

    /* Verify files exist */
    for (const auto &path : {file1, file2, file3})
    {
        std::ifstream ifs(path);
        ASSERT_TRUE(ifs.good()) << "File should exist: " << path;
    }

    aeron_archive_delete_segments_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_delete_segments_session_create(
        &session,
        recording_id,
        correlation_id,
        mock_send_signal,
        mock_send_error,
        mock_remove_session,
        mock_error_handler,
        nullptr,
        nullptr));

    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, file1.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, file2.c_str()));
    ASSERT_EQ(0, aeron_archive_delete_segments_session_add_file(session, file3.c_str()));

    aeron_archive_delete_segments_session_finalise(session);

    EXPECT_EQ(2097152LL, aeron_archive_delete_segments_session_max_delete_position(session));

    /* Run do_work until all files are processed */
    while (!aeron_archive_delete_segments_session_is_done(session))
    {
        ASSERT_GT(aeron_archive_delete_segments_session_do_work(session), 0);
    }

    /* Verify files are deleted */
    for (const auto &path : {file1, file2, file3})
    {
        std::ifstream ifs(path);
        EXPECT_FALSE(ifs.good()) << "File should have been deleted: " << path;
    }

    aeron_archive_delete_segments_session_close(session);

    /* Clean up temp directory */
    std::string rm_cmd = "rm -rf " + dir;
    if (std::system(rm_cmd.c_str())) {}
}

TEST_F(DeleteSegmentsSessionTest, shouldReportDoneWhenEmpty)
{
    const int64_t recording_id = 99;
    const int64_t correlation_id = 1;

    aeron_archive_delete_segments_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_delete_segments_session_create(
        &session,
        recording_id,
        correlation_id,
        mock_send_signal,
        mock_send_error,
        mock_remove_session,
        mock_error_handler,
        nullptr,
        nullptr));

    aeron_archive_delete_segments_session_finalise(session);

    EXPECT_TRUE(aeron_archive_delete_segments_session_is_done(session));
    EXPECT_EQ(0, aeron_archive_delete_segments_session_do_work(session));

    aeron_archive_delete_segments_session_close(session);
}

TEST_F(DeleteSegmentsSessionTest, shouldReturnSessionId)
{
    const int64_t recording_id = 42;
    const int64_t correlation_id = 7;

    aeron_archive_delete_segments_session_t *session = nullptr;
    ASSERT_EQ(0, aeron_archive_delete_segments_session_create(
        &session,
        recording_id,
        correlation_id,
        mock_send_signal,
        mock_send_error,
        mock_remove_session,
        mock_error_handler,
        nullptr,
        nullptr));

    EXPECT_EQ(recording_id, aeron_archive_delete_segments_session_session_id(session));

    aeron_archive_delete_segments_session_close(session);
}
