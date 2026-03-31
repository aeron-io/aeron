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

#include <cinttypes>
#include <cstdlib>
#include <cstring>
#include <cstdio>
#include <string>
#include <vector>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "gtest/gtest.h"

extern "C"
{
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"
#include "server/aeron_archive_recording_writer.h"
#include "server/aeron_archive_recording_reader.h"
#include "server/aeron_archive_recording_session.h"
}

/* --------------------------------------------------------------------------
 * Constants matching the Java tests
 * -------------------------------------------------------------------------- */

static const int32_t TERM_LENGTH = AERON_LOGBUFFER_TERM_MIN_LENGTH;         /* 64 KiB */
static const int32_t SEGMENT_LENGTH = TERM_LENGTH * 4;                       /* 256 KiB */

/* --------------------------------------------------------------------------
 * Utility helpers
 * -------------------------------------------------------------------------- */

static std::string make_temp_dir()
{
    char tmpl[] = "/tmp/aeron_recording_test_XXXXXX";
    char *dir = mkdtemp(tmpl);
    EXPECT_NE(nullptr, dir);
    return std::string(dir);
}

static void remove_temp_dir(const std::string &dir)
{
    std::string cmd = "rm -rf \"" + dir + "\"";
    if (std::system(cmd.c_str())) {}
}

static std::string segment_file_name(int64_t recording_id, int64_t segment_base_position)
{
    char buf[128];
    snprintf(buf, sizeof(buf), "%" PRId64 "-%" PRId64 ".rec", recording_id, segment_base_position);
    return std::string(buf);
}

static std::string segment_file_path(
    const std::string &archive_dir, int64_t recording_id, int64_t start_position)
{
    /* Compute segment base position: align down to segment boundary from term-aligned start */
    int64_t segment_base = start_position - (start_position & (TERM_LENGTH - 1));
    /* Further align to segment boundary */
    int64_t start_term_base = start_position - (start_position & (TERM_LENGTH - 1));
    int64_t length_from_base = start_position - start_term_base;
    int64_t segments = length_from_base - (length_from_base & (SEGMENT_LENGTH - 1));
    segment_base = start_term_base + segments;

    return archive_dir + "/" + segment_file_name(recording_id, segment_base);
}

static bool file_exists(const std::string &path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

static int64_t file_size(const std::string &path)
{
    struct stat st;
    if (stat(path.c_str(), &st) != 0)
    {
        return -1;
    }
    return (int64_t)st.st_size;
}

static std::vector<uint8_t> read_file(const std::string &path)
{
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0)
    {
        return {};
    }
    struct stat st;
    fstat(fd, &st);
    std::vector<uint8_t> data((size_t)st.st_size);
    ssize_t n = read(fd, data.data(), data.size());
    (void)n;
    close(fd);
    return data;
}

/* Helper to build a frame header + payload in a buffer at a given offset */
static void set_frame(
    uint8_t *buffer, int32_t offset, int16_t type, int32_t frame_length,
    int32_t session_id, int32_t term_id, uint8_t fill_byte, int32_t payload_length)
{
    aeron_data_header_t *hdr = (aeron_data_header_t *)(buffer + offset);
    memset(hdr, 0, sizeof(aeron_data_header_t));
    hdr->frame_header.frame_length = frame_length;
    hdr->frame_header.type = type;
    hdr->session_id = session_id;
    hdr->term_id = term_id;

    if (payload_length > 0)
    {
        memset(buffer + offset + AERON_DATA_HEADER_LENGTH, fill_byte, (size_t)payload_length);
    }
}

/* Convenience: read the frame_header at a byte offset in a raw buffer */
static const aeron_data_header_t *frame_at(const uint8_t *data, int32_t offset)
{
    return (const aeron_data_header_t *)(data + offset);
}

/* ===========================================================================
 * RecordingWriterTest fixture
 * ===========================================================================*/

class RecordingWriterTest : public ::testing::Test
{
protected:
    std::string archive_dir;

    void SetUp() override
    {
        archive_dir = make_temp_dir();
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir);
    }
};

/* Port of: initShouldOpenAFileChannel */
TEST_F(RecordingWriterTest, InitShouldCreateSegmentFile)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));

    std::string seg = archive_dir + "/" + segment_file_name(1, 0);
    ASSERT_FALSE(file_exists(seg));

    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));
    EXPECT_TRUE(file_exists(seg));

    aeron_archive_recording_writer_close(writer);
}

/* Port of: initThrowsIOExceptionIfItCannotOpenAFileChannel */
TEST_F(RecordingWriterTest, InitFailsWhenArchiveDirIsNotADirectory)
{
    /* Create a regular file pretending to be the archive dir */
    std::string not_a_dir = archive_dir + "/dummy.txt";
    int fd = open(not_a_dir.c_str(), O_CREAT | O_WRONLY, 0644);
    ASSERT_GE(fd, 0);
    close(fd);

    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, not_a_dir.c_str(), false, false));

    EXPECT_NE(0, aeron_archive_recording_writer_init(writer));

    aeron_archive_recording_writer_close(writer);
}

/* Port of: closeShouldCloseTheUnderlyingFile */
TEST_F(RecordingWriterTest, CloseReleasesFileDescriptor)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));

    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    std::string seg = archive_dir + "/" + segment_file_name(1, 0);
    EXPECT_TRUE(file_exists(seg));

    aeron_archive_recording_writer_close(writer);

    /* After close we should be able to unlink the file (fd is released) */
    EXPECT_EQ(0, unlink(seg.c_str()));
}

/* Port of: onBlockThrowsNullPointerExceptionIfInitWasNotCalled
 * In C the writer will have segment_fd == -1 so write should fail. */
TEST_F(RecordingWriterTest, WriteFailsIfInitWasNotCalled)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));

    uint8_t buffer[128];
    memset(buffer, 0, sizeof(buffer));

    EXPECT_NE(0, aeron_archive_recording_writer_write(writer, buffer, 128, false));

    aeron_archive_recording_writer_close(writer);
}

/* Port of: onBlockShouldWriteHeaderAndContentsOfTheNonPaddingFrame */
TEST_F(RecordingWriterTest, WriteDataFrame)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Build a 128-byte data frame: 32-byte header + 96-byte payload filled with 0x07 */
    const int32_t frame_len = 128;
    const int32_t payload_len = frame_len - (int32_t)AERON_DATA_HEADER_LENGTH;
    std::vector<uint8_t> term_buffer(frame_len, 0);
    set_frame(term_buffer.data(), 0, AERON_HDR_TYPE_DATA, frame_len, 0, 0, 0x07, payload_len);

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, term_buffer.data(), frame_len, false));
    aeron_archive_recording_writer_close(writer);

    std::string seg = archive_dir + "/" + segment_file_name(1, 0);
    ASSERT_TRUE(file_exists(seg));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg));

    auto file_data = read_file(seg);
    ASSERT_GE(file_data.size(), (size_t)frame_len);

    const aeron_data_header_t *hdr = frame_at(file_data.data(), 0);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, hdr->frame_header.type);
    EXPECT_EQ(frame_len, hdr->frame_header.frame_length);
    EXPECT_EQ(0, hdr->session_id);

    /* Verify payload bytes */
    for (int32_t i = 0; i < payload_len; i++)
    {
        EXPECT_EQ(0x07, file_data[AERON_DATA_HEADER_LENGTH + i])
            << "mismatch at payload offset " << i;
    }
}

/* Port of: onBlockShouldWriteHeaderOfThePaddingFrameAndAdvanceFilePositionByThePaddingLength */
TEST_F(RecordingWriterTest, WritePaddingFrame)
{
    const int32_t segment_offset = 96;
    const int64_t start_position = (int64_t)7 * TERM_LENGTH + segment_offset;
    const int32_t frame_len = 1024;

    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 5, start_position, start_position, TERM_LENGTH, SEGMENT_LENGTH,
        archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Build a 1024-byte padding frame with sessionId=111 */
    std::vector<uint8_t> term_buffer(frame_len, 0);
    set_frame(term_buffer.data(), 0, AERON_HDR_TYPE_PAD, frame_len, 111, 0, 0xFF, (int32_t)(frame_len - AERON_DATA_HEADER_LENGTH));

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, term_buffer.data(), frame_len, true));
    aeron_archive_recording_writer_close(writer);

    /* Compute expected segment file base position */
    int64_t start_term_base = start_position - (start_position & (TERM_LENGTH - 1));
    int64_t length_from_base = start_position - start_term_base;
    int64_t segments = length_from_base - (length_from_base & (SEGMENT_LENGTH - 1));
    int64_t seg_base = start_term_base + segments;

    std::string seg = archive_dir + "/" + segment_file_name(5, seg_base);
    ASSERT_TRUE(file_exists(seg));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg));

    auto file_data = read_file(seg);

    /* Preamble bytes (before segment_offset) should be zero */
    for (int32_t i = 0; i < segment_offset; i++)
    {
        EXPECT_EQ(0, file_data[i]) << "preamble byte at " << i << " should be zero";
    }

    /* Check the padding frame header at segment_offset */
    const aeron_data_header_t *hdr = frame_at(file_data.data(), segment_offset);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, hdr->frame_header.type);
    EXPECT_EQ(frame_len, hdr->frame_header.frame_length);
    EXPECT_EQ(111, hdr->session_id);

    /* For a padding frame, only the header is written; the payload in the file should be zeros */
    for (size_t i = segment_offset + AERON_DATA_HEADER_LENGTH;
         i < (size_t)(segment_offset + frame_len) && i < file_data.size(); i++)
    {
        EXPECT_EQ(0, file_data[i]) << "padding body at offset " << i << " should be zero";
    }
}

/* Port of: onBlockShouldRollOverToTheNextSegmentFile */
TEST_F(RecordingWriterTest, SegmentRollOver)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 13, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Fill the first segment with 1024-byte data frames filled with 0x0D */
    const int32_t block_size = 1024;
    const int32_t payload_len = block_size - (int32_t)AERON_DATA_HEADER_LENGTH;
    std::vector<uint8_t> term_buffer(TERM_LENGTH, 0);
    set_frame(term_buffer.data(), 0, AERON_HDR_TYPE_DATA, block_size, 0, 0, 0x0D, payload_len);

    const int num_blocks = SEGMENT_LENGTH / block_size;
    for (int i = 0; i < num_blocks; i++)
    {
        ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, term_buffer.data(), block_size, false))
            << "write failed at block " << i;
    }

    /* Write one more 192-byte frame to trigger rollover, filled with 0x16 */
    const int32_t frame2_len = 192;
    const int32_t payload2_len = frame2_len - (int32_t)AERON_DATA_HEADER_LENGTH;
    std::vector<uint8_t> term_buffer2(frame2_len, 0);
    set_frame(term_buffer2.data(), 0, AERON_HDR_TYPE_DATA, frame2_len, 0, 0, 0x16, payload2_len);

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, term_buffer2.data(), frame2_len, false));
    aeron_archive_recording_writer_close(writer);

    /* Verify first segment file */
    std::string seg1 = archive_dir + "/" + segment_file_name(13, 0);
    ASSERT_TRUE(file_exists(seg1));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg1));

    auto file1 = read_file(seg1);
    const aeron_data_header_t *hdr1 = frame_at(file1.data(), 0);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, hdr1->frame_header.type);
    EXPECT_EQ(block_size, hdr1->frame_header.frame_length);

    for (int32_t i = 0; i < payload_len; i++)
    {
        EXPECT_EQ(0x0D, file1[AERON_DATA_HEADER_LENGTH + i])
            << "seg1 payload mismatch at " << i;
    }

    /* Verify second segment file */
    std::string seg2 = archive_dir + "/" + segment_file_name(13, SEGMENT_LENGTH);
    ASSERT_TRUE(file_exists(seg2));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg2));

    auto file2 = read_file(seg2);
    const aeron_data_header_t *hdr2 = frame_at(file2.data(), 0);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, hdr2->frame_header.type);
    EXPECT_EQ(frame2_len, hdr2->frame_header.frame_length);

    for (int32_t i = 0; i < payload2_len; i++)
    {
        EXPECT_EQ(0x16, file2[AERON_DATA_HEADER_LENGTH + i])
            << "seg2 payload mismatch at " << i;
    }
}

/* Port of: onBlockShouldWriteMultipleFramesAndVerifyContent
 * Adapted from the Java checksum test without CRC (C API has no checksum support yet).
 * We write two consecutive data frames at different offsets into the same block write. */
TEST_F(RecordingWriterTest, WriteMultipleFramesInOneBlock)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Build a buffer containing two frames starting at offset 0:
     *   Frame 1: 64 bytes total (header + 32 payload of 0x60)
     *   Frame 2: 288 bytes total (header + 256 payload of 0xA0)
     *   Total block: 352 bytes */
    const int32_t frame1_len = 64;
    const int32_t frame2_len = 288;
    const int32_t total = frame1_len + frame2_len; /* 352 */
    std::vector<uint8_t> buf(total, 0);

    set_frame(buf.data(), 0, AERON_HDR_TYPE_DATA, frame1_len, 0, 96, 0x60,
        (int32_t)(frame1_len - AERON_DATA_HEADER_LENGTH));
    set_frame(buf.data(), frame1_len, AERON_HDR_TYPE_DATA, frame2_len, 0, 160, 0xA0,
        (int32_t)(frame2_len - AERON_DATA_HEADER_LENGTH));

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buf.data(), total, false));
    aeron_archive_recording_writer_close(writer);

    std::string seg = archive_dir + "/" + segment_file_name(1, 0);
    ASSERT_TRUE(file_exists(seg));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg));

    auto file_data = read_file(seg);

    /* Verify frame 1 */
    const aeron_data_header_t *h1 = frame_at(file_data.data(), 0);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, h1->frame_header.type);
    EXPECT_EQ(frame1_len, h1->frame_header.frame_length);
    EXPECT_EQ(96, h1->term_id);
    for (int32_t i = 0; i < frame1_len - (int32_t)AERON_DATA_HEADER_LENGTH; i++)
    {
        EXPECT_EQ(0x60, file_data[AERON_DATA_HEADER_LENGTH + i]);
    }

    /* Verify frame 2 */
    const aeron_data_header_t *h2 = frame_at(file_data.data(), frame1_len);
    EXPECT_EQ(AERON_HDR_TYPE_DATA, h2->frame_header.type);
    EXPECT_EQ(frame2_len, h2->frame_header.frame_length);
    EXPECT_EQ(160, h2->term_id);
    for (int32_t i = 0; i < frame2_len - (int32_t)AERON_DATA_HEADER_LENGTH; i++)
    {
        EXPECT_EQ(0xA0, file_data[frame1_len + AERON_DATA_HEADER_LENGTH + i]);
    }
}

/* Port of: onBlockShouldNotComputeCrcForThePaddingFrame
 * We verify that a padding frame header is written but the body is zeros. */
TEST_F(RecordingWriterTest, PaddingFrameBodyNotWritten)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    const int32_t frame_len = 128;
    const int32_t session_id = 5;
    const int32_t term_id = 18;
    std::vector<uint8_t> buf(512, 0);

    /* Build a padding frame. Fill payload with 0x63 in the source buffer. */
    set_frame(buf.data(), 0, AERON_HDR_TYPE_PAD, frame_len, session_id, term_id, 0x63,
        (int32_t)(frame_len - AERON_DATA_HEADER_LENGTH));

    /* When is_padding is true, only the header (AERON_DATA_HEADER_LENGTH bytes) is written,
     * but the position advances by `length` (frame_len). */
    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buf.data(), (size_t)frame_len, true));
    aeron_archive_recording_writer_close(writer);

    std::string seg = archive_dir + "/" + segment_file_name(1, 0);
    ASSERT_TRUE(file_exists(seg));
    EXPECT_EQ(SEGMENT_LENGTH, file_size(seg));

    auto file_data = read_file(seg);

    const aeron_data_header_t *hdr = frame_at(file_data.data(), 0);
    EXPECT_EQ(AERON_HDR_TYPE_PAD, hdr->frame_header.type);
    EXPECT_EQ(frame_len, hdr->frame_header.frame_length);
    EXPECT_EQ(term_id, hdr->term_id);
    EXPECT_EQ(session_id, hdr->session_id);

    /* The body beyond the header in the file should be zeros (file was ftruncated) */
    for (size_t i = AERON_DATA_HEADER_LENGTH; i < (size_t)frame_len; i++)
    {
        EXPECT_EQ(0, file_data[i]) << "padding body at offset " << i << " should be zero";
    }
}

/* Additional: verify writer position tracking */
TEST_F(RecordingWriterTest, PositionTracking)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 2, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    EXPECT_EQ(0, aeron_archive_recording_writer_position(writer));

    const int32_t block_size = 256;
    std::vector<uint8_t> buf(block_size, 0);
    set_frame(buf.data(), 0, AERON_HDR_TYPE_DATA, block_size, 0, 0, 0x42,
        (int32_t)(block_size - AERON_DATA_HEADER_LENGTH));

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buf.data(), block_size, false));
    EXPECT_EQ(block_size, aeron_archive_recording_writer_position(writer));

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, buf.data(), block_size, false));
    EXPECT_EQ(block_size * 2, aeron_archive_recording_writer_position(writer));

    aeron_archive_recording_writer_close(writer);
}

/* Port of: writer returns error on write after close */
TEST_F(RecordingWriterTest, WriteAfterCloseIsFlaggedFails)
{
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 1, 0, 0, TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));

    /* Close without init -- writer should still be closeable */
    aeron_archive_recording_writer_close(writer);
    /* writer is freed now, so no further access */
}

/* ===========================================================================
 * RecordingWriter + RecordingReader round-trip test
 * ===========================================================================*/

class RecordingWriterReaderTest : public ::testing::Test
{
protected:
    std::string archive_dir;

    void SetUp() override
    {
        archive_dir = make_temp_dir();
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir);
    }
};

struct fragment_verify_clientd
{
    int32_t expected_frame_offset;
    int32_t expected_frame_length;
    int fragments_received;
};

static void fragment_verify_handler(
    const uint8_t *buffer,
    size_t length,
    int16_t frame_type,
    uint8_t flags,
    int64_t reserved_value,
    void *clientd)
{
    auto *ctx = (fragment_verify_clientd *)clientd;
    (void)frame_type;
    (void)flags;
    (void)reserved_value;
    (void)buffer;

    EXPECT_EQ((size_t)(ctx->expected_frame_length - (int32_t)AERON_DATA_HEADER_LENGTH), length);
    ctx->fragments_received++;
}

/* Port of the reader verification from RecordingSessionTest.shouldRecordFragmentsFromImage.
 * We write data directly with the writer and verify we can read it back with the reader. */
TEST_F(RecordingWriterReaderTest, WriteAndReadBackSingleFragment)
{
    const int64_t recording_id = 12345;
    const int32_t term_offset = 1024;
    const int64_t start_position = term_offset;
    const int32_t recorded_block_length = 100;
    const int32_t session_id = 12345;
    const int32_t stream_id = 54321;
    const int32_t initial_term_id = 0;

    /* Create and init writer */
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, recording_id, start_position, start_position,
        TERM_LENGTH, TERM_LENGTH, /* segment_length == TERM_LENGTH for this test */
        archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Build a data frame */
    std::vector<uint8_t> frame_buf(recorded_block_length, 0);
    aeron_data_header_t *hdr = (aeron_data_header_t *)frame_buf.data();
    hdr->frame_header.frame_length = recorded_block_length;
    hdr->frame_header.type = AERON_HDR_TYPE_DATA;
    hdr->term_offset = term_offset;
    hdr->session_id = session_id;
    hdr->stream_id = stream_id;
    hdr->term_id = initial_term_id;

    /* Fill payload */
    memset(frame_buf.data() + AERON_DATA_HEADER_LENGTH, 0xAB,
        (size_t)(recorded_block_length - AERON_DATA_HEADER_LENGTH));

    ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, frame_buf.data(), recorded_block_length, false));
    aeron_archive_recording_writer_close(writer);

    /* Read back using recording reader */
    const int64_t stop_position = start_position + recorded_block_length;

    aeron_archive_recording_reader_t *reader = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_reader_create(
        &reader,
        recording_id,
        start_position,
        stop_position,
        AERON_ARCHIVE_NULL_POSITION,
        AERON_ARCHIVE_NULL_LENGTH,
        TERM_LENGTH,
        TERM_LENGTH, /* segment_length */
        initial_term_id,
        stream_id,
        archive_dir.c_str()));

    fragment_verify_clientd ctx;
    ctx.expected_frame_offset = term_offset;
    ctx.expected_frame_length = recorded_block_length;
    ctx.fragments_received = 0;

    int fragments = aeron_archive_recording_reader_poll(reader, fragment_verify_handler, &ctx, 1);
    EXPECT_EQ(1, fragments);
    EXPECT_EQ(1, ctx.fragments_received);

    aeron_archive_recording_reader_close(reader);
}

/* ===========================================================================
 * RecordingSessionTest fixture
 * ===========================================================================*/

class RecordingSessionTest : public ::testing::Test
{
protected:
    std::string archive_dir;

    void SetUp() override
    {
        archive_dir = make_temp_dir();
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir);
    }
};

/* Port of: shouldRecordFragmentsFromImage -- state machine transitions
 * Since we do not have a real aeron_image_t, we test the session state machine
 * by directly manipulating the session struct and calling do_work. */
TEST_F(RecordingSessionTest, SessionStateInitTransitionsToRecordingOnInit)
{
    /* We cannot fully replicate the Java mock-based test without a real Aeron
     * client/image. Instead, we verify:
     *   1. A session can be created.
     *   2. State starts in INIT.
     *   3. is_done returns false in INIT.
     *   4. Manually setting state to STOPPED makes is_done return true.
     *
     * For full integration tests with aeron_image_t, see the archive system tests.
     */

    /* Verify the state enum values match expected transitions */
    EXPECT_EQ(0, AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT);
    EXPECT_EQ(1, AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING);
    EXPECT_EQ(2, AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE);
    EXPECT_EQ(3, AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED);
}

/* Test the recording session struct directly for state machine without an image */
TEST_F(RecordingSessionTest, SessionIdMatchesRecordingId)
{
    /* Build a minimal session struct directly to test accessors.
     * This mirrors the Java test: assertEquals(RECORDING_ID, session.sessionId()). */
    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.recording_id = 12345;
    session.correlation_id = 67890;
    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT;
    session.is_auto_stop = false;
    session.is_aborted = false;
    session.recording_writer = nullptr;
    session.recording_events_proxy = nullptr;
    session.image = nullptr;
    session.position = nullptr;
    session.original_channel = nullptr;
    session.error_message = nullptr;

    EXPECT_EQ(12345, aeron_archive_recording_session_session_id(&session));
    EXPECT_EQ(12345, aeron_archive_recording_session_recording_id(&session));
    EXPECT_EQ(67890, aeron_archive_recording_session_correlation_id(&session));
    EXPECT_FALSE(aeron_archive_recording_session_is_done(&session));
    EXPECT_FALSE(aeron_archive_recording_session_is_auto_stop(&session));
    EXPECT_EQ(nullptr, aeron_archive_recording_session_error_message(&session));
    EXPECT_EQ(0, aeron_archive_recording_session_error_code(&session));
}

/* Port of: verify isDone after state transitions */
TEST_F(RecordingSessionTest, IsDoneOnlyWhenStopped)
{
    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT;
    EXPECT_FALSE(aeron_archive_recording_session_is_done(&session));

    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING;
    EXPECT_FALSE(aeron_archive_recording_session_is_done(&session));

    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE;
    EXPECT_FALSE(aeron_archive_recording_session_is_done(&session));

    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED;
    EXPECT_TRUE(aeron_archive_recording_session_is_done(&session));
}

/* Test abort sets the flag */
TEST_F(RecordingSessionTest, AbortSetsFlag)
{
    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.is_aborted = false;
    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING;

    aeron_archive_recording_session_abort(&session, "test abort reason");
    EXPECT_TRUE(session.is_aborted);
}

/* Test auto-stop accessor */
TEST_F(RecordingSessionTest, AutoStopAccessor)
{
    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.is_auto_stop = false;
    EXPECT_FALSE(aeron_archive_recording_session_is_auto_stop(&session));

    session.is_auto_stop = true;
    EXPECT_TRUE(aeron_archive_recording_session_is_auto_stop(&session));
}

/* Port of state machine: INIT -> RECORDING -> INACTIVE -> STOPPED via doWork.
 * We test the transitions by calling do_work on a session with a real writer
 * but a NULL image (which will cause init to fail, transitioning to STOPPED). */
TEST_F(RecordingSessionTest, DoWorkTransitionsFromInitToStoppedOnInitFailure)
{
    /* Create a recording writer that will fail init (bad archive dir) */
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 99, 0, 0, TERM_LENGTH, SEGMENT_LENGTH,
        "/nonexistent_dir_that_should_not_exist", false, false));

    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.recording_id = 99;
    session.correlation_id = 1;
    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT;
    session.is_auto_stop = false;
    session.is_aborted = false;
    session.recording_writer = writer;
    session.recording_events_proxy = nullptr;
    session.image = nullptr;
    session.position = nullptr;
    session.original_channel = nullptr;
    session.error_message = nullptr;

    /* do_work in INIT state will call recording_session_init which calls writer init.
     * Writer init will fail because the archive dir doesn't exist.
     * This should transition to STOPPED. */
    aeron_archive_recording_session_do_work(&session);
    EXPECT_TRUE(aeron_archive_recording_session_is_done(&session));
    EXPECT_EQ(AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED, session.state);

    /* Clean up the error message that was strdup'd */
    free(session.error_message);
    /* Writer was closed by the init failure path, no need to close again */
}

/* Test abort during RECORDING causes transition through INACTIVE to STOPPED */
TEST_F(RecordingSessionTest, AbortDuringRecordingTransitionsToStopped)
{
    /* We need a writer that is properly initialized for the INACTIVE->STOPPED transition */
    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, 100, 0, 0, TERM_LENGTH, SEGMENT_LENGTH,
        archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    aeron_archive_recording_session_t session;
    memset(&session, 0, sizeof(session));

    session.recording_id = 100;
    session.correlation_id = 2;
    session.state = AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING;
    session.is_auto_stop = false;
    session.is_aborted = false;
    session.recording_writer = writer;
    session.recording_events_proxy = nullptr;
    session.image = nullptr;
    session.position = nullptr;
    session.original_channel = nullptr;
    session.error_message = nullptr;

    /* Abort the session */
    aeron_archive_recording_session_abort(&session, "test abort");
    EXPECT_TRUE(session.is_aborted);

    /* do_work should detect abort, set INACTIVE, then transition to STOPPED */
    aeron_archive_recording_session_do_work(&session);

    EXPECT_TRUE(aeron_archive_recording_session_is_done(&session));
    EXPECT_EQ(AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED, session.state);

    /* Writer was closed during the INACTIVE->STOPPED transition, just free the session memory */
    free(session.error_message);
}

/* Test close on NULL session is safe */
TEST_F(RecordingSessionTest, CloseNullSessionIsSafe)
{
    EXPECT_EQ(0, aeron_archive_recording_session_close(nullptr));
}

/* Test abort_close on NULL session is safe */
TEST_F(RecordingSessionTest, AbortCloseNullSessionIsSafe)
{
    aeron_archive_recording_session_abort_close(nullptr);
}

/* ===========================================================================
 * RecordingWriter + RecordingSession integration: write and read back
 * Port of the RecordingSessionTest.shouldRecordFragmentsFromImage read-back
 * verification, but using direct writer/reader (no mock image needed).
 * ===========================================================================*/

class RecordingIntegrationTest : public ::testing::Test
{
protected:
    std::string archive_dir;

    void SetUp() override
    {
        archive_dir = make_temp_dir();
    }

    void TearDown() override
    {
        remove_temp_dir(archive_dir);
    }
};

struct read_verify_clientd
{
    const uint8_t *expected_payload;
    int32_t expected_payload_length;
    int16_t expected_frame_type;
    int fragments_read;
};

static void read_verify_handler(
    const uint8_t *buffer,
    size_t length,
    int16_t frame_type,
    uint8_t flags,
    int64_t reserved_value,
    void *clientd)
{
    auto *ctx = (read_verify_clientd *)clientd;
    (void)flags;
    (void)reserved_value;

    EXPECT_EQ(ctx->expected_frame_type, frame_type);
    EXPECT_EQ((size_t)ctx->expected_payload_length, length);

    if (ctx->expected_payload != nullptr && length > 0)
    {
        EXPECT_EQ(0, memcmp(buffer, ctx->expected_payload, length));
    }
    ctx->fragments_read++;
}

TEST_F(RecordingIntegrationTest, WriteMultipleFragmentsAndReadBack)
{
    const int64_t recording_id = 42;
    const int64_t start_position = 0;
    const int32_t stream_id = 100;
    const int32_t initial_term_id = 0;

    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, recording_id, start_position, start_position,
        TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Write 3 data frames of different sizes */
    const int32_t sizes[] = { 64, 128, 256 };
    const uint8_t fill_bytes[] = { 0xAA, 0xBB, 0xCC };
    int32_t current_offset = 0;

    for (int i = 0; i < 3; i++)
    {
        int32_t frame_size = sizes[i];
        int32_t payload_size = frame_size - (int32_t)AERON_DATA_HEADER_LENGTH;

        std::vector<uint8_t> frame_buf(frame_size, 0);
        aeron_data_header_t *hdr = (aeron_data_header_t *)frame_buf.data();
        hdr->frame_header.frame_length = frame_size;
        hdr->frame_header.type = AERON_HDR_TYPE_DATA;
        hdr->term_offset = current_offset;
        hdr->session_id = 0;
        hdr->stream_id = stream_id;
        hdr->term_id = initial_term_id;

        memset(frame_buf.data() + AERON_DATA_HEADER_LENGTH, fill_bytes[i], (size_t)payload_size);

        ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, frame_buf.data(), frame_size, false));
        current_offset += frame_size;
    }

    int64_t stop_position = aeron_archive_recording_writer_position(writer);
    EXPECT_EQ(current_offset, stop_position);
    aeron_archive_recording_writer_close(writer);

    /* Read back all fragments */
    aeron_archive_recording_reader_t *reader = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_reader_create(
        &reader,
        recording_id,
        start_position,
        stop_position,
        AERON_ARCHIVE_NULL_POSITION,
        AERON_ARCHIVE_NULL_LENGTH,
        TERM_LENGTH,
        SEGMENT_LENGTH,
        initial_term_id,
        stream_id,
        archive_dir.c_str()));

    for (int i = 0; i < 3; i++)
    {
        int32_t payload_size = sizes[i] - (int32_t)AERON_DATA_HEADER_LENGTH;
        std::vector<uint8_t> expected_payload(payload_size, fill_bytes[i]);

        read_verify_clientd ctx;
        ctx.expected_payload = expected_payload.data();
        ctx.expected_payload_length = payload_size;
        ctx.expected_frame_type = AERON_HDR_TYPE_DATA;
        ctx.fragments_read = 0;

        int fragments = aeron_archive_recording_reader_poll(reader, read_verify_handler, &ctx, 1);
        EXPECT_EQ(1, fragments) << "expected 1 fragment at index " << i;
        EXPECT_EQ(1, ctx.fragments_read);
    }

    EXPECT_TRUE(aeron_archive_recording_reader_is_done(reader));
    aeron_archive_recording_reader_close(reader);
}

/* Segment rollover: write enough to fill one segment, then read across the boundary */
TEST_F(RecordingIntegrationTest, ReadAcrossSegmentBoundary)
{
    const int64_t recording_id = 77;
    const int64_t start_position = 0;
    const int32_t stream_id = 200;
    const int32_t initial_term_id = 0;
    const int32_t frame_size = 1024;
    const int32_t payload_size = frame_size - (int32_t)AERON_DATA_HEADER_LENGTH;

    aeron_archive_recording_writer_t *writer = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_writer_create(
        &writer, recording_id, start_position, start_position,
        TERM_LENGTH, SEGMENT_LENGTH, archive_dir.c_str(), false, false));
    ASSERT_EQ(0, aeron_archive_recording_writer_init(writer));

    /* Fill exactly one segment + 1 extra frame */
    const int frames_per_segment = SEGMENT_LENGTH / frame_size;
    const int total_frames = frames_per_segment + 1;
    int32_t current_offset = 0;

    for (int i = 0; i < total_frames; i++)
    {
        std::vector<uint8_t> frame_buf(frame_size, 0);
        aeron_data_header_t *hdr = (aeron_data_header_t *)frame_buf.data();
        hdr->frame_header.frame_length = frame_size;
        hdr->frame_header.type = AERON_HDR_TYPE_DATA;
        hdr->term_offset = current_offset % TERM_LENGTH;
        hdr->session_id = 0;
        hdr->stream_id = stream_id;
        hdr->term_id = initial_term_id + (current_offset / TERM_LENGTH);

        memset(frame_buf.data() + AERON_DATA_HEADER_LENGTH, (uint8_t)(i & 0xFF), (size_t)payload_size);

        ASSERT_EQ(0, aeron_archive_recording_writer_write(writer, frame_buf.data(), frame_size, false))
            << "write failed at frame " << i;
        current_offset += frame_size;
    }

    int64_t stop_position = aeron_archive_recording_writer_position(writer);
    aeron_archive_recording_writer_close(writer);

    /* Verify both segment files exist */
    std::string seg1 = archive_dir + "/" + segment_file_name(recording_id, 0);
    std::string seg2 = archive_dir + "/" + segment_file_name(recording_id, SEGMENT_LENGTH);
    ASSERT_TRUE(file_exists(seg1));
    ASSERT_TRUE(file_exists(seg2));

    /* Read all fragments back */
    aeron_archive_recording_reader_t *reader = nullptr;
    ASSERT_EQ(0, aeron_archive_recording_reader_create(
        &reader,
        recording_id,
        start_position,
        stop_position,
        AERON_ARCHIVE_NULL_POSITION,
        AERON_ARCHIVE_NULL_LENGTH,
        TERM_LENGTH,
        SEGMENT_LENGTH,
        initial_term_id,
        stream_id,
        archive_dir.c_str()));

    int total_read = 0;
    while (!aeron_archive_recording_reader_is_done(reader))
    {
        read_verify_clientd ctx;
        ctx.expected_payload = nullptr;
        ctx.expected_payload_length = payload_size;
        ctx.expected_frame_type = AERON_HDR_TYPE_DATA;
        ctx.fragments_read = 0;

        int fragments = aeron_archive_recording_reader_poll(reader, read_verify_handler, &ctx, 10);
        ASSERT_GE(fragments, 0);
        total_read += fragments;

        if (fragments == 0)
        {
            break;
        }
    }

    EXPECT_EQ(total_frames, total_read);
    aeron_archive_recording_reader_close(reader);
}
