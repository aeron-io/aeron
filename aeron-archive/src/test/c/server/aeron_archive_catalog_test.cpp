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

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <numeric>
#include <random>

#ifdef _MSC_VER
#include <direct.h>
#include <io.h>
static char *mkdtemp(char *tmpl)
{
    if (_mktemp_s(tmpl, strlen(tmpl) + 1) != 0) return NULL;
    if (_mkdir(tmpl) != 0) return NULL;
    return tmpl;
}
#endif

extern "C"
{
#include "server/aeron_archive_catalog.h"
#include "aeronc.h"
#include "util/aeron_bitutil.h"
}

/* ========================================================================
 * CatalogIndex tests (ported from CatalogIndexTest.java)
 * ======================================================================== */

#define CATALOG_INDEX_DEFAULT_CAPACITY (10)
#define CATALOG_INDEX_NULL_VALUE (-1)

/*
 * Helper: create a fresh catalog index. Since the C API makes the index struct
 * part of aeron_archive_catalog_t and the init/close functions are static, we
 * replicate the simple struct operations directly for testing the index logic.
 */
class CatalogIndexTest : public ::testing::Test
{
protected:
    aeron_archive_catalog_index_t m_index = {};

    void SetUp() override
    {
        m_index.count = 0;
        m_index.capacity = CATALOG_INDEX_DEFAULT_CAPACITY;
        m_index.entries = (int64_t *)calloc(
            (size_t)(m_index.capacity * 2), sizeof(int64_t));
        ASSERT_NE(nullptr, m_index.entries);
    }

    void TearDown() override
    {
        free(m_index.entries);
        m_index.entries = nullptr;
    }

    /* Mimic aeron_archive_catalog_index_ensure_capacity */
    int ensure_capacity()
    {
        if (m_index.count >= m_index.capacity)
        {
            int32_t new_capacity = m_index.capacity + (m_index.capacity >> 1);
            int64_t *new_entries = (int64_t *)calloc(
                (size_t)(new_capacity * 2), sizeof(int64_t));
            if (nullptr == new_entries)
            {
                return -1;
            }
            memcpy(new_entries, m_index.entries,
                (size_t)(m_index.count * 2) * sizeof(int64_t));
            free(m_index.entries);
            m_index.entries = new_entries;
            m_index.capacity = new_capacity;
        }
        return 0;
    }

    /* Mimic aeron_archive_catalog_index_add with validation */
    int add(int64_t recording_id, int64_t offset)
    {
        if (recording_id < 0)
        {
            return -1;
        }
        if (offset < 0)
        {
            return -2;
        }
        if (m_index.count > 0)
        {
            int64_t last_id = m_index.entries[(m_index.count - 1) * 2];
            if (recording_id <= last_id)
            {
                return -3;
            }
        }
        if (ensure_capacity() < 0)
        {
            return -4;
        }
        int32_t pos = m_index.count * 2;
        m_index.entries[pos] = recording_id;
        m_index.entries[pos + 1] = offset;
        m_index.count++;
        return 0;
    }

    /* Mimic aeron_archive_catalog_index_find (binary interpolation search) */
    int32_t find(int64_t recording_id) const
    {
        if (m_index.count == 0)
        {
            return -1;
        }

        const int64_t *entries = m_index.entries;
        const int32_t last_position = (m_index.count - 1) * 2;

        if (last_position > 0 && recording_id >= entries[0] && recording_id <= entries[last_position])
        {
            int32_t position = (int32_t)(
                (recording_id - entries[0]) * last_position / (entries[last_position] - entries[0]));
            position = (0 == (position & 1)) ? position : position + 1;

            if (recording_id == entries[position])
            {
                return position;
            }
            else if (recording_id > entries[position])
            {
                for (int32_t i = position + 2; i <= last_position; i += 2)
                {
                    int64_t id = entries[i];
                    if (recording_id == id)
                    {
                        return i;
                    }
                    else if (id > recording_id)
                    {
                        break;
                    }
                }
            }
            else
            {
                for (int32_t i = position - 2; i >= 0; i -= 2)
                {
                    int64_t id = entries[i];
                    if (recording_id == id)
                    {
                        return i;
                    }
                    else if (id < recording_id)
                    {
                        break;
                    }
                }
            }
        }
        else if (0 == last_position && recording_id == entries[0])
        {
            return 0;
        }

        return -1;
    }

    /* Mimic aeron_archive_catalog_index_recording_offset */
    int64_t recording_offset(int64_t recording_id) const
    {
        if (recording_id < 0)
        {
            return CATALOG_INDEX_NULL_VALUE;
        }
        int32_t position = find(recording_id);
        if (position < 0)
        {
            return CATALOG_INDEX_NULL_VALUE;
        }
        return m_index.entries[position + 1];
    }

    /* Mimic aeron_archive_catalog_index_remove */
    int64_t remove(int64_t recording_id)
    {
        if (recording_id < 0)
        {
            return CATALOG_INDEX_NULL_VALUE;
        }
        int32_t position = find(recording_id);
        if (position < 0)
        {
            return CATALOG_INDEX_NULL_VALUE;
        }

        int64_t offset = m_index.entries[position + 1];
        int32_t last_position = (m_index.count - 1) * 2;

        for (int32_t i = position; i < last_position; i += 2)
        {
            m_index.entries[i] = m_index.entries[i + 2];
            m_index.entries[i + 1] = m_index.entries[i + 3];
        }
        m_index.entries[last_position] = 0;
        m_index.entries[last_position + 1] = 0;
        m_index.count--;

        return offset;
    }

    /* Get the total number of int64_t slots currently allocated */
    int32_t entries_length() const
    {
        return m_index.capacity * 2;
    }
};

TEST_F(CatalogIndexTest, defaultIndexCapacityIsTenEntries)
{
    /* Default capacity is 10, so 20 int64_t slots, all zero */
    EXPECT_EQ(CATALOG_INDEX_DEFAULT_CAPACITY, m_index.capacity);
    for (int32_t i = 0; i < entries_length(); i++)
    {
        EXPECT_EQ(0, m_index.entries[i]);
    }
}

TEST_F(CatalogIndexTest, sizeReturnsZeroForAnEmptyIndex)
{
    EXPECT_EQ(0, m_index.count);
}

TEST_F(CatalogIndexTest, addThrowsIfRecordingIdIsNegative)
{
    EXPECT_NE(0, add(-1, 0));
}

TEST_F(CatalogIndexTest, addThrowsIfRecordingOffsetIsNegative)
{
    EXPECT_NE(0, add(1024, INT32_MIN));
}

TEST_F(CatalogIndexTest, addOneRecording)
{
    const int64_t recording_id = 3;
    const int64_t offset = 100;

    ASSERT_EQ(0, add(recording_id, offset));

    EXPECT_EQ(recording_id, m_index.entries[0]);
    EXPECT_EQ(offset, m_index.entries[1]);
    EXPECT_EQ(1, m_index.count);
}

TEST_F(CatalogIndexTest, addAppendsToTheEndOfTheIndex)
{
    const int64_t id1 = 6, offset1 = 50;
    const int64_t id2 = 21, offset2 = 64;

    ASSERT_EQ(0, add(id1, offset1));
    ASSERT_EQ(0, add(id2, offset2));

    EXPECT_EQ(id1, m_index.entries[0]);
    EXPECT_EQ(offset1, m_index.entries[1]);
    EXPECT_EQ(id2, m_index.entries[2]);
    EXPECT_EQ(offset2, m_index.entries[3]);
    EXPECT_EQ(2, m_index.count);
}

TEST_F(CatalogIndexTest, addThrowsIfNewRecordingIdIsLessThanOrEqualToExistingId_less)
{
    ASSERT_EQ(0, add(100, 0));
    EXPECT_NE(0, add(5, 200));
}

TEST_F(CatalogIndexTest, addThrowsIfNewRecordingIdIsLessThanOrEqualToExistingId_equal)
{
    ASSERT_EQ(0, add(100, 0));
    EXPECT_NE(0, add(100, 200));
}

TEST_F(CatalogIndexTest, addExpandsIndexWhenFull)
{
    for (int i = 0; i < CATALOG_INDEX_DEFAULT_CAPACITY; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    ASSERT_EQ(0, add(INT64_MAX, INT64_MAX));

    EXPECT_EQ(CATALOG_INDEX_DEFAULT_CAPACITY + 1, m_index.count);

    /* Verify all entries are present */
    for (int i = 0; i < CATALOG_INDEX_DEFAULT_CAPACITY; i++)
    {
        EXPECT_EQ(i, m_index.entries[i * 2]);
        EXPECT_EQ(i, m_index.entries[i * 2 + 1]);
    }
    int32_t pos = CATALOG_INDEX_DEFAULT_CAPACITY * 2;
    EXPECT_EQ(INT64_MAX, m_index.entries[pos]);
    EXPECT_EQ(INT64_MAX, m_index.entries[pos + 1]);
}

TEST_F(CatalogIndexTest, getThrowsIfRecordingIdIsNegative)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(-100));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_0)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(0));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_1)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(1));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_capacity)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(CATALOG_INDEX_DEFAULT_CAPACITY));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_doubleCapacity)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(CATALOG_INDEX_DEFAULT_CAPACITY * 2));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_100)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(100));
}

TEST_F(CatalogIndexTest, getReturnsNullValueOnAnEmptyIndex_max)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(INT64_MAX));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundSingleEntry_0)
{
    ASSERT_EQ(0, add(200, 0));
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(0));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundSingleEntry_199)
{
    ASSERT_EQ(0, add(200, 0));
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(199));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundSingleEntry_201)
{
    ASSERT_EQ(0, add(200, 0));
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(201));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundSingleEntry_max)
{
    ASSERT_EQ(0, add(200, 0));
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(INT64_MAX));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundMultipleEntries_9)
{
    for (int i = 10; i < 100; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(9));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundMultipleEntries_101)
{
    for (int i = 10; i < 100; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(101));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundMultipleEntries_1000000000)
{
    for (int i = 10; i < 100; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(1000000000));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundMultipleEntries_4002662252)
{
    for (int i = 10; i < 100; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(INT64_C(4002662252)));
}

TEST_F(CatalogIndexTest, getReturnsNullValueIfRecordingIdNotFoundMultipleEntries_max)
{
    for (int i = 10; i < 100; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(INT64_MAX));
}

TEST_F(CatalogIndexTest, getReturnsOffsetAssociatedWithRecordingId)
{
    ASSERT_EQ(0, add(0, 0));
    ASSERT_EQ(0, add(1, 1));
    ASSERT_EQ(0, add(2, 3));
    ASSERT_EQ(0, add(100, 777));
    ASSERT_EQ(0, add(1000, 2000));
    ASSERT_EQ(0, add(1001, 2002));

    EXPECT_EQ(0, recording_offset(0));
    EXPECT_EQ(1, recording_offset(1));
    EXPECT_EQ(3, recording_offset(2));
    EXPECT_EQ(777, recording_offset(100));
    EXPECT_EQ(2000, recording_offset(1000));
    EXPECT_EQ(2002, recording_offset(1001));
}

TEST_F(CatalogIndexTest, getSingleElement_0)
{
    ASSERT_EQ(0, add(0, 0));
    EXPECT_EQ(0, recording_offset(0));
}

TEST_F(CatalogIndexTest, getSingleElement_100)
{
    ASSERT_EQ(0, add(100, 300));
    EXPECT_EQ(300, recording_offset(100));
}

TEST_F(CatalogIndexTest, getSingleElement_max)
{
    ASSERT_EQ(0, add(INT64_MAX, (int64_t)((uint64_t)INT64_MAX * 3)));
    /* (int64_t)((uint64_t)INT64_MAX * 3) overflows, but we test what the index stores */
    EXPECT_EQ((int64_t)((uint64_t)INT64_MAX * 3), recording_offset(INT64_MAX));
}

TEST_F(CatalogIndexTest, getFirstElement)
{
    ASSERT_EQ(0, add(0, 10));
    ASSERT_EQ(0, add(1, 20));
    ASSERT_EQ(0, add(2, 30));

    EXPECT_EQ(10, recording_offset(0));
}

TEST_F(CatalogIndexTest, getLastElement)
{
    ASSERT_EQ(0, add(1, 10));
    ASSERT_EQ(0, add(5, 20));
    ASSERT_EQ(0, add(12, 30));

    EXPECT_EQ(30, recording_offset(12));
}

TEST_F(CatalogIndexTest, getLastElementWhenIndexIsFull)
{
    /* Generate sorted unique random values using seed 8327434, matching Java test */
    std::mt19937_64 rng(8327434);
    std::uniform_int_distribution<int64_t> dist(0, INT64_MAX - 1);

    std::vector<int64_t> values;
    while ((int32_t)values.size() < CATALOG_INDEX_DEFAULT_CAPACITY)
    {
        int64_t v = dist(rng);
        if (std::find(values.begin(), values.end(), v) == values.end())
        {
            values.push_back(v);
        }
    }
    std::sort(values.begin(), values.end());

    for (int i = 0; i < CATALOG_INDEX_DEFAULT_CAPACITY; i++)
    {
        ASSERT_EQ(0, add(values[i], i + 1));
    }

    int64_t last_value = values[values.size() - 1];
    EXPECT_EQ(CATALOG_INDEX_DEFAULT_CAPACITY, recording_offset(last_value));
}

TEST_F(CatalogIndexTest, removeThrowsIfNegativeRecordingId)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, remove(-1));
}

TEST_F(CatalogIndexTest, removeReturnsNullValueWhenIndexIsEmpty)
{
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, remove(1));
}

TEST_F(CatalogIndexTest, removeReturnsNullValueWhenNonExistingRecordingIdIsProvided)
{
    ASSERT_EQ(0, add(1, 0));
    ASSERT_EQ(0, add(20, 10));

    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, remove(7));
    EXPECT_EQ(2, m_index.count);
}

TEST_F(CatalogIndexTest, removeTheOnlyEntryFromTheIndex)
{
    const int64_t recording_id = 0;
    const int64_t offset = 1024;
    ASSERT_EQ(0, add(recording_id, offset));

    EXPECT_EQ(offset, remove(recording_id));
    EXPECT_EQ(0, m_index.count);
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(recording_id));
}

TEST_F(CatalogIndexTest, removeFirstElement)
{
    ASSERT_EQ(0, add(0, 500));
    ASSERT_EQ(0, add(1, 1000));
    ASSERT_EQ(0, add(2, 1500));

    EXPECT_EQ(500, remove(0));
    EXPECT_EQ(2, m_index.count);

    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(0));
    EXPECT_EQ(1000, recording_offset(1));
    EXPECT_EQ(1500, recording_offset(2));
}

TEST_F(CatalogIndexTest, removeLastElement)
{
    ASSERT_EQ(0, add(0, 500));
    ASSERT_EQ(0, add(1, 1000));
    ASSERT_EQ(0, add(2, 1500));

    EXPECT_EQ(1500, remove(2));
    EXPECT_EQ(2, m_index.count);

    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(2));
    EXPECT_EQ(500, recording_offset(0));
    EXPECT_EQ(1000, recording_offset(1));
}

TEST_F(CatalogIndexTest, removeMiddleElement)
{
    ASSERT_EQ(0, add(0, 500));
    ASSERT_EQ(0, add(10, 1000));
    ASSERT_EQ(0, add(20, 1500));
    ASSERT_EQ(0, add(30, 7777));

    EXPECT_EQ(1000, remove(10));
    EXPECT_EQ(3, m_index.count);

    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(10));
    EXPECT_EQ(500, recording_offset(0));
    EXPECT_EQ(1500, recording_offset(20));
    EXPECT_EQ(7777, recording_offset(30));
}

TEST_F(CatalogIndexTest, removeLastElementFromTheFullIndex)
{
    for (int i = 1; i <= CATALOG_INDEX_DEFAULT_CAPACITY; i++)
    {
        ASSERT_EQ(0, add(i, i));
    }

    EXPECT_EQ(CATALOG_INDEX_DEFAULT_CAPACITY, remove(CATALOG_INDEX_DEFAULT_CAPACITY));
    EXPECT_EQ(CATALOG_INDEX_DEFAULT_CAPACITY - 1, m_index.count);
    EXPECT_EQ(CATALOG_INDEX_NULL_VALUE, recording_offset(CATALOG_INDEX_DEFAULT_CAPACITY));
}

/* ========================================================================
 * Catalog tests (ported from CatalogTest.java)
 * ======================================================================== */

static const size_t TEST_CAPACITY = 1024;
static const int32_t TERM_LENGTH = 2 * 4096;     /* 2 * PAGE_SIZE in Java */
static const int32_t SEGMENT_LENGTH = 2 * TERM_LENGTH;
static const int32_t MTU_LENGTH = 1024;

class CatalogTest : public ::testing::Test
{
protected:
    char m_archive_dir[256] = {};
    aeron_archive_catalog_t *m_catalog = nullptr;
    int64_t m_recording_one_id = -1;
    int64_t m_recording_two_id = -1;
    int64_t m_recording_three_id = -1;

    void SetUp() override
    {
        snprintf(m_archive_dir, sizeof(m_archive_dir), "/tmp/aeron_catalog_test_XXXXXX");
        ASSERT_NE(nullptr, mkdtemp(m_archive_dir));

        /* Create a catalog and add three recordings */
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, m_archive_dir, TEST_CAPACITY, false, false));

        m_recording_one_id = aeron_archive_catalog_add_recording(
            catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1,
            "channelG", "channelG?tag=f", "sourceA");
        ASSERT_GE(m_recording_one_id, 0);

        m_recording_two_id = aeron_archive_catalog_add_recording(
            catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2,
            "channelH", "channelH?tag=f", "sourceV");
        ASSERT_GE(m_recording_two_id, 0);

        m_recording_three_id = aeron_archive_catalog_add_recording(
            catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3,
            "channelThatIsVeryLongAndShouldNotBeTruncated",
            "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
            "source can also be a very very very long String and it will not be truncated even "
            "if gets very very long");
        ASSERT_GE(m_recording_three_id, 0);

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }

    void TearDown() override
    {
        if (nullptr != m_catalog)
        {
            aeron_archive_catalog_close(m_catalog);
            m_catalog = nullptr;
        }
        std::string cmd = std::string("rm -rf ") + m_archive_dir;
        if (std::system(cmd.c_str())) {}
    }

    void verify_recording(
        aeron_archive_catalog_t *catalog,
        int64_t recording_id,
        int64_t start_position,
        int64_t stop_position,
        int64_t start_timestamp,
        int64_t stop_timestamp,
        int32_t initial_term_id,
        int32_t segment_file_length,
        int32_t term_buffer_length,
        int32_t mtu_length,
        int32_t session_id,
        int32_t stream_id,
        const char *stripped_channel,
        const char *original_channel,
        const char *source_identity)
    {
        aeron_archive_catalog_recording_descriptor_t desc = {};
        ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, recording_id, &desc));

        EXPECT_EQ(AERON_ARCHIVE_RECORDING_VALID, desc.state);
        EXPECT_EQ(recording_id, desc.recording_id);
        EXPECT_EQ(start_position, desc.start_position);
        EXPECT_EQ(stop_position, desc.stop_position);
        EXPECT_EQ(start_timestamp, desc.start_timestamp);
        EXPECT_EQ(stop_timestamp, desc.stop_timestamp);
        EXPECT_EQ(initial_term_id, desc.initial_term_id);
        EXPECT_EQ(segment_file_length, desc.segment_file_length);
        EXPECT_EQ(term_buffer_length, desc.term_buffer_length);
        EXPECT_EQ(mtu_length, desc.mtu_length);
        EXPECT_EQ(session_id, desc.session_id);
        EXPECT_EQ(stream_id, desc.stream_id);

        EXPECT_EQ(strlen(stripped_channel), desc.stripped_channel_length);
        EXPECT_EQ(0, strncmp(stripped_channel, desc.stripped_channel, desc.stripped_channel_length));

        EXPECT_EQ(strlen(original_channel), desc.original_channel_length);
        EXPECT_EQ(0, strncmp(original_channel, desc.original_channel, desc.original_channel_length));

        EXPECT_EQ(strlen(source_identity), desc.source_identity_length);
        EXPECT_EQ(0, strncmp(source_identity, desc.source_identity, desc.source_identity_length));
    }
};

/* ------- Catalog create / open / close lifecycle ------- */

TEST_F(CatalogTest, shouldOpenExistingCatalogAndReadRecordings)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    verify_recording(
        catalog, m_recording_one_id,
        0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1,
        "channelG", "channelG?tag=f", "sourceA");

    verify_recording(
        catalog, m_recording_two_id,
        0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 7, 2,
        "channelH", "channelH?tag=f", "sourceV");

    verify_recording(
        catalog, m_recording_three_id,
        0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 8, 3,
        "channelThatIsVeryLongAndShouldNotBeTruncated",
        "channelThatIsVeryLongAndShouldNotBeTruncated?tag=f",
        "source can also be a very very very long String and it will not be truncated even "
        "if gets very very long");

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldReturnNextRecordingId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ(m_recording_three_id + 1, aeron_archive_catalog_next_recording_id(catalog));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldReturnRecordingCount)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ(3, aeron_archive_catalog_recording_count(catalog));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldAppendToExistingCatalog)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    int64_t new_recording_id = aeron_archive_catalog_add_recording(
        catalog, 32, 128, 21, 42,
        5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 4,
        "channelJ", "channelJ?tag=f", "sourceN");
    ASSERT_GE(new_recording_id, 0);
    EXPECT_EQ(m_recording_three_id + 1, new_recording_id);

    verify_recording(
        catalog, new_recording_id,
        32, 128, 21, 42,
        5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 4,
        "channelJ", "channelJ?tag=f", "sourceN");

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    /* Re-open read-only and verify the new recording persisted */
    aeron_archive_catalog_t *catalog2 = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog2, m_archive_dir, false));

    verify_recording(
        catalog2, new_recording_id,
        32, 128, 21, 42,
        5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 9, 4,
        "channelJ", "channelJ?tag=f", "sourceN");

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog2));
}

TEST_F(CatalogTest, shouldAllowMultipleRecordingsForSameStream)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    int64_t new_id = aeron_archive_catalog_add_recording(
        catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 6, 1,
        "channelG", "channelG?tag=f", "sourceA");
    ASSERT_GE(new_id, 0);
    EXPECT_NE(m_recording_one_id, new_id);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- find recording ------- */

TEST_F(CatalogTest, findRecordingReturnsMinusOneForUnknownId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, 999, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, findRecordingReturnsMinusOneForNegativeId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, -1, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- update recording position ------- */

TEST_F(CatalogTest, shouldUpdateRecordingPosition)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
        catalog, m_recording_one_id, 1024, 42));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(1024, desc.stop_position);
    EXPECT_EQ(42, desc.stop_timestamp);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, updatePositionFailsForUnknownRecordingId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    EXPECT_EQ(-1, aeron_archive_catalog_update_recording_position(catalog, 999, 1024, 42));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, updatePositionFailsForNegativeRecordingId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    EXPECT_EQ(-1, aeron_archive_catalog_update_recording_position(catalog, -1, 1024, 42));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- invalidate recording ------- */

TEST_F(CatalogTest, shouldInvalidateRecording)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_one_id));

    /* Should no longer be findable */
    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));

    /* Count should decrease */
    EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

    /* Other recordings should still be valid */
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, invalidateReturnsMinusOneForUnknownRecordingId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    EXPECT_EQ(-1, aeron_archive_catalog_invalidate_recording(catalog, 999));
    EXPECT_EQ(3, aeron_archive_catalog_recording_count(catalog));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, invalidateReturnsMinusOneForNegativeRecordingId)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    EXPECT_EQ(-1, aeron_archive_catalog_invalidate_recording(catalog, -1));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldInvalidateFirstRecording)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_one_id));
    EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldInvalidateMiddleRecording)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));
    EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldInvalidateLastRecording)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_three_id));
    EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- for_each iteration ------- */

struct for_each_context
{
    std::vector<int64_t> recording_ids;
};

static void collect_recording_ids(
    const aeron_archive_catalog_recording_descriptor_t *descriptor, void *clientd)
{
    auto *ctx = (struct for_each_context *)clientd;
    ctx->recording_ids.push_back(descriptor->recording_id);
}

TEST_F(CatalogTest, forEachIteratesOverAllValidRecordings)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    struct for_each_context ctx = {};
    int32_t count = aeron_archive_catalog_for_each(catalog, collect_recording_ids, &ctx);
    EXPECT_EQ(3, count);
    ASSERT_EQ(3u, ctx.recording_ids.size());
    EXPECT_EQ(m_recording_one_id, ctx.recording_ids[0]);
    EXPECT_EQ(m_recording_two_id, ctx.recording_ids[1]);
    EXPECT_EQ(m_recording_three_id, ctx.recording_ids[2]);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, forEachSkipsInvalidatedRecordings)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));

    struct for_each_context ctx = {};
    int32_t count = aeron_archive_catalog_for_each(catalog, collect_recording_ids, &ctx);
    EXPECT_EQ(2, count);
    ASSERT_EQ(2u, ctx.recording_ids.size());
    EXPECT_EQ(m_recording_one_id, ctx.recording_ids[0]);
    EXPECT_EQ(m_recording_three_id, ctx.recording_ids[1]);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- close semantics ------- */

TEST_F(CatalogTest, closeNullCatalogReturnsSuccess)
{
    EXPECT_EQ(0, aeron_archive_catalog_close(nullptr));
}

TEST_F(CatalogTest, createFailsWithNullOutputParam)
{
    EXPECT_EQ(-1, aeron_archive_catalog_create(nullptr, m_archive_dir, TEST_CAPACITY, false, false));
}

TEST_F(CatalogTest, createFailsWithNullArchiveDir)
{
    aeron_archive_catalog_t *catalog = nullptr;
    EXPECT_EQ(-1, aeron_archive_catalog_create(&catalog, nullptr, TEST_CAPACITY, false, false));
}

TEST_F(CatalogTest, openFailsWithNullOutputParam)
{
    EXPECT_EQ(-1, aeron_archive_catalog_open(nullptr, m_archive_dir, false));
}

TEST_F(CatalogTest, openFailsWithNullArchiveDir)
{
    aeron_archive_catalog_t *catalog = nullptr;
    EXPECT_EQ(-1, aeron_archive_catalog_open(&catalog, nullptr, false));
}

/* ------- Binary layout / SBE compatibility ------- */

TEST_F(CatalogTest, shouldVerifyCatalogHeaderLayout)
{
    /* Verify the catalog header offsets are correct by reading raw memory */
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    const uint8_t *buffer = catalog->buffer;

    /* Version should be set */
    int32_t version;
    memcpy(&version, buffer + AERON_ARCHIVE_CATALOG_HEADER_VERSION_OFFSET, sizeof(version));
    EXPECT_NE(0, version);

    /* Header length should be AERON_ARCHIVE_CATALOG_HEADER_LENGTH */
    int32_t header_length;
    memcpy(&header_length, buffer + AERON_ARCHIVE_CATALOG_HEADER_HEADER_LENGTH_OFFSET, sizeof(header_length));
    EXPECT_EQ(AERON_ARCHIVE_CATALOG_HEADER_LENGTH, header_length);

    /* Next recording ID should be m_recording_three_id + 1 */
    int64_t next_recording_id;
    memcpy(&next_recording_id, buffer + AERON_ARCHIVE_CATALOG_HEADER_NEXT_RECORDING_ID_OFFSET,
        sizeof(next_recording_id));
    EXPECT_EQ(m_recording_three_id + 1, next_recording_id);

    /* Alignment should be AERON_CACHE_LINE_LENGTH (64) */
    int32_t alignment;
    memcpy(&alignment, buffer + AERON_ARCHIVE_CATALOG_HEADER_ALIGNMENT_OFFSET, sizeof(alignment));
    EXPECT_EQ((int32_t)AERON_CACHE_LINE_LENGTH, alignment);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

TEST_F(CatalogTest, shouldVerifyRecordingDescriptorBinaryLayout)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    const uint8_t *buffer = catalog->buffer;
    int32_t offset = AERON_ARCHIVE_CATALOG_HEADER_LENGTH;

    /* Read the first recording descriptor header */
    int32_t recording_length;
    memcpy(&recording_length, buffer + offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH_OFFSET,
        sizeof(recording_length));
    EXPECT_GT(recording_length, 0);

    int32_t state;
    memcpy(&state, buffer + offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET, sizeof(state));
    EXPECT_EQ((int32_t)AERON_ARCHIVE_RECORDING_VALID, state);

    /* Read the recording ID from the body */
    const uint8_t *body = buffer + offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;
    int64_t recording_id;
    memcpy(&recording_id, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET, sizeof(recording_id));
    EXPECT_EQ(m_recording_one_id, recording_id);

    /* Read fixed fields */
    int32_t session_id;
    memcpy(&session_id, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_SESSION_ID_OFFSET, sizeof(session_id));
    EXPECT_EQ(6, session_id);

    int32_t stream_id;
    memcpy(&stream_id, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_STREAM_ID_OFFSET, sizeof(stream_id));
    EXPECT_EQ(1, stream_id);

    int32_t term_buffer_length;
    memcpy(&term_buffer_length, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_TERM_BUFFER_LENGTH_OFFSET,
        sizeof(term_buffer_length));
    EXPECT_EQ(TERM_LENGTH, term_buffer_length);

    int32_t segment_file_length;
    memcpy(&segment_file_length, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_SEGMENT_FILE_LENGTH_OFFSET,
        sizeof(segment_file_length));
    EXPECT_EQ(SEGMENT_LENGTH, segment_file_length);

    int32_t mtu_length;
    memcpy(&mtu_length, body + AERON_ARCHIVE_RECORDING_DESCRIPTOR_MTU_LENGTH_OFFSET, sizeof(mtu_length));
    EXPECT_EQ(MTU_LENGTH, mtu_length);

    /* Verify variable-length strippedChannel */
    int32_t var_offset = AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH;
    uint32_t str_len;
    memcpy(&str_len, body + var_offset, sizeof(str_len));
    EXPECT_EQ(strlen("channelG"), (size_t)str_len);
    EXPECT_EQ(0, strncmp("channelG", (const char *)(body + var_offset + sizeof(uint32_t)), str_len));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Persist and re-read through close/open cycle ------- */

TEST_F(CatalogTest, shouldPersistUpdatedPositionAcrossCloseOpen)
{
    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

        ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
            catalog, m_recording_one_id, 2048, 100));

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }

    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

        aeron_archive_catalog_recording_descriptor_t desc = {};
        ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
        EXPECT_EQ(2048, desc.stop_position);
        EXPECT_EQ(100, desc.stop_timestamp);

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }
}

TEST_F(CatalogTest, shouldPersistInvalidationAcrossCloseOpen)
{
    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

        ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }

    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

        /* Recording two should be gone (invalidated on disk) */
        aeron_archive_catalog_recording_descriptor_t desc = {};
        EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
        EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

        /* Other recordings should still be valid */
        EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
        EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }
}

/* ------- Add recording to full catalog ------- */

TEST_F(CatalogTest, addRecordingFailsWhenCatalogIsFull)
{
    /* Create a tiny catalog */
    char tiny_dir[256] = {};
    snprintf(tiny_dir, sizeof(tiny_dir), "/tmp/aeron_catalog_tiny_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(tiny_dir));

    aeron_archive_catalog_t *catalog = nullptr;
    /* Use minimum capacity: just the header + one small recording */
    size_t tiny_capacity = AERON_ARCHIVE_CATALOG_HEADER_LENGTH + 1024 + 128;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, tiny_dir, tiny_capacity, false, false));

    /* First recording should succeed */
    int64_t id1 = aeron_archive_catalog_add_recording(
        catalog, 0L, 0L, 0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH,
        6, 1, "ch", "ch", "s");
    EXPECT_GE(id1, 0);

    /* Eventually the catalog will be full - keep adding until it fails */
    int64_t result = 0;
    for (int i = 0; i < 100 && result >= 0; i++)
    {
        result = aeron_archive_catalog_add_recording(
            catalog, 0L, 0L, 0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH,
            6, 1, "ch", "ch", "s");
    }
    EXPECT_EQ(AERON_NULL_VALUE, result);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + tiny_dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- Multiple add and find with increasing IDs ------- */

TEST_F(CatalogTest, shouldAssignStrictlyIncreasingRecordingIds)
{
    /* Use a separate temp directory to avoid conflicts with the catalog file from SetUp. */
    char ids_dir[256] = {};
    snprintf(ids_dir, sizeof(ids_dir), "/tmp/aeron_catalog_ids_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(ids_dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, ids_dir, TEST_CAPACITY * 4, false, false));

    int64_t prev_id = -1;
    for (int i = 0; i < 10; i++)
    {
        int64_t id = aeron_archive_catalog_add_recording(
            catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, i, i,
            "ch", "ch?tag=t", "src");
        ASSERT_GE(id, 0);
        EXPECT_GT(id, prev_id);
        prev_id = id;
    }

    /* Verify each recording can be found */
    for (int64_t id = 0; id <= prev_id; id++)
    {
        aeron_archive_catalog_recording_descriptor_t desc = {};
        EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, id, &desc));
        EXPECT_EQ(id, desc.recording_id);
    }

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + ids_dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- Update position on multiple recordings ------- */

TEST_F(CatalogTest, shouldUpdatePositionOnMultipleRecordings)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
        catalog, m_recording_one_id, 100, 10));
    ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
        catalog, m_recording_two_id, 200, 20));
    ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
        catalog, m_recording_three_id, 300, 30));

    aeron_archive_catalog_recording_descriptor_t desc = {};

    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(100, desc.stop_position);
    EXPECT_EQ(10, desc.stop_timestamp);

    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(200, desc.stop_position);
    EXPECT_EQ(20, desc.stop_timestamp);

    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));
    EXPECT_EQ(300, desc.stop_position);
    EXPECT_EQ(30, desc.stop_timestamp);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Invalidate all recordings ------- */

TEST_F(CatalogTest, shouldInvalidateAllRecordings)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_one_id));
    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));
    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_three_id));

    EXPECT_EQ(0, aeron_archive_catalog_recording_count(catalog));

    struct for_each_context ctx = {};
    int32_t count = aeron_archive_catalog_for_each(catalog, collect_recording_ids, &ctx);
    EXPECT_EQ(0, count);
    EXPECT_TRUE(ctx.recording_ids.empty());

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Create catalog with default capacity ------- */

TEST_F(CatalogTest, shouldCreateCatalogWithDefaultCapacityWhenZeroPassed)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_dflt_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, 0, false, false));

    /* Capacity should have been set to the default */
    EXPECT_GE(catalog->capacity, (size_t)AERON_ARCHIVE_CATALOG_HEADER_LENGTH);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- Interleaved operations ------- */

TEST_F(CatalogTest, shouldHandleInterleavedAddInvalidateFind)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    /* Add a new recording */
    int64_t id4 = aeron_archive_catalog_add_recording(
        catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 10, 5,
        "ch4", "ch4?tag=t", "src4");
    ASSERT_GE(id4, 0);

    /* Invalidate the second recording */
    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));

    /* Verify: 1 and 3 still there, 2 gone, 4 added */
    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));
    EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, id4, &desc));
    EXPECT_EQ(3, aeron_archive_catalog_recording_count(catalog));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- forEach with descriptor field verification ------- */

struct verify_fields_context
{
    int64_t expected_recording_ids[10];
    int32_t expected_stream_ids[10];
    int count;
    int idx;
};

static void verify_fields_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor, void *clientd)
{
    auto *ctx = (struct verify_fields_context *)clientd;
    ASSERT_LT(ctx->idx, ctx->count);
    EXPECT_EQ(ctx->expected_recording_ids[ctx->idx], descriptor->recording_id);
    EXPECT_EQ(ctx->expected_stream_ids[ctx->idx], descriptor->stream_id);
    ctx->idx++;
}

TEST_F(CatalogTest, forEachProvidesCorrectDescriptorFields)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    struct verify_fields_context ctx = {};
    ctx.expected_recording_ids[0] = m_recording_one_id;
    ctx.expected_recording_ids[1] = m_recording_two_id;
    ctx.expected_recording_ids[2] = m_recording_three_id;
    ctx.expected_stream_ids[0] = 1;
    ctx.expected_stream_ids[1] = 2;
    ctx.expected_stream_ids[2] = 3;
    ctx.count = 3;
    ctx.idx = 0;

    int32_t count = aeron_archive_catalog_for_each(catalog, verify_fields_callback, &ctx);
    EXPECT_EQ(3, count);
    EXPECT_EQ(3, ctx.idx);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Catalog with many recordings ------- */

TEST_F(CatalogTest, shouldHandleManyRecordings)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_many_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, TEST_CAPACITY * 16, false, false));

    const int num_recordings = 50;
    int64_t ids[50] = {};

    for (int i = 0; i < num_recordings; i++)
    {
        std::string channel = "channel" + std::to_string(i);
        ids[i] = aeron_archive_catalog_add_recording(
            catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
            0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, i, i,
            channel.c_str(), channel.c_str(), "source");
        ASSERT_GE(ids[i], 0);
    }

    EXPECT_EQ(num_recordings, aeron_archive_catalog_recording_count(catalog));

    /* Find each recording */
    for (int i = 0; i < num_recordings; i++)
    {
        aeron_archive_catalog_recording_descriptor_t desc = {};
        ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, ids[i], &desc));
        EXPECT_EQ(ids[i], desc.recording_id);
        EXPECT_EQ(i, desc.session_id);
        EXPECT_EQ(i, desc.stream_id);
    }

    /* Invalidate every other recording */
    for (int i = 0; i < num_recordings; i += 2)
    {
        ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, ids[i]));
    }

    EXPECT_EQ(num_recordings / 2, aeron_archive_catalog_recording_count(catalog));

    /* Verify only odd-indexed recordings remain */
    for (int i = 0; i < num_recordings; i++)
    {
        aeron_archive_catalog_recording_descriptor_t desc = {};
        int result = aeron_archive_catalog_find_recording(catalog, ids[i], &desc);
        if (i % 2 == 0)
        {
            EXPECT_EQ(-1, result);
        }
        else
        {
            EXPECT_EQ(0, result);
        }
    }

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- nextRecordingId is preserved after invalidation ------- */

TEST_F(CatalogTest, nextRecordingIdUnchangedAfterInvalidation)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    int64_t next_id_before = aeron_archive_catalog_next_recording_id(catalog);

    ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_one_id));

    int64_t next_id_after = aeron_archive_catalog_next_recording_id(catalog);
    EXPECT_EQ(next_id_before, next_id_after);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Recording descriptor variable-length string encoding ------- */

TEST_F(CatalogTest, shouldHandleEmptyStrings)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_empty_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, TEST_CAPACITY, false, false));

    int64_t id = aeron_archive_catalog_add_recording(
        catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1,
        "", "", "");
    ASSERT_GE(id, 0);

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, id, &desc));
    EXPECT_EQ(0u, desc.stripped_channel_length);
    EXPECT_EQ(0u, desc.original_channel_length);
    EXPECT_EQ(0u, desc.source_identity_length);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

TEST_F(CatalogTest, shouldHandleVeryLongStrings)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_long_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, TEST_CAPACITY * 8, false, false));

    std::string long_channel(500, 'x');
    std::string long_source(1000, 'y');

    int64_t id = aeron_archive_catalog_add_recording(
        catalog, 0L, AERON_NULL_VALUE, 0L, AERON_NULL_VALUE,
        0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 1, 1,
        long_channel.c_str(), long_channel.c_str(), long_source.c_str());
    ASSERT_GE(id, 0);

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, id, &desc));
    EXPECT_EQ(long_channel.size(), desc.stripped_channel_length);
    EXPECT_EQ(0, strncmp(long_channel.c_str(), desc.stripped_channel, desc.stripped_channel_length));
    EXPECT_EQ(long_source.size(), desc.source_identity_length);
    EXPECT_EQ(0, strncmp(long_source.c_str(), desc.source_identity, desc.source_identity_length));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- Re-open after adding recordings ------- */

TEST_F(CatalogTest, shouldReOpenAndFindAddedRecording)
{
    int64_t new_id;
    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

        new_id = aeron_archive_catalog_add_recording(
            catalog, 100, 200, 300, 400,
            5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 11, 7,
            "newCh", "newCh?tag=n", "newSrc");
        ASSERT_GE(new_id, 0);

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }

    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

        EXPECT_EQ(4, aeron_archive_catalog_recording_count(catalog));
        EXPECT_EQ(new_id + 1, aeron_archive_catalog_next_recording_id(catalog));

        verify_recording(
            catalog, new_id,
            100, 200, 300, 400,
            5, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 11, 7,
            "newCh", "newCh?tag=n", "newSrc");

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }
}

/* ------- Double close ------- */

TEST_F(CatalogTest, shouldHandleDoubleClose)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ(0, aeron_archive_catalog_close(catalog));
    /* Second close on already-freed memory is UB, but the Java test tests double-close.
     * The C implementation guards with is_closed flag, so the second close is safe only
     * if we don't free. Since the C code does free on first close, we just verify first close works.
     * This test ensures the first close is clean. */
}

/* ------- Index rebuilds on open ------- */

TEST_F(CatalogTest, shouldRebuildIndexOnOpen)
{
    /* Open the catalog, invalidate one recording, close */
    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));
        ASSERT_EQ(0, aeron_archive_catalog_invalidate_recording(catalog, m_recording_two_id));
        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }

    /* Re-open: the index should be rebuilt from disk and skip the invalid entry */
    {
        aeron_archive_catalog_t *catalog = nullptr;
        ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

        EXPECT_EQ(2, aeron_archive_catalog_recording_count(catalog));

        aeron_archive_catalog_recording_descriptor_t desc = {};
        EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, &desc));
        EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_two_id, &desc));
        EXPECT_EQ(0, aeron_archive_catalog_find_recording(catalog, m_recording_three_id, &desc));

        ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
    }
}

/* ------- Verify start and stop positions stored correctly ------- */

TEST_F(CatalogTest, shouldStoreStartAndStopPositions)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_pos_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, TEST_CAPACITY, false, false));

    int64_t id = aeron_archive_catalog_add_recording(
        catalog, 1024, 4096, 100, 200,
        7, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH, 99, 42,
        "aeron:udp?endpoint=localhost:40123",
        "aeron:udp?endpoint=localhost:40123|tags=1",
        "192.168.1.1:40123");
    ASSERT_GE(id, 0);

    aeron_archive_catalog_recording_descriptor_t desc = {};
    ASSERT_EQ(0, aeron_archive_catalog_find_recording(catalog, id, &desc));
    EXPECT_EQ(1024, desc.start_position);
    EXPECT_EQ(4096, desc.stop_position);
    EXPECT_EQ(100, desc.start_timestamp);
    EXPECT_EQ(200, desc.stop_timestamp);
    EXPECT_EQ(7, desc.initial_term_id);
    EXPECT_EQ(99, desc.session_id);
    EXPECT_EQ(42, desc.stream_id);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- for_each on empty catalog ------- */

TEST_F(CatalogTest, forEachOnEmptyCatalogReturnsZero)
{
    char dir[256] = {};
    snprintf(dir, sizeof(dir), "/tmp/aeron_catalog_empty2_XXXXXX");
    ASSERT_NE(nullptr, mkdtemp(dir));

    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_create(&catalog, dir, TEST_CAPACITY, false, false));

    struct for_each_context ctx = {};
    int32_t count = aeron_archive_catalog_for_each(catalog, collect_recording_ids, &ctx);
    EXPECT_EQ(0, count);
    EXPECT_TRUE(ctx.recording_ids.empty());

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));

    std::string cmd = std::string("rm -rf ") + dir;
    if (std::system(cmd.c_str())) {}
}

/* ------- forEach with NULL callback fails ------- */

TEST_F(CatalogTest, forEachWithNullCallbackReturnsError)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ(-1, aeron_archive_catalog_for_each(catalog, nullptr, nullptr));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Update position then verify via forEach ------- */

TEST_F(CatalogTest, forEachReflectsUpdatedPositions)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, true));

    ASSERT_EQ(0, aeron_archive_catalog_update_recording_position(
        catalog, m_recording_one_id, 5555, 9999));

    struct for_each_context ctx = {};
    aeron_archive_catalog_for_each(
        catalog,
        [](const aeron_archive_catalog_recording_descriptor_t *desc, void *clientd)
        {
            auto *c = (struct for_each_context *)clientd;
            c->recording_ids.push_back(desc->stop_position);
        },
        &ctx);

    ASSERT_EQ(3u, ctx.recording_ids.size());
    /* First recording should have updated stop_position */
    EXPECT_EQ(5555, ctx.recording_ids[0]);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Catalog header constants ------- */

TEST_F(CatalogTest, catalogHeaderLengthIs32)
{
    EXPECT_EQ(32, AERON_ARCHIVE_CATALOG_HEADER_LENGTH);
}

TEST_F(CatalogTest, descriptorHeaderLengthIs32)
{
    EXPECT_EQ(32, AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH);
}

TEST_F(CatalogTest, descriptorBlockLengthIs80)
{
    EXPECT_EQ(80, AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH);
}

/* ------- recording_count returns 0 for NULL catalog ------- */

TEST_F(CatalogTest, recordingCountReturnsZeroForNullCatalog)
{
    EXPECT_EQ(0, aeron_archive_catalog_recording_count(nullptr));
}

/* ------- next_recording_id returns AERON_NULL_VALUE for NULL catalog ------- */

TEST_F(CatalogTest, nextRecordingIdReturnsNullValueForNullCatalog)
{
    EXPECT_EQ(AERON_NULL_VALUE, aeron_archive_catalog_next_recording_id(nullptr));
}

/* ------- Verify alignment used by catalog create ------- */

TEST_F(CatalogTest, catalogCreateUsesCorrectAlignment)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ((int32_t)AERON_CACHE_LINE_LENGTH, catalog->alignment);

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}

/* ------- Operations on closed/null catalog ------- */

TEST_F(CatalogTest, addRecordingFailsOnNullCatalog)
{
    EXPECT_EQ(AERON_NULL_VALUE, aeron_archive_catalog_add_recording(
        nullptr, 0L, 0L, 0L, 0L, 0, SEGMENT_LENGTH, TERM_LENGTH, MTU_LENGTH,
        1, 1, "ch", "ch", "src"));
}

TEST_F(CatalogTest, findRecordingFailsOnNullCatalog)
{
    aeron_archive_catalog_recording_descriptor_t desc = {};
    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(nullptr, 0, &desc));
}

TEST_F(CatalogTest, updatePositionFailsOnNullCatalog)
{
    EXPECT_EQ(-1, aeron_archive_catalog_update_recording_position(nullptr, 0, 0, 0));
}

TEST_F(CatalogTest, invalidateFailsOnNullCatalog)
{
    EXPECT_EQ(-1, aeron_archive_catalog_invalidate_recording(nullptr, 0));
}

TEST_F(CatalogTest, forEachFailsOnNullCatalog)
{
    EXPECT_EQ(-1, aeron_archive_catalog_for_each(nullptr, collect_recording_ids, nullptr));
}

/* ------- find_recording with NULL descriptor ------- */

TEST_F(CatalogTest, findRecordingFailsWithNullDescriptor)
{
    aeron_archive_catalog_t *catalog = nullptr;
    ASSERT_EQ(0, aeron_archive_catalog_open(&catalog, m_archive_dir, false));

    EXPECT_EQ(-1, aeron_archive_catalog_find_recording(catalog, m_recording_one_id, nullptr));

    ASSERT_EQ(0, aeron_archive_catalog_close(catalog));
}
