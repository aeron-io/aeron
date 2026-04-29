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

#include <functional>
#include <cmath>
#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"

extern "C"
{
#include "aeron_client.h"
#include "aeron_cnc_file_descriptor.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
}

using namespace aeron;

class TerminateTest : public testing::Test
{
public:
    TerminateTest()
    {
        m_driver.start();
    }

    ~TerminateTest() override
    {
        m_driver.stop();
    }

protected:
    EmbeddedMediaDriver m_driver;
};

TEST_F(TerminateTest, shouldShutdownDriver)
{
    char path[AERON_MAX_PATH] = { 0 };
    aeron_cnc_resolve_filename(m_driver.directory(), path, sizeof(path));

    EXPECT_EQ(1, aeron_context_request_driver_termination(
        m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY))) << aeron_errmsg();
}

TEST_F(TerminateTest, shouldRejectTerminationRequestIfTokenExceedsMaxPathValue)
{
    m_driver.stop();

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, AERON_MAX_PATH + 1));
    EXPECT_EQ(EINVAL, aeron_errcode());
}

TEST_F(TerminateTest, shouldRejectTerminationRequestIfCncFileDoesNotExist)
{
    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    m_driver.joinAndClose();

    aeron_delete_file(filename);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(EINVAL, aeron_errcode());
}

TEST_F(TerminateTest, noOpIfCncFileIsEmpty)
{
    aeron_err_clear();
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH / 2;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    EXPECT_EQ(0, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(0, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfCncFileHasWrongMajorVersion)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + AERON_CACHE_LINE_LENGTH;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = aeron_semantic_version_compose(1, 0, 0);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(EINVAL, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfCncFileMinorVersionIsBelowTheCurrentVersion)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + AERON_CACHE_LINE_LENGTH;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = aeron_semantic_version_compose(
        aeron_semantic_version_major(AERON_CNC_VERSION), aeron_semantic_version_minor(AERON_CNC_VERSION) - 1, 0);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(EINVAL, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfCncFileLengthIsInsufficient)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + AERON_CACHE_LINE_LENGTH;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = AERON_CNC_VERSION;

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(EINVAL, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfRingBufferCapacityIsNotAPowerOfTwo)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    size_t to_driver_buffer_length = 100 + AERON_RB_TRAILER_LENGTH;
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + to_driver_buffer_length;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = AERON_CNC_VERSION;
    metadata->to_driver_buffer_length = static_cast<int32_t>(to_driver_buffer_length);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(EINVAL, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfTokenIsLargerThanRingBufferMaxMessageSize)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    size_t to_driver_buffer_length = AERON_MPSC_RB_MIN_CAPACITY + AERON_RB_TRAILER_LENGTH;
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + to_driver_buffer_length;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = AERON_CNC_VERSION;
    metadata->to_driver_buffer_length = static_cast<int32_t>(to_driver_buffer_length);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, 100));
    EXPECT_EQ(EINVAL, aeron_errcode());

    aeron_unmap(&mapped_file);
}

TEST_F(TerminateTest, shouldFailIfDriverCommandRingBufferIsFull)
{
    m_driver.stop();

    char filename[AERON_MAX_PATH] = { 0 };
    EXPECT_GT(aeron_cnc_resolve_filename(m_driver.directory(), filename, sizeof(filename)), 0);

    aeron_delete_file(filename);

    aeron_mapped_file_t mapped_file = {};
    size_t to_driver_buffer_length = AERON_MPSC_RB_MIN_CAPACITY * 128 + AERON_RB_TRAILER_LENGTH;
    mapped_file.length = AERON_CNC_VERSION_AND_META_DATA_LENGTH + to_driver_buffer_length;
    EXPECT_EQ(0, aeron_map_new_file(&mapped_file, filename, true)) << aeron_errmsg();

    auto *metadata = static_cast<aeron_cnc_metadata_t*>(mapped_file.addr);
    metadata->cnc_version = AERON_CNC_VERSION;
    metadata->to_driver_buffer_length = static_cast<int32_t>(to_driver_buffer_length);

    // pretend that command RB is full
    aeron_mpsc_rb_t rb;
    EXPECT_EQ(0, aeron_mpsc_rb_init(&rb, aeron_cnc_to_driver_buffer(metadata), to_driver_buffer_length));
    rb.descriptor->tail_position = static_cast<int64_t>(to_driver_buffer_length - AERON_RB_TRAILER_LENGTH - 1);

    EXPECT_EQ(-1, aeron_context_request_driver_termination(m_driver.directory(), (uint8_t *)TERMINATION_KEY, strlen(TERMINATION_KEY)));
    EXPECT_EQ(-AERON_CLIENT_ERROR_BUFFER_FULL, aeron_errcode());

    aeron_unmap(&mapped_file);
}
