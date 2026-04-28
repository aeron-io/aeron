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

#include <exception>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "util/aeron_fileutil.h"
#include "util/aeron_error.h"

int aeron_create_file(const char *path, size_t length, bool sparse_file);
}

#ifdef _MSC_VER
#include <io.h>
#else
#include <unistd.h>
#endif

#if defined(AERON_COMPILER_GCC)
#define removeDir remove
#elif defined(AERON_COMPILER_MSVC)
#define removeDir RemoveDirectoryA
#endif

#ifdef _MSC_VER
#define AERON_FILE_SEP_STR "\\"
#else
#define AERON_FILE_SEP_STR "/"
#endif

static bool probe_sparse_file_support(const char *dir)
{
    char probe_path[1024];
    snprintf(probe_path, sizeof(probe_path), "%s%saeron_sparse_probe", dir, AERON_FILE_SEP_STR);
    aeron_delete_file(probe_path); // sweep up any stale file from a prior crashed run
    int fd = aeron_create_file(probe_path, 4096, true);
    if (fd < 0)
    {
        aeron_err_clear();
        return false;
    }
#ifdef _MSC_VER
    _close(fd);
#else
    close(fd);
#endif
    aeron_delete_file(probe_path);
    return true;
}

class FileUtilTest : public testing::Test {
public:
    FileUtilTest() = default;

    static bool s_sparse_supported;

    static void SetUpTestSuite()
    {
        s_sparse_supported = probe_sparse_file_support(".");
    }
};

bool FileUtilTest::s_sparse_supported = false;

#define SKIP_IF_NO_SPARSE_SUPPORT() \
    do { \
        if (!FileUtilTest::s_sparse_supported) \
        { \
            GTEST_SKIP() << "filesystem at cwd does not support sparse files"; \
        } \
    } while (0)

TEST_F(FileUtilTest, rawLogCloseShouldUnmapAndDeleteLogFile)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_close_unused_file.log";
    const size_t file_length = 16384;
    const size_t term_length = 4096;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, term_length, 4096)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));
    EXPECT_EQ(term_length, mapped_raw_log.term_length);
    EXPECT_NE(nullptr, mapped_raw_log.log_meta_data.addr);
    EXPECT_EQ(AERON_LOGBUFFER_META_DATA_LENGTH, mapped_raw_log.log_meta_data.length);
    for (auto &term_buffer : mapped_raw_log.term_buffers)
    {
        EXPECT_NE(nullptr, term_buffer.addr);
        EXPECT_EQ(term_length, term_buffer.length);
    }

    ASSERT_EQ(0, aeron_raw_log_close(&mapped_raw_log, file)) << aeron_errmsg();

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogFreeShouldUnmapAndDeleteLogFile)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_free_unused_file.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(true, aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogCloseShouldNotDeleteFileIfUnmapFails)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_close_unmap_fails.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096)) << aeron_errmsg();
    const auto mapped_addr = mapped_raw_log.mapped_file.addr;
    mapped_raw_log.mapped_file.addr = reinterpret_cast<void *>(-1);

    ASSERT_EQ(-1, aeron_raw_log_close(&mapped_raw_log, file)) << aeron_errmsg();
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    mapped_raw_log.mapped_file.addr = mapped_addr;
    ASSERT_EQ(0, aeron_raw_log_close(&mapped_raw_log, file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogFreeShouldNotDeleteFileIfUnmapFails)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_free_unmap_fails.log";
    const size_t file_length = 16384;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, 4096, 4096)) << aeron_errmsg();
    const auto mapped_addr = mapped_raw_log.mapped_file.addr;
    mapped_raw_log.mapped_file.addr = reinterpret_cast<void *>(-1);

    ASSERT_EQ(false, aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    mapped_raw_log.mapped_file.addr = mapped_addr;
    ASSERT_EQ(true, aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, resolveShouldConcatPaths)
{
    const char *parent = "this_is_the_parent";
    const char *child = "this_is_the_child";
#ifdef _MSC_VER
    const char *expected = "this_is_the_parent\\this_is_the_child";
#else
    const char *expected = "this_is_the_parent/this_is_the_child";
#endif
    char result[AERON_MAX_PATH];

    ASSERT_LT(0, aeron_file_resolve(parent, child, result, sizeof(result))) << aeron_errmsg();
    ASSERT_STREQ(expected, result);
}

TEST_F(FileUtilTest, resolveShouldReportTruncatedPaths)
{
    const char *parent = "this_is_the_parent";
    const char *child = "this_is_the_child";
    char result[10];

    ASSERT_EQ(-1, aeron_file_resolve(parent, child, result, sizeof(result))) << aeron_errmsg();
    ASSERT_EQ(EINVAL, aeron_errcode());
    ASSERT_EQ('\0', result[sizeof(result) - 1]);
}

TEST_F(FileUtilTest, mapNewFileShouldHandleFilesBiggerThan2GB)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_file_t mapped_file = {};
    const char *file = "test_map_new_file_big_size.log";
    const size_t file_length = 3221225472;
    mapped_file.length = file_length;
    ASSERT_EQ(0, aeron_map_new_file(&mapped_file, file, false)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(0, aeron_unmap(&mapped_file)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ(0, remove(file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, mapExistingFileShouldHandleFilesBiggerThan2GB)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_file_t mapped_file = {};
    const char *file = "test_map_existing_file_big_size.log";
    const size_t file_length = 2500000000;
    mapped_file.length = file_length;
    ASSERT_EQ(0, aeron_map_new_file(&mapped_file, file, false)) << aeron_errmsg();
    ASSERT_EQ(0, aeron_unmap(&mapped_file)) << aeron_errmsg();

    ASSERT_EQ(0, aeron_map_existing_file(&mapped_file, file)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(0, aeron_unmap(&mapped_file)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ(0, remove(file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogMapShouldHandleMaxTermBufferLengthAndMaxPageSize)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_raw_log_map_new_file_max_buffer_length.log";
    const size_t file_length = 4294967296;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, AERON_LOGBUFFER_TERM_MAX_LENGTH, AERON_PAGE_MAX_SIZE)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));
    EXPECT_EQ(AERON_LOGBUFFER_TERM_MAX_LENGTH, mapped_raw_log.term_length);
    EXPECT_NE(nullptr, mapped_raw_log.log_meta_data.addr);
    EXPECT_EQ(AERON_LOGBUFFER_META_DATA_LENGTH, mapped_raw_log.log_meta_data.length);
    for (size_t i = 0; i < AERON_LOGBUFFER_PARTITION_COUNT; i++)
    {
        EXPECT_NE(nullptr, mapped_raw_log.term_buffers[i].addr);
        EXPECT_EQ(AERON_LOGBUFFER_TERM_MAX_LENGTH, mapped_raw_log.term_buffers[i].length);
    }

    ASSERT_TRUE(aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogMapExistingShouldHandleMaxTermBufferLength)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_raw_log_map_existing_file_max_buffer_length.log";
    const size_t file_length = 3223322624;
    const size_t term_length = AERON_LOGBUFFER_TERM_MAX_LENGTH;
    const size_t page_size = 2 * 1024 * 1024;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, true, term_length, page_size)) << aeron_errmsg();
    auto logbuffer_metadata = (aeron_logbuffer_metadata_t *)(mapped_raw_log.log_meta_data.addr);
    logbuffer_metadata->term_length = (int32_t)term_length;
    logbuffer_metadata->page_size = (int32_t)page_size;
    ASSERT_EQ(0, aeron_unmap(&mapped_raw_log.mapped_file)) << aeron_errmsg();

    mapped_raw_log = {};
    ASSERT_EQ(0, aeron_raw_log_map_existing(&mapped_raw_log, file, false)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));
    EXPECT_EQ(term_length, mapped_raw_log.term_length);
    EXPECT_NE(nullptr, mapped_raw_log.log_meta_data.addr);
    EXPECT_EQ(AERON_LOGBUFFER_META_DATA_LENGTH, mapped_raw_log.log_meta_data.length);
    logbuffer_metadata = (aeron_logbuffer_metadata_t *)(mapped_raw_log.log_meta_data.addr);
    EXPECT_EQ(term_length, (size_t)logbuffer_metadata->term_length);
    EXPECT_EQ(page_size, (size_t)logbuffer_metadata->page_size);
    for (auto &term_buffer : mapped_raw_log.term_buffers)
    {
        EXPECT_NE(nullptr, term_buffer.addr);
        EXPECT_EQ(term_length, term_buffer.length);
    }

    ASSERT_TRUE(aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, mapNewFileShouldCreateANonSparseFile)
{
    aeron_mapped_file_t mapped_file = {};
    const char *file = "test_map_new_file_non_sparse.log";
    const size_t file_length = 64 * 1024;
    mapped_file.length = file_length;
    ASSERT_EQ(0, aeron_map_new_file(&mapped_file, file, true)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(0, aeron_unmap(&mapped_file)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ(0, remove(file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, rawLogMapShouldCreateNonSparseFile)
{
    aeron_mapped_raw_log_t mapped_raw_log = {};
    const char *file = "test_raw_log_map_non_sparse_file.log";
    const size_t file_length = 3149824;
    const size_t term_length = 1024 * 1024;
    ASSERT_EQ(0, aeron_raw_log_map(&mapped_raw_log, file, false, term_length, AERON_PAGE_MIN_SIZE)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ(file_length, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));
    EXPECT_EQ(term_length, mapped_raw_log.term_length);
    EXPECT_NE(nullptr, mapped_raw_log.log_meta_data.addr);
    EXPECT_EQ(AERON_LOGBUFFER_META_DATA_LENGTH, mapped_raw_log.log_meta_data.length);
    for (auto &term_buffer : mapped_raw_log.term_buffers)
    {
        EXPECT_NE(nullptr, term_buffer.addr);
        EXPECT_EQ(term_length, term_buffer.length);
    }

    ASSERT_TRUE(aeron_raw_log_free(&mapped_raw_log, file)) << aeron_errmsg();

    EXPECT_EQ(nullptr, mapped_raw_log.mapped_file.addr);
    EXPECT_EQ((size_t)0, mapped_raw_log.mapped_file.length);
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, shouldMsyncMappedFile)
{
    SKIP_IF_NO_SPARSE_SUPPORT();
    aeron_mapped_file_t mapped_file = {};
    const char *file = "test.txt";
    const size_t file_length = 1024 * 512;
    mapped_file.length = file_length;
    ASSERT_EQ(0, aeron_map_new_file(&mapped_file, file, false)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ((int64_t)file_length, aeron_file_length(file));

    ASSERT_EQ(0, aeron_msync(mapped_file.addr, mapped_file.length));
    ASSERT_EQ(0, aeron_msync(mapped_file.addr, 1));

    ASSERT_EQ(0, aeron_unmap(&mapped_file)) << aeron_errmsg();

    EXPECT_NE(nullptr, mapped_file.addr);
    EXPECT_EQ(file_length, mapped_file.length);
    EXPECT_EQ(0, remove(file));
    EXPECT_EQ((int64_t)-1, aeron_file_length(file));
}

TEST_F(FileUtilTest, shouldErrorIfMSyncingNonMappedData)
{
    char data[10];
    ASSERT_EQ(-1, aeron_msync(&data, 1));
    ASSERT_NE(0, aeron_errcode());
    ASSERT_NE(std::string(""), aeron_errmsg());
}

TEST_F(FileUtilTest, shouldNotErrorIfAddressIsNull)
{
    ASSERT_EQ(0, aeron_msync(nullptr, 10));
}

TEST_F(FileUtilTest, simpleMkdir)
{
    const char *dirA = "dirA";
    const char *dirB = "dirA" AERON_FILE_SEP_STR "dirB";
    const char *dirC = "dirA" AERON_FILE_SEP_STR "dirNOPE" AERON_FILE_SEP_STR "dirC";

    removeDir("dirA" AERON_FILE_SEP_STR "dirNOPE" AERON_FILE_SEP_STR "dirC");
    removeDir("dirA" AERON_FILE_SEP_STR "dirNOPE");
    removeDir("dirA" AERON_FILE_SEP_STR "dirB");
    removeDir("dirA");

    ASSERT_EQ(0, aeron_mkdir(dirA, S_IRWXU | S_IRWXG | S_IRWXO));
    ASSERT_EQ(0, aeron_mkdir(dirB, S_IRWXU | S_IRWXG | S_IRWXO));
    ASSERT_EQ(-1, aeron_mkdir(dirC, S_IRWXU | S_IRWXG | S_IRWXO));
}

TEST_F(FileUtilTest, recursiveMkdir)
{
    const char *dirW = "dirW";
    const char *dirY = "dirX" AERON_FILE_SEP_STR "dirY";
    const char *dirZ = "dirW" AERON_FILE_SEP_STR "dirX" AERON_FILE_SEP_STR "dirY" AERON_FILE_SEP_STR "dirZ";

    removeDir("dirW" AERON_FILE_SEP_STR "dirX" AERON_FILE_SEP_STR "dirY" AERON_FILE_SEP_STR "dirZ");
    removeDir("dirW" AERON_FILE_SEP_STR "dirX" AERON_FILE_SEP_STR "dirY");
    removeDir("dirW" AERON_FILE_SEP_STR "dirX");
    removeDir("dirW");
    removeDir("dirX" AERON_FILE_SEP_STR "dirY");
    removeDir("dirX");

    ASSERT_EQ(0, aeron_mkdir_recursive(dirW, S_IRWXU | S_IRWXG | S_IRWXO));
    ASSERT_EQ(0, aeron_mkdir_recursive(dirY, S_IRWXU | S_IRWXG | S_IRWXO));
    ASSERT_EQ(0, aeron_mkdir_recursive(dirZ, S_IRWXU | S_IRWXG | S_IRWXO));
}
