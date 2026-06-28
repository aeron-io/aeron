/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

extern "C"
{
#include "util/aeron_dlopen.h"
}

class DlopenTest : public testing::Test
{
};

TEST_F(DlopenTest, shouldFindExistingSymbolWithRtldDefault)
{
    // printf is universally available in the C runtime on all platforms
    void *sym = aeron_dlsym(RTLD_DEFAULT, "printf");
    EXPECT_NE(nullptr, sym);
}

TEST_F(DlopenTest, shouldReturnNullForNonexistentSymbolWithRtldDefault)
{
    void *sym = aeron_dlsym(RTLD_DEFAULT, "aeron_nonexistent_symbol_that_does_not_exist_xyz");
    EXPECT_EQ(nullptr, sym);
}

TEST_F(DlopenTest, shouldLoadLibraryAndFindSymbolByHandle)
{
    void *lib = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib) << "Failed to load test lib A: " << aeron_dlerror();

    typedef int (*func_t)(void);
    void *sym = aeron_dlsym(lib, "aeron_test_lib_a_func");
    ASSERT_NE(nullptr, sym);

    func_t func = (func_t)sym;
    EXPECT_EQ(42, func());
}

TEST_F(DlopenTest, shouldReturnNullForNonexistentSymbolByHandle)
{
    void *lib = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib) << "Failed to load test lib A: " << aeron_dlerror();

    void *sym = aeron_dlsym(lib, "aeron_nonexistent_symbol_xyz");
    EXPECT_EQ(nullptr, sym);
}

TEST_F(DlopenTest, shouldReturnNullForNonexistentLibrary)
{
    void *lib = aeron_dlopen("nonexistent_library_that_does_not_exist.so");
    EXPECT_EQ(nullptr, lib);
}

TEST_F(DlopenTest, shouldFindSymbolFromLoadedLibraryWithRtldDefault)
{
    void *lib = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib) << "Failed to load test lib A: " << aeron_dlerror();

    typedef int (*func_t)(void);
    void *sym = aeron_dlsym(RTLD_DEFAULT, "aeron_test_lib_a_func");
    ASSERT_NE(nullptr, sym);

    func_t func = (func_t)sym;
    EXPECT_EQ(42, func());
}

TEST_F(DlopenTest, shouldFindSymbolNotInFirstModuleWithRtldDefault)
{
    void *lib_b = aeron_dlopen(AERON_DLOPEN_TEST_LIB_B_PATH);
    ASSERT_NE(nullptr, lib_b) << "Failed to load test lib B: " << aeron_dlerror();

    typedef int (*func_t)(void);
    void *sym = aeron_dlsym(RTLD_DEFAULT, "aeron_test_lib_b_func");
    ASSERT_NE(nullptr, sym);

    func_t func = (func_t)sym;
    EXPECT_EQ(99, func());
}

TEST_F(DlopenTest, shouldFindFirstLoadedSymbolWithRtldDefault)
{
    void *lib_a = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib_a) << "Failed to load test lib A: " << aeron_dlerror();

    void *lib_b = aeron_dlopen(AERON_DLOPEN_TEST_LIB_B_PATH);
    ASSERT_NE(nullptr, lib_b) << "Failed to load test lib B: " << aeron_dlerror();

    typedef int (*func_t)(void);

    // Both libs export aeron_test_shared_func; RTLD_DEFAULT searches in load order,
    // so lib_a (loaded first) should be found first
    void *sym = aeron_dlsym(RTLD_DEFAULT, "aeron_test_shared_func");
    ASSERT_NE(nullptr, sym);

    func_t func = (func_t)sym;
    EXPECT_EQ(1, func());
}

// RTLD_NEXT tests are skipped on macOS because its native dlsym(RTLD_NEXT, ...) does not search
// through dlopen'd libraries, only the static dependency chain. On Linux and Windows (our
// implementation), RTLD_NEXT correctly searches all loaded modules after the caller's module.
#if !defined(__APPLE__)
TEST_F(DlopenTest, shouldFindNextSymbolAfterCallerWithRtldNext)
{
    void *lib_a = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib_a) << "Failed to load test lib A: " << aeron_dlerror();

    void *lib_b = aeron_dlopen(AERON_DLOPEN_TEST_LIB_B_PATH);
    ASSERT_NE(nullptr, lib_b) << "Failed to load test lib B: " << aeron_dlerror();

    typedef int (*func_t)(void);

    // RTLD_NEXT searches modules after the caller's module.
    // The test executable is the caller, so it should find lib_a first (loaded before lib_b).
    void *sym = aeron_dlsym(RTLD_NEXT, "aeron_test_shared_func");
    ASSERT_NE(nullptr, sym);

    func_t func = (func_t)sym;
    EXPECT_EQ(1, func());
}

TEST_F(DlopenTest, shouldReturnNullForRtldNextWithNoMatchAfterCaller)
{
    void *sym = aeron_dlsym(RTLD_NEXT, "aeron_nonexistent_symbol_that_does_not_exist_xyz");
    EXPECT_EQ(nullptr, sym);
}
#endif

TEST_F(DlopenTest, shouldNotFindSymbolFromOtherLibByHandle)
{
    void *lib_a = aeron_dlopen(AERON_DLOPEN_TEST_LIB_A_PATH);
    ASSERT_NE(nullptr, lib_a) << "Failed to load test lib A: " << aeron_dlerror();

    void *lib_b = aeron_dlopen(AERON_DLOPEN_TEST_LIB_B_PATH);
    ASSERT_NE(nullptr, lib_b) << "Failed to load test lib B: " << aeron_dlerror();

    // lib_a should not contain lib_b's unique symbol
    void *sym = aeron_dlsym(lib_a, "aeron_test_lib_b_func");
    EXPECT_EQ(nullptr, sym);

    // lib_b should not contain lib_a's unique symbol
    sym = aeron_dlsym(lib_b, "aeron_test_lib_a_func");
    EXPECT_EQ(nullptr, sym);
}
