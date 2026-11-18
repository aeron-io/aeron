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

#include <array>
#include <cstdint>
#include <thread>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_counters_manager.h"
}

#define FREE_TO_REUSE_TIMEOUT_MS (1000L)

class CountersManagerTest : public testing::Test
{
public:
    CountersManagerTest() = default;

    ~CountersManagerTest() override
    {
        aeron_counters_manager_close(&m_manager);
    }

    void SetUp() override
    {
        m_metadata.fill(0);
        m_values.fill(0);
    }

    int counters_manager_init()
    {
        return aeron_counters_manager_init(
            &m_manager,
            m_metadata.data(),
            m_metadata.size(),
            m_values.data(),
            m_values.size(),
            &m_cached_clock,
            0);
    }

    int counters_manager_with_cool_down_init()
    {
        return aeron_counters_manager_init(
            &m_manager,
            m_metadata.data(),
            m_metadata.size(),
            m_values.data(),
            m_values.size(),
            &m_cached_clock,
            FREE_TO_REUSE_TIMEOUT_MS);
    }

    static const size_t NUM_COUNTERS = 4;
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_METADATA_LENGTH> m_metadata = {};
    std::array<std::uint8_t, NUM_COUNTERS * AERON_COUNTERS_MANAGER_VALUE_LENGTH> m_values = {};
    aeron_counters_manager_t m_manager = {};
    aeron_clock_cache_t m_cached_clock = {};
};

void func_should_never_be_called(
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const uint8_t *label,
    size_t label_length,
    void *clientd)
{
    FAIL();
}

static void test_concurrent_aeron_counter_increment(
    int num_threads, size_t iterations, std::atomic<int>& started_threads, int64_t *addr)
{
    started_threads++;
    while (started_threads < num_threads)
    {
        std::this_thread::yield();
    }

    for (size_t j = 0; j < iterations; j++)
    {
        aeron_counter_increment(addr);
    }
}

static void test_concurrent_aeron_counter_get_and_add(
    int num_threads, size_t iterations, std::atomic<int>& started_threads, int64_t *addr, int64_t value)
{
    started_threads++;
    while (started_threads < num_threads)
    {
        std::this_thread::yield();
    }

    for (size_t j = 0; j < iterations; j++)
    {
        aeron_counter_get_and_add(addr, value);
    }
}

TEST_F(CountersManagerTest, shouldNotIterateOverEmptyCounters)
{
    ASSERT_EQ(counters_manager_init(), 0);

    aeron_counters_reader_foreach_metadata(m_metadata.data(), m_metadata.size(), func_should_never_be_called, nullptr);
}

TEST_F(CountersManagerTest, shouldEnsureAlignmentOfKeyLayout)
{
    ASSERT_EQ(counters_manager_init(), 0);

    size_t counter_key_header_size = (2 * sizeof(int32_t)) + sizeof(int64_t);
    EXPECT_GE(2u * AERON_CACHE_LINE_LENGTH, sizeof(aeron_stream_position_counter_key_layout_t) + counter_key_header_size);
    EXPECT_GE(2u * AERON_CACHE_LINE_LENGTH, sizeof(aeron_channel_endpoint_status_key_layout_t) + counter_key_header_size);
    EXPECT_GE(2u * AERON_CACHE_LINE_LENGTH, sizeof(aeron_local_sockaddr_key_layout_t) + counter_key_header_size);
}

TEST_F(CountersManagerTest, shouldErrorOnAllocatingWhenFull)
{
    ASSERT_EQ(counters_manager_init(), 0);

    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "lab0", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "lab1", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "lab2", 4), 0);
    EXPECT_GE(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "lab3", 4), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "lab4", 4), -1);
}

void func_check_and_remove_from_map(
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const uint8_t *label,
    size_t label_length,
    void *clientd)
{
    auto allocated = reinterpret_cast<std::map<int32_t, std::string> *>(clientd);

    ASSERT_EQ(allocated->at(id), std::string((const char *)label, label_length));
    allocated->erase(allocated->find(id));
}

TEST_F(CountersManagerTest, shouldAllocateIntoEmptyCounters)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };
    std::map<int32_t, std::string> allocated;

    ASSERT_EQ(counters_manager_init(), 0);

    for (auto &label: labels)
    {
        int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, label.c_str(), label.length());

        ASSERT_GE(id, 0);
        allocated[id] = label;
    }

    aeron_counters_reader_foreach_metadata(
        m_metadata.data(), m_metadata.size(), func_check_and_remove_from_map, &allocated);

    ASSERT_TRUE(allocated.empty());
}

TEST_F(CountersManagerTest, shouldRecycleCounterIdWhenFreed)
{
    std::vector<std::string> labels = { "lab0", "lab1", "lab2", "lab3" };

    ASSERT_EQ(counters_manager_init(), 0);

    for (auto &label: labels)
    {
        ASSERT_GE(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, label.c_str(), label.length()), 0);
    }

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, 2), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "newLab2", 7), 2);
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCounters)
{
    ASSERT_EQ(counters_manager_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "def", 3);
    aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "the next label", 14), def);
}

TEST_F(CountersManagerTest, shouldFreeAndNotReuseCountersThatHaveCoolDown)
{
    ASSERT_EQ(counters_manager_with_cool_down_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "def", 3);
    int32_t ghi = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);

    aeron_clock_update_cached_time(&m_cached_clock, FREE_TO_REUSE_TIMEOUT_MS - 1, 0);
    EXPECT_GT(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "the next label", 14), ghi);
}

TEST_F(CountersManagerTest, shouldFreeAndReuseCountersAfterCoolDown)
{
    ASSERT_EQ(counters_manager_with_cool_down_init(), 0);

    aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    int32_t def = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "def", 3);
    aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "ghi", 3);

    ASSERT_EQ(aeron_counters_manager_free(&m_manager, def), 0);

    aeron_clock_update_cached_time(&m_cached_clock, FREE_TO_REUSE_TIMEOUT_MS, 0);
    EXPECT_EQ(aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "the next label", 14), def);
}

TEST_F(CountersManagerTest, shouldStoreAndLoadCounterValue)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    const int64_t value = 7L;
    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    aeron_counter_set_release(addr, value);
    EXPECT_EQ(aeron_counter_get_plain(addr), value);
    EXPECT_EQ(aeron_counter_get_acquire(addr), value);
}

TEST_F(CountersManagerTest, shouldIncrementValueWithVolatileSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    int64_t initial_value = 1010101010101;
    aeron_counter_set_release(addr, initial_value);
    EXPECT_EQ(aeron_counter_get_plain(addr), initial_value);

    const int num_threads = 3;
    const size_t iterations = 777777;
    std::vector<std::thread> threads;
    std::atomic<int> started_threads(0);
    for (int i = 0; i < num_threads; i++)
    {
        std::thread x(test_concurrent_aeron_counter_increment, num_threads, iterations, std::ref(started_threads), addr);
        threads.push_back(std::move(x));
    }

    for (int i = 0; i < num_threads; i++)
    {
        threads[i].join();
    }

    EXPECT_EQ(aeron_counter_get_plain(addr), initial_value + (num_threads * iterations));
}

TEST_F(CountersManagerTest, shouldIncrementValueWithReleaseSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_EQ(aeron_counter_get_plain(addr), 0);

    EXPECT_EQ(aeron_counter_increment_release(addr), 0);
    EXPECT_EQ(aeron_counter_get_plain(addr), 1);

    EXPECT_EQ(aeron_counter_increment_release(addr), 1);
    EXPECT_EQ(aeron_counter_get_plain(addr), 2);
}

TEST_F(CountersManagerTest, shouldIncrementValueWithPlainSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_EQ(aeron_counter_get_plain(addr), 0);

    EXPECT_EQ(aeron_counter_increment_plain(addr), 0);
    EXPECT_EQ(aeron_counter_get_plain(addr), 1);

    EXPECT_EQ(aeron_counter_increment_plain(addr), 1);
    EXPECT_EQ(aeron_counter_get_plain(addr), 2);
}

TEST_F(CountersManagerTest, shouldGetAndAddValueWithVolatileSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    int64_t initial_value = 567;
    aeron_counter_set_release(addr, initial_value);
    EXPECT_EQ(aeron_counter_get_plain(addr), initial_value);

    const int num_threads = 2;
    const size_t iterations = 777777;
    std::atomic<int> started_threads(0);

    const int64_t v1 = 19, v2 = 64;
    std::thread t1(test_concurrent_aeron_counter_get_and_add, num_threads, iterations, std::ref(started_threads), addr, v1);
    std::thread t2(test_concurrent_aeron_counter_get_and_add, num_threads, iterations, std::ref(started_threads), addr, v2);

    t1.join();
    t2.join();

    EXPECT_EQ(aeron_counter_get_plain(addr), initial_value + iterations * v1 + iterations * v2);
}

TEST_F(CountersManagerTest, shouldGetAndAddValueWithReleaseSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_EQ(aeron_counter_get_plain(addr), 0);

    EXPECT_EQ(aeron_counter_get_and_add_release(addr, 5), 0);
    EXPECT_EQ(aeron_counter_get_plain(addr), 5);

    EXPECT_EQ(aeron_counter_get_and_add_release(addr, -2), 5);
    EXPECT_EQ(aeron_counter_get_plain(addr), 3);

    EXPECT_EQ(aeron_counter_get_and_add_release(addr, 10), 3);
    EXPECT_EQ(aeron_counter_get_plain(addr), 13);
}

TEST_F(CountersManagerTest, shouldGetAndAddValueWithPlainSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_EQ(aeron_counter_get_plain(addr), 0);

    EXPECT_EQ(aeron_counter_get_and_add_plain(addr, 5), 0);
    EXPECT_EQ(aeron_counter_get_plain(addr), 5);

    EXPECT_EQ(aeron_counter_get_and_add_plain(addr, -2), 5);
    EXPECT_EQ(aeron_counter_get_plain(addr), 3);

    EXPECT_EQ(aeron_counter_get_and_add_plain(addr, 10), 3);
    EXPECT_EQ(aeron_counter_get_plain(addr), 13);
}

TEST_F(CountersManagerTest, shouldProposeMaxValueWithReleaseSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_EQ(aeron_counter_get_plain(addr), 0);

    EXPECT_TRUE(aeron_counter_propose_max_release(addr, 5));
    EXPECT_EQ(aeron_counter_get_plain(addr), 5);

    EXPECT_FALSE(aeron_counter_propose_max_release(addr, 5));
    EXPECT_EQ(aeron_counter_get_plain(addr), 5);

    EXPECT_FALSE(aeron_counter_propose_max_release(addr, -1));
    EXPECT_EQ(aeron_counter_get_plain(addr), 5);

    EXPECT_TRUE(aeron_counter_propose_max_release(addr, 100));
    EXPECT_EQ(aeron_counter_get_plain(addr), 100);
}

TEST_F(CountersManagerTest, shouldProposeMaxValueWithPlainSemantics)
{
    ASSERT_EQ(counters_manager_init(), 0);

    int32_t id = aeron_counters_manager_allocate(&m_manager, 0, nullptr, 0, "abc", 3);
    ASSERT_GE(id, 0);

    int64_t *addr = aeron_counters_manager_addr(&m_manager, id);

    EXPECT_TRUE(aeron_counter_propose_max_plain(addr, 111));
    EXPECT_EQ(aeron_counter_get_plain(addr), 111);

    EXPECT_FALSE(aeron_counter_propose_max_plain(addr, 0));
    EXPECT_EQ(aeron_counter_get_plain(addr), 111);

    EXPECT_TRUE(aeron_counter_propose_max_plain(addr, 1000));
    EXPECT_EQ(aeron_counter_get_plain(addr), 1000);
}

struct metadata_test_stct
{
    std::string label;
    int32_t type_id;
    int32_t counter_id;
    int64_t key;
};

void func_should_store_metadata(
    int32_t id, int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const uint8_t *label,
    size_t label_length,
    void *clientd)
{
    auto *info = reinterpret_cast<struct metadata_test_stct *>(clientd);
    static size_t times_called = 0;

    ASSERT_LT(times_called, 2u);

    EXPECT_EQ(id, info[times_called].counter_id);
    EXPECT_EQ(type_id, info[times_called].type_id);
    EXPECT_EQ(*(int64_t *)key, info[times_called].key);
    EXPECT_EQ(std::string((const char *)label, label_length), info[times_called].label);
    times_called++;
}

TEST_F(CountersManagerTest, shouldStoreMetaData)
{
    ASSERT_EQ(counters_manager_init(), 0);

    struct metadata_test_stct info[2] =
        {
            {
                "lab0", 333, 0, 777L
            },
            {
                "lab1", 222, 1, 444L
            }
        };

    ASSERT_EQ(
        aeron_counters_manager_allocate(
            &m_manager,
            info[0].type_id,
            (const uint8_t *)&(info[0].key),
            sizeof(info[0].key),
            info[0].label.c_str(),
            info[0].label.length()),
        info[0].counter_id);

    ASSERT_EQ(
        aeron_counters_manager_allocate(
            &m_manager,
            info[1].type_id,
            (const uint8_t *)&(info[1].key),
            sizeof(info[1].key),
            info[1].label.c_str(),
            info[1].label.length()),
        info[1].counter_id);

    aeron_counters_reader_foreach_metadata(m_metadata.data(), m_metadata.size(), func_should_store_metadata, info);
}

TEST_F(CountersManagerTest, shouldMetadataNumberOfCountersNotBeSmallerThanValuesNumberOfCounters)
{
    size_t one_counter_metadata_length = 1 * AERON_COUNTERS_MANAGER_METADATA_LENGTH;
    size_t ten_counters_value_length = 10 * AERON_COUNTERS_MANAGER_VALUE_LENGTH;

    aeron_counters_manager_t temp_manager = {};
    ASSERT_EQ(-1, aeron_counters_manager_init(
        &temp_manager,
        nullptr,
        one_counter_metadata_length,
        nullptr,
        ten_counters_value_length,
        nullptr,
        0));

    aeron_counters_reader_t temp_reader = {};
    ASSERT_EQ(-1, aeron_counters_reader_init(
        &temp_reader,
        nullptr,
        one_counter_metadata_length,
        nullptr,
        ten_counters_value_length));
}
