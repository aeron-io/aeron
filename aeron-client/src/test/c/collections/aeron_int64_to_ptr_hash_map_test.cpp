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

#include <gtest/gtest.h>

extern "C"
{
#include "collections/aeron_int64_to_ptr_hash_map.h"
}

class Int64ToPtrHashMapTest : public testing::Test
{
public:
    ~Int64ToPtrHashMapTest() override
    {
        aeron_int64_to_ptr_hash_map_delete(&m_map);
    }

protected:
    static void for_each(void *clientd, int64_t key, void *value)
    {
        auto *t = (Int64ToPtrHashMapTest *)clientd;

        t->m_for_each(key, value);
    }

    static bool remove_if(void *clientd, int64_t key, void *value)
    {
        auto *t = (Int64ToPtrHashMapTest *)clientd;

        return t->m_remove_if(key, value);
    }

    void for_each(const std::function<void(int64_t, void *)> &func)
    {
        m_for_each = func;
        aeron_int64_to_ptr_hash_map_for_each(&m_map, Int64ToPtrHashMapTest::for_each, this);
    }

    void remove_if(const std::function<bool(int64_t, void *)> &func)
    {
        m_remove_if = func;
        aeron_int64_to_ptr_hash_map_remove_if(&m_map, Int64ToPtrHashMapTest::remove_if, this);
    }

    aeron_int64_to_ptr_hash_map_t m_map = {};
    std::function<void(int64_t, void *)> m_for_each;
    std::function<bool(int64_t, void *)> m_remove_if;
};

TEST_F(Int64ToPtrHashMapTest, shouldDoPutAndThenGetOnEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), &value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToPtrHashMapTest, shouldReplaceExistingValueForTheSameKey)
{
    int value = 42, new_value = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&new_value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), &new_value);
    EXPECT_EQ(m_map.size, 1u);
}

TEST_F(Int64ToPtrHashMapTest, shouldGrowWhenThresholdExceeded)
{
    int value = 42, value_at_16 = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);

    for (size_t i = 0; i < 16; i++)
    {
        EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, i, (void *)&value), 0);
    }

    EXPECT_EQ(m_map.resize_threshold, 16u);
    EXPECT_EQ(m_map.capacity, 32u);
    EXPECT_EQ(m_map.size, 16u);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 16, (void *)&value_at_16), 0);

    EXPECT_EQ(m_map.resize_threshold, 32u);
    EXPECT_EQ(m_map.capacity, 64u);
    EXPECT_EQ(m_map.size, 17u);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 16), &value_at_16);
}

TEST_F(Int64ToPtrHashMapTest, shouldHandleCollisionAndThenLinearProbe)
{
    int value = 42, collision_value = 43;
    int64_t key = 7;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 32, 0.5f), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, key, &value), 0);

    auto collision_key = (int64_t)(key + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, collision_key, &collision_value), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, key), &value);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, collision_key), &collision_value);
}

TEST_F(Int64ToPtrHashMapTest, shouldRemoveEntry)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, 0.5f), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_remove(&m_map, 7), &value);
    EXPECT_EQ(m_map.size, 0u);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_get(&m_map, 7), (void *)nullptr);
}

TEST_F(Int64ToPtrHashMapTest, shouldRemoveEntryAndCompactCollisionChain)
{
    int value_12 = 12, value_13 = 13, value_14 = 14, collision_value = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, 0.55f), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 12, (void *)&value_12), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 13, (void *)&value_13), 0);

    auto collision_key = (int64_t)(12 + m_map.capacity);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, collision_key, &collision_value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 14, (void *)&value_14), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_remove(&m_map, 12), &value_12);
}

TEST_F(Int64ToPtrHashMapTest, shouldNotForEachEmptyMap)
{
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, void *value_ptr)
        {
            called++;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64ToPtrHashMapTest, shouldForEachNonEmptyMap)
{
    int value = 42;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, 7, (void *)&value), 0);

    size_t called = 0;
    for_each(
        [&](int64_t key, void *value_ptr)
        {
            EXPECT_EQ(key, 7);
            EXPECT_EQ(value_ptr, &value);
            called++;
        });

    ASSERT_EQ(called, 1u);
}

TEST_F(Int64ToPtrHashMapTest, shouldNotRemoveIfEmptyMap)
{
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    size_t called = 0;
    remove_if(
        [&](int64_t key, void *value_ptr)
        {
            called++;
            return false;
        });

    ASSERT_EQ(called, 0u);
}

TEST_F(Int64ToPtrHashMapTest, shouldRemoveIfNonEmptyMap)
{
    int value0 = 42;
    int value1 = 43;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 8, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    for (int i = 0; i < 100; i++)
    {
        void *value = ((i & 1) == 0) ? &value0 : &value1;
        EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, i, value), 0);
    }

    size_t called = 0;
    remove_if(
        [&](int64_t key, void *value_ptr)
        {
            called++;
            int val = *((int *)value_ptr);
            return (val & 1) == 1;
        });

    ASSERT_EQ(called, 100u);
    ASSERT_EQ(m_map.size, 50u);

    for_each(
        [&](int64_t key, void *value_ptr)
        {
            int val = *((int *)value_ptr);
            EXPECT_EQ(0, val & 1);
            called++;
        });
}

TEST_F(Int64ToPtrHashMapTest, removeIfWorksWhenChainCompactionBringsElementsToEndOfMap)
{
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);

    int value = 1;

    // Check our indices have the same hash, to ensure chained entries will wrap from 15->0
    ASSERT_EQ(aeron_hash(-243406781, 15), 15);
    ASSERT_EQ(aeron_hash(-333209241, 15), 15);

    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, -243406781, &value), 0);
    EXPECT_EQ(aeron_int64_to_ptr_hash_map_put(&m_map, -333209241, &value), 0);

    size_t called = 0;
    remove_if(
        [&](int64_t, void*)
        {
            called++;
            return true;
        });

    ASSERT_EQ(called, 2u);
    ASSERT_EQ(m_map.size, 0u);
}

TEST_F(Int64ToPtrHashMapTest, removeIfDoesNotSkipEntriesAfterCompaction)
{
    int value = 42;
    int32_t num_entries = 1000;
    ASSERT_EQ(aeron_int64_to_ptr_hash_map_init(&m_map, 16, AERON_MAP_DEFAULT_LOAD_FACTOR), 0);
    for (int32_t i = 0; i < num_entries; ++i)
    {
        EXPECT_EQ(0, aeron_int64_to_ptr_hash_map_put(&m_map, i, &value));
    }
    EXPECT_EQ(num_entries, m_map.size);
    EXPECT_EQ(2048, m_map.capacity);

    size_t called = 0;
    remove_if(
        [&](int64_t key, void *value_ptr)
        {
            // std::cout << key << std::endl;
            called++;
            EXPECT_EQ(&value, value_ptr);
            return 0 == (key & 1);
        });

    EXPECT_EQ(num_entries, called);
    EXPECT_EQ(num_entries / 2, m_map.size);

    for (int32_t i = 0; i < num_entries; ++i)
    {
        if (0 == (i & 1))
        {
            EXPECT_EQ(nullptr, aeron_int64_to_ptr_hash_map_get(&m_map, i));
        }
        else
        {
            EXPECT_EQ(&value, aeron_int64_to_ptr_hash_map_get(&m_map, i));
        }
    }
}
