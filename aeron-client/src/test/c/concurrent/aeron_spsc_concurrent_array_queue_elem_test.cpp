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
#include <atomic>
#include <functional>

#include <gtest/gtest.h>

extern "C"
{
#include "concurrent/aeron_spsc_concurrent_array_queue_elem.h"
}

#define CAPACITY (8u)

struct elem_t
{
    int32_t a;
    int64_t b;
};


class SpscQueueElemTest : public testing::Test
{
public:
    SpscQueueElemTest()
    {
        if (aeron_spsc_concurrent_array_queue_elem_init(&m_q, CAPACITY, sizeof(elem_t)) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    ~SpscQueueElemTest() override
    {
        aeron_spsc_concurrent_array_queue_elem_close(&m_q);
    }

    static void drain_func(void *clientd, void *element)
    {
        auto *t = (SpscQueueElemTest *)clientd;

        (*t).m_drain(element);
    }

    void fillQueue()
    {
        for (size_t i = 1; i <= CAPACITY; i++)
        {
            elem_t elem = { .a = (int32_t) i, .b = (int64_t) i * 7 };
            ASSERT_EQ(aeron_spsc_concurrent_array_queue_elem_offer(&m_q, &elem), AERON_OFFER_SUCCESS);
        }
    }

protected:
    aeron_spsc_concurrent_array_queue_elem_t m_q = {};
    std::function<void(volatile void *)> m_drain;
};

TEST_F(SpscQueueElemTest, shouldGetSizeWhenEmpty)
{
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), 0u);
}

TEST_F(SpscQueueElemTest, shouldReturnErrorWhenNullOffered)
{
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_offer(&m_q, nullptr), AERON_OFFER_ERROR);
}

TEST_F(SpscQueueElemTest, shouldOfferAndDrainToEmptyQueue)
{
    elem_t element = { .a = 1, .b = 2 };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_offer(&m_q, (void *)&element), AERON_OFFER_SUCCESS);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), 1u);

    m_drain =
        [&](volatile void *e)
        {
            elem_t* actual = (elem_t*) e;
            ASSERT_EQ(actual->a, element.a);
            ASSERT_EQ(actual->b, element.b);
        };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_drain(&m_q, SpscQueueElemTest::drain_func, this, UINT64_MAX), 1u);
}

TEST_F(SpscQueueElemTest, shouldFailToOfferToFullQueue)
{
    elem_t element;

    fillQueue();
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), CAPACITY);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_offer(&m_q, (void *)&element), AERON_OFFER_FULL);
}

TEST_F(SpscQueueElemTest, shouldDrainSingleElementFromFullQueue)
{
    fillQueue();

    m_drain =
        [&](volatile void *e)
        {
            elem_t* element = (elem_t*) e;
            ASSERT_EQ(element->a, 1);
            ASSERT_EQ(element->b, 7);
        };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_drain(&m_q, SpscQueueElemTest::drain_func, this, 1), 1u);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), CAPACITY - 1);
}

TEST_F(SpscQueueElemTest, shouldDrainNothingFromEmptyQueue)
{
    m_drain =
        [&](volatile void *e)
        {
            FAIL();
        };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_drain(&m_q, SpscQueueElemTest::drain_func, this, UINT64_MAX), 0u);
}

TEST_F(SpscQueueElemTest, shouldDrainFullQueue)
{
    fillQueue();

    int64_t counter = 1;
    m_drain =
        [&](volatile void *e)
        {
            elem_t* element = (elem_t*) e;
            ASSERT_EQ(element->a, counter);
            ASSERT_EQ(element->b, counter * 7);
            counter++;
        };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_drain(&m_q, SpscQueueElemTest::drain_func, this, UINT64_MAX), CAPACITY);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), 0u);
}

TEST_F(SpscQueueElemTest, shouldDrainingFullQueueWithLimit)
{
    size_t limit = CAPACITY / 2;
    fillQueue();

    int64_t counter = 1;
    m_drain =
        [&](volatile void *e)
        {
            elem_t* element = (elem_t*) e;
            ASSERT_EQ(element->a, counter);
            ASSERT_EQ(element->b, counter * 7);
            counter++;
        };

    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_drain(&m_q, SpscQueueElemTest::drain_func, this, limit), limit);
    EXPECT_EQ(aeron_spsc_concurrent_array_queue_elem_size(&m_q), CAPACITY - limit);
}

#define NUM_MESSAGES (10 * 1000 * 1000)

static void spsc_queue_concurrent_elem_handler(void *clientd, void *element)
{
    auto *counts = (size_t *)clientd;
    auto elem = (elem_t*)element;

    EXPECT_EQ(++(*counts), (size_t)elem->b);
}

TEST(SpscQueueElemConcurrentTest, shouldExchangeMessages)
{
    aeron_spsc_concurrent_array_queue_elem_t q;
    ASSERT_EQ(aeron_spsc_concurrent_array_queue_elem_init(&q, CAPACITY, sizeof(elem_t)), 0);

    std::atomic<int> countDown(1);

    std::vector<std::thread> threads;
    size_t msgCount = 0;
    size_t counts = 0;

    threads.push_back(std::thread(
        [&]()
        {
            countDown--;
            while (countDown > 0)
            {
                std::this_thread::yield();
            }

            for (uint64_t m = 1; m <= NUM_MESSAGES; m++)
            {
                elem_t e = { .b = (int64_t) m };
                while (AERON_OFFER_SUCCESS != aeron_spsc_concurrent_array_queue_elem_offer(&q, (void *)&e))
                {
                    std::this_thread::yield();
                }
            }
        }));

    while (msgCount < NUM_MESSAGES)
    {
        const size_t drainCount = aeron_spsc_concurrent_array_queue_elem_drain_all(
            &q, spsc_queue_concurrent_elem_handler, &counts);

        if (0 == drainCount)
        {
            std::this_thread::yield();
        }

        msgCount += drainCount;
    }

    for (std::thread &t: threads)
    {
        if (t.joinable())
        {
            t.join();
        }
    }

    aeron_spsc_concurrent_array_queue_elem_close(&q);
}
