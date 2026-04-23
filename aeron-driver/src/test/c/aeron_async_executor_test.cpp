// Copyright 2014-2024 Real Logic Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#include <gtest/gtest.h>
#include <queue>

extern "C"
{
#include "aeron_async_executor.h"
}

typedef struct
{
    int some_value;
    int some_other_value;
}
task_clientd_t;

class ExecutorTest : public testing::Test
{
public:
    void SetUp() override
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            throw std::runtime_error("could not init context");
        }

        m_context->async_executor_enabled = be_async();

        if (aeron_async_executor_init(
            &m_executor,
            m_context,
            m_name_resolver,
            "role_name",
            this) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    void TearDown() override
    {
        aeron_async_executor_close(&m_executor);
        aeron_driver_context_close(m_context);
    }

    virtual bool be_async() = 0;

    static int on_execute(void *task_clientd, void *executor_clientd)
    {
        auto *e = (ExecutorTest *)executor_clientd;
        e->m_on_execute_count++;
        return e->m_on_execute(task_clientd, e);
    }

    static void on_complete(int result, int errcode, const char *errmsg, void *task_clientd, void *executor_clientd)
    {
        auto *e = (ExecutorTest *)executor_clientd;
        e->m_on_complete(result, task_clientd, e);
        e->m_on_complete_count++;
    }

    static void on_cancel(void *task_clientd, void *executor_clientd)
    {
    }

protected:
    aeron_driver_context_t *m_context = nullptr;
    aeron_name_resolver_t *m_name_resolver = nullptr;
    aeron_async_executor_t m_executor = {};
    std::function<int(void *, void *)> m_on_execute;
    std::function<int(int, void *, void *)> m_on_complete;
    int m_on_execute_count = 0;
    int m_on_complete_count = 0;
};

class SyncExecutorTest : public ExecutorTest
{
public:
    bool be_async() override
    {
        return false;
    }
};

TEST_F(SyncExecutorTest, shouldExecuteSynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return ++result;
        };

    int result = aeron_async_executor_submit(
        &m_executor,
        SyncExecutorTest::on_execute,
        SyncExecutorTest::on_complete,
        SyncExecutorTest::on_cancel,
        &tcd);

    ASSERT_EQ(result, 0);
    ASSERT_EQ(m_on_execute_count, 1);
    ASSERT_EQ(m_on_complete_count, 1);
    ASSERT_EQ(tcd.some_value, 200);
}

#define TOTAL_TASKS 1000


class AsyncExecutorTest : public ExecutorTest
{
public:
    bool be_async() override
    {
        return true;
    }
};

TEST_F(AsyncExecutorTest, shouldExecuteAsynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;
    tcd.some_other_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_other_value += 50;

            return ++result;
        };

    for (int i = 0; i < TOTAL_TASKS; i++)
    {
        int result = aeron_async_executor_submit(
            &m_executor,
            SyncExecutorTest::on_execute,
            SyncExecutorTest::on_complete,
            SyncExecutorTest::on_cancel,
            &tcd);
        ASSERT_EQ(result, 0);
    }

    int work_count = 0;

    while (m_on_complete_count < TOTAL_TASKS)
    {
        work_count += aeron_async_executor_process_completions(&m_executor, 50);
    }

    ASSERT_EQ(work_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_execute_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_complete_count, TOTAL_TASKS);
    ASSERT_EQ(tcd.some_value, TOTAL_TASKS * 100);
    ASSERT_EQ(tcd.some_other_value, TOTAL_TASKS * 50);
}
