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

#include "aeron_executor.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_atomic.h"

struct aeron_executor_task_stct
{
    aeron_executor_t *executor;
    aeron_executor_task_on_execute_func_t on_execute;
    aeron_executor_task_on_complete_func_t on_complete;
    void *clientd;
    int result;
    aeron_linked_queue_node_t *queue_node;
    volatile bool shutdown;
    int errcode;
    char errmsg[AERON_ERROR_MAX_TOTAL_LENGTH];
};

#define USE_RETURN_QUEUE(_e) (NULL == (_e)->on_execution_complete)

aeron_executor_task_t *aeron_executor_task_acquire(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    void *clientd,
    bool shutdown)
{
    aeron_executor_task_t *task;

    if (aeron_alloc((void **)&task, sizeof(aeron_executor_task_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

    task->executor = executor;
    task->on_execute = on_execute;
    task->on_complete = on_complete;
    task->clientd = clientd;
    task->result = -1;
    task->queue_node = NULL;
    task->shutdown = shutdown;

    return task;
}

void aeron_executor_task_release(aeron_executor_task_t *task)
{
    if (NULL != task)
    {
        aeron_linked_queue_node_delete(task->queue_node);
        aeron_free(task);
    }
}

static void *aeron_executor_dispatch(void *arg)
{
    aeron_executor_t *executor = (aeron_executor_t *)arg;

    aeron_thread_set_name("async-executor");

    aeron_executor_task_t *task;
    aeron_linked_queue_node_t *node;
    bool shutdown = false;

    do
    {
        task = (aeron_executor_task_t *)aeron_blocking_linked_queue_take_ex(&executor->queue, &node);

        if (NULL == task)
        {
            continue;
        }

        shutdown = task->shutdown;

        task->queue_node = node;
        task->result = (NULL == task->on_execute) ? 0 : task->on_execute(task->clientd, executor->clientd);

        if (task->result < 0)
        {
            task->errcode = aeron_errcode();
            memcpy(task->errmsg, aeron_errmsg(), strlen(aeron_errmsg()));
            aeron_err_clear();
        }

        if (USE_RETURN_QUEUE(executor))
        {
            aeron_blocking_linked_queue_offer_ex(&executor->return_queue, task, node);
        }
        else if (shutdown)
        {
            aeron_executor_task_release(task);
        }
        else
        {
            executor->on_execution_complete(task, executor->clientd);
        }
    }
    while (false == shutdown);

    return NULL;
}

int aeron_executor_init(
    aeron_executor_t *executor,
    bool async,
    aeron_executor_on_execution_complete_func_t on_execution_complete,
    void *clientd)
{
    executor->async = async,
    executor->on_execution_complete = on_execution_complete;
    executor->clientd = clientd;

    if (async)
    {
        if (USE_RETURN_QUEUE(executor))
        {
            if (aeron_blocking_linked_queue_init(&executor->return_queue) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }
        }

        if (aeron_blocking_linked_queue_init(&executor->queue) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        aeron_thread_attr_t attr;
        int result;
        if ((result = aeron_thread_attr_init(&attr)) != 0)
        {
            AERON_SET_ERR(result, "%s", "aeron_thread_attr_init failed");
            return -1;
        }

        if ((result = aeron_thread_create(&executor->dispatch_thread, &attr, aeron_executor_dispatch, executor)) != 0)
        {
            AERON_SET_ERR(result, "%s", "aeron_thread_create failed");
            return -1;
        }

    }

    return 0;
}

int aeron_executor_close(aeron_executor_t *executor)
{
    if (!executor->async)
    {
        return 0;
    }

    aeron_executor_task_t *task;

    // enqueue a task with shutdown = true
    task = aeron_executor_task_acquire(executor, NULL, NULL, NULL, true);
    if (NULL == task)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_blocking_linked_queue_offer(&executor->queue, task) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int result = aeron_thread_join(executor->dispatch_thread, NULL);
    if (0 != result)
    {
        AERON_SET_ERR(result, "aeron_thread_join: %s", strerror(result));
        return -1;
    }

    if (aeron_blocking_linked_queue_close(&executor->queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (!USE_RETURN_QUEUE(executor))
    {
        // we're done
        return 0;
    }

    // at this point, the executor thread has already been joined, so the shutdown task must already be on the return queue
    aeron_linked_queue_node_t *node;
    bool shutdown = false;

    // theoretically, if the executor was empty when _close was called, we should only go through this loop one time
    do
    {
        // retrieve the node so that it can be deleted when the task is released
        task = aeron_blocking_linked_queue_poll_ex(&executor->return_queue, &node);

        if (NULL == task)
        {
            continue;
        }

        shutdown = task->shutdown;

        aeron_executor_task_release(task);
    }
    while (shutdown == false);

    if (aeron_blocking_linked_queue_close(&executor->return_queue) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_executor_submit(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    void *clientd)
{

    if (executor->async)
    {
        aeron_executor_task_t *task;

        task = aeron_executor_task_acquire(executor, on_execute, on_complete, clientd, false);
        if (NULL == task)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        return aeron_blocking_linked_queue_offer(&executor->queue, task);
    }

    /* not async, so just run execute and complete back to back */
    int result = on_execute(clientd, executor->clientd);

    /* error handling must be done inside the on_complete function */
    on_complete(
        result,
        aeron_errcode(),
        aeron_errmsg(),
        clientd,
        executor->clientd);

    return 0;
}

int aeron_executor_process_completions(aeron_executor_t *executor, int limit)
{
    aeron_executor_task_t *task;
    aeron_linked_queue_node_t *node;
    int count = 0;

    if (!executor->async || !USE_RETURN_QUEUE(executor))
    {
        return 0;
    }

    for (; count < limit; count++)
    {
        task = aeron_blocking_linked_queue_poll_ex(&executor->return_queue, &node);

        if (NULL == task)
        {
            break;
        }

        aeron_executor_task_do_complete(task);
    }

    return count;
}

void aeron_executor_task_do_complete(aeron_executor_task_t *task)
{
    task->on_complete(
        task->result,
        task->errcode,
        task->errmsg,
        task->clientd,
        task->executor->clientd);

    aeron_executor_task_release(task);
}
