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

#ifndef AERON_EXECUTOR_H
#define AERON_EXECUTOR_H

#include "concurrent/aeron_blocking_linked_queue.h"

typedef struct aeron_executor_task_stct aeron_executor_task_t;

typedef int (*aeron_executor_on_execution_complete_func_t)(aeron_executor_task_t *task, void *executor_clientd);

typedef struct aeron_executor_stct
{
    bool async;
    aeron_executor_on_execution_complete_func_t on_execution_complete;
    void *clientd;
    aeron_blocking_linked_queue_t queue;
    aeron_blocking_linked_queue_t return_queue;
    aeron_thread_t dispatch_thread;
}
aeron_executor_t;

int aeron_executor_init(
    aeron_executor_t *executor,
    bool async,
    aeron_executor_on_execution_complete_func_t on_execution_complete,
    void *clientd);

int aeron_executor_close(aeron_executor_t *executor);

typedef int (*aeron_executor_task_on_execute_func_t)(void *task_clientd, void *executor_clientd);

/* void return type means error handling must complete inside this function */
typedef void (*aeron_executor_task_on_complete_func_t)(int execution_result, int errcode, const char *errmsg, void *task_clientd, void *executor_clientd);

int aeron_executor_submit(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    void *clientd);

int aeron_executor_process_completions(aeron_executor_t *executor, int limit);

void aeron_executor_task_do_complete(aeron_executor_task_t *task);

#endif //AERON_EXECUTOR_H
