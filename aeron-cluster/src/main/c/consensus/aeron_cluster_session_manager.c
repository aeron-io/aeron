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

#include <string.h>
#include <errno.h>

#include "aeron_cluster_session_manager.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define INITIAL_SESSION_CAPACITY 16

int aeron_cluster_session_manager_create(
    aeron_cluster_session_manager_t **manager,
    int64_t initial_session_id,
    aeron_t *aeron)
{
    aeron_cluster_session_manager_t *m = NULL;
    if (aeron_alloc((void **)&m, sizeof(aeron_cluster_session_manager_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate session manager");
        return -1;
    }

    if (aeron_alloc((void **)&m->sessions,
        INITIAL_SESSION_CAPACITY * sizeof(aeron_cluster_cluster_session_t *)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate sessions array");
        aeron_free(m);
        return -1;
    }

    m->session_count    = 0;
    m->session_capacity = INITIAL_SESSION_CAPACITY;
    m->next_session_id  = initial_session_id;
    m->aeron            = aeron;

    *manager = m;
    return 0;
}

int aeron_cluster_session_manager_close(aeron_cluster_session_manager_t *manager)
{
    if (NULL != manager)
    {
        for (int i = 0; i < manager->session_count; i++)
        {
            aeron_cluster_cluster_session_close_and_free(manager->sessions[i]);
        }
        aeron_free(manager->sessions);
        aeron_free(manager);
    }
    return 0;
}

aeron_cluster_cluster_session_t *aeron_cluster_session_manager_new_session(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    /* Grow if needed */
    if (manager->session_count >= manager->session_capacity)
    {
        int new_cap = manager->session_capacity * 2;
        if (aeron_reallocf((void **)&manager->sessions,
            (size_t)new_cap * sizeof(aeron_cluster_cluster_session_t *)) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to grow sessions array");
            return NULL;
        }
        manager->session_capacity = new_cap;
    }

    aeron_cluster_cluster_session_t *session = NULL;
    int64_t session_id = manager->next_session_id++;

    if (aeron_cluster_cluster_session_create(
        &session, session_id, correlation_id,
        response_stream_id, response_channel,
        encoded_principal, principal_length,
        manager->aeron) < 0)
    {
        return NULL;
    }

    manager->sessions[manager->session_count++] = session;
    return session;
}

aeron_cluster_cluster_session_t *aeron_cluster_session_manager_find(
    aeron_cluster_session_manager_t *manager, int64_t session_id)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        if (manager->sessions[i]->id == session_id)
        {
            return manager->sessions[i];
        }
    }
    return NULL;
}

int aeron_cluster_session_manager_remove(
    aeron_cluster_session_manager_t *manager, int64_t session_id)
{
    for (int i = 0; i < manager->session_count; i++)
    {
        if (manager->sessions[i]->id == session_id)
        {
            aeron_cluster_cluster_session_close_and_free(manager->sessions[i]);
            manager->sessions[i] = manager->sessions[--manager->session_count];
            return 0;
        }
    }
    return -1;
}

int aeron_cluster_session_manager_check_timeouts(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t session_timeout_ns,
    aeron_cluster_session_timeout_fn_t on_timeout,
    void *clientd)
{
    int count = 0;
    for (int i = manager->session_count - 1; i >= 0; i--)
    {
        aeron_cluster_cluster_session_t *session = manager->sessions[i];
        if (aeron_cluster_cluster_session_is_timed_out(session, now_ns, session_timeout_ns))
        {
            on_timeout(clientd, session);
            manager->sessions[i] = manager->sessions[--manager->session_count];
            count++;
        }
    }
    return count;
}

int aeron_cluster_session_manager_session_count(aeron_cluster_session_manager_t *manager)
{
    return manager->session_count;
}
