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

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_control_session_proxy.h"

int aeron_archive_control_session_proxy_create(
    aeron_archive_control_session_proxy_t **proxy,
    aeron_archive_control_response_proxy_t *control_response_proxy)
{
    aeron_archive_control_session_proxy_t *_proxy = NULL;

    if (aeron_alloc((void **)&_proxy, sizeof(aeron_archive_control_session_proxy_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_session_proxy_t");
        return -1;
    }

    _proxy->control_response_proxy = control_response_proxy;
    _proxy->control_session = NULL;

    *proxy = _proxy;

    return 0;
}

int aeron_archive_control_session_proxy_close(aeron_archive_control_session_proxy_t *proxy)
{
    if (NULL != proxy)
    {
        aeron_free(proxy);
    }

    return 0;
}

void aeron_archive_control_session_proxy_set_control_session(
    aeron_archive_control_session_proxy_t *proxy,
    aeron_archive_control_session_t *control_session)
{
    proxy->control_session = control_session;
}

int64_t aeron_archive_control_session_proxy_session_id(
    const aeron_archive_control_session_proxy_t *proxy)
{
    if (NULL == proxy->control_session)
    {
        return -1;
    }

    return aeron_archive_control_session_get_id(proxy->control_session);
}

bool aeron_archive_control_session_proxy_challenge(
    aeron_archive_control_session_proxy_t *proxy,
    const uint8_t *encoded_challenge,
    size_t challenge_length)
{
    aeron_archive_control_session_t *session = proxy->control_session;

    const int64_t session_id = aeron_archive_control_session_get_id(session);
    aeron_exclusive_publication_t *publication = aeron_archive_control_session_publication(session);

    if (aeron_archive_control_response_proxy_send_challenge(
        proxy->control_response_proxy,
        session_id,
        session->correlation_id,
        encoded_challenge,
        challenge_length,
        publication))
    {
        aeron_archive_control_session_challenged(session);
        return true;
    }

    return false;
}

bool aeron_archive_control_session_proxy_authenticate(
    aeron_archive_control_session_proxy_t *proxy,
    const uint8_t *encoded_principal,
    size_t principal_length)
{
    aeron_archive_control_session_t *session = proxy->control_session;

    const int64_t session_id = aeron_archive_control_session_get_id(session);
    aeron_exclusive_publication_t *publication = aeron_archive_control_session_publication(session);

    if (aeron_archive_control_response_proxy_send_response(
        proxy->control_response_proxy,
        session_id,
        session->correlation_id,
        session_id,
        AERON_ARCHIVE_CONTROL_RESPONSE_CODE_OK,
        NULL,
        publication))
    {
        aeron_archive_control_session_authenticate(session, encoded_principal, principal_length);
        return true;
    }

    return false;
}

void aeron_archive_control_session_proxy_reject(
    aeron_archive_control_session_proxy_t *proxy)
{
    aeron_archive_control_session_reject(proxy->control_session);
}
