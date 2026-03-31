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

#ifndef AERON_ARCHIVE_CONTROL_SESSION_PROXY_H
#define AERON_ARCHIVE_CONTROL_SESSION_PROXY_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeron_archive_control_response_proxy.h"
#include "aeron_archive_control_session.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Proxy for sending control responses from authenticator callbacks back to
 * the control session. Implements the SessionProxy interface from the Java
 * security layer.
 *
 * Mirrors the Java ControlSessionProxy. Wraps a ControlResponseProxy and
 * a mutable reference to the current ControlSession being authenticated.
 */
typedef struct aeron_archive_control_session_proxy_stct
{
    aeron_archive_control_response_proxy_t *control_response_proxy;
    aeron_archive_control_session_t *control_session;
}
aeron_archive_control_session_proxy_t;

/**
 * Create a control session proxy.
 *
 * @param proxy                  out param for the allocated proxy.
 * @param control_response_proxy the response proxy used to send wire messages.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_proxy_create(
    aeron_archive_control_session_proxy_t **proxy,
    aeron_archive_control_response_proxy_t *control_response_proxy);

/**
 * Close and free the control session proxy.
 *
 * @param proxy the proxy to close. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_proxy_close(aeron_archive_control_session_proxy_t *proxy);

/**
 * Set the current control session being authenticated.
 *
 * @param proxy           the control session proxy.
 * @param control_session the control session to associate.
 */
void aeron_archive_control_session_proxy_set_control_session(
    aeron_archive_control_session_proxy_t *proxy,
    aeron_archive_control_session_t *control_session);

/**
 * Get the session id of the current control session.
 *
 * @param proxy the control session proxy.
 * @return the session id, or -1 if no session is set.
 */
int64_t aeron_archive_control_session_proxy_session_id(
    const aeron_archive_control_session_proxy_t *proxy);

/**
 * Send a challenge to the client as part of the authentication handshake.
 *
 * @param proxy             the control session proxy.
 * @param encoded_challenge the encoded challenge bytes.
 * @param challenge_length  the length of the challenge data.
 * @return true if the challenge was sent and the session transitioned to CHALLENGED.
 */
bool aeron_archive_control_session_proxy_challenge(
    aeron_archive_control_session_proxy_t *proxy,
    const uint8_t *encoded_challenge,
    size_t challenge_length);

/**
 * Authenticate the current control session, sending an OK response and
 * transitioning the session to the AUTHENTICATED state.
 *
 * @param proxy             the control session proxy.
 * @param encoded_principal the encoded principal bytes (may be NULL).
 * @param principal_length  the length of the principal data.
 * @return true if the response was sent and the session transitioned to AUTHENTICATED.
 */
bool aeron_archive_control_session_proxy_authenticate(
    aeron_archive_control_session_proxy_t *proxy,
    const uint8_t *encoded_principal,
    size_t principal_length);

/**
 * Reject the current control session, transitioning it to the REJECTED state.
 *
 * @param proxy the control session proxy.
 */
void aeron_archive_control_session_proxy_reject(
    aeron_archive_control_session_proxy_t *proxy);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_CONTROL_SESSION_PROXY_H */
