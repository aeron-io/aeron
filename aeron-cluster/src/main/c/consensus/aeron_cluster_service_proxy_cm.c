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
#include "aeron_cluster_service_proxy_cm.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/joinLog.h"
#include "aeron_cluster_client/serviceTerminationPosition.h"
#include "aeron_cluster_client/requestServiceAck.h"

static bool svc_proxy_offer(aeron_cluster_service_proxy_cm_t *proxy, size_t length)
{
    int64_t result;
    do {
        result = aeron_exclusive_publication_offer(proxy->publication, proxy->buffer, length, NULL, NULL);
    } while (AERON_PUBLICATION_BACK_PRESSURED == result || AERON_PUBLICATION_ADMIN_ACTION == result);
    return result > 0;
}

int aeron_cluster_service_proxy_cm_init(
    aeron_cluster_service_proxy_cm_t *proxy,
    aeron_exclusive_publication_t *publication,
    int service_count)
{
    proxy->publication  = publication;
    proxy->service_count = service_count;
    memset(proxy->buffer, 0, sizeof(proxy->buffer));
    return 0;
}

bool aeron_cluster_service_proxy_cm_join_log(
    aeron_cluster_service_proxy_cm_t *proxy,
    int64_t log_position, int64_t max_log_position,
    int32_t member_id, int32_t log_session_id, int32_t log_stream_id,
    bool is_startup, int32_t role, const char *log_channel)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_joinLog msg;
    if (NULL == aeron_cluster_client_joinLog_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr)) { return false; }

    aeron_cluster_client_joinLog_set_logPosition(&msg, log_position);
    aeron_cluster_client_joinLog_set_maxLogPosition(&msg, max_log_position);
    aeron_cluster_client_joinLog_set_memberId(&msg, member_id);
    aeron_cluster_client_joinLog_set_logSessionId(&msg, log_session_id);
    aeron_cluster_client_joinLog_set_logStreamId(&msg, log_stream_id);
    aeron_cluster_client_joinLog_set_isStartup(&msg,
        is_startup ? aeron_cluster_client_booleanType_TRUE : aeron_cluster_client_booleanType_FALSE);
    aeron_cluster_client_joinLog_set_role(&msg, role);

    const char *ch = log_channel != NULL ? log_channel : "";
    aeron_cluster_client_joinLog_put_logChannel(&msg, ch, (uint32_t)strlen(ch));

    return svc_proxy_offer(proxy, aeron_cluster_client_joinLog_encoded_length(&msg));
}

bool aeron_cluster_service_proxy_cm_termination_position(
    aeron_cluster_service_proxy_cm_t *proxy, int64_t log_position)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_serviceTerminationPosition msg;
    if (NULL == aeron_cluster_client_serviceTerminationPosition_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr)) { return false; }

    aeron_cluster_client_serviceTerminationPosition_set_logPosition(&msg, log_position);
    return svc_proxy_offer(proxy, aeron_cluster_client_serviceTerminationPosition_encoded_length(&msg));
}

bool aeron_cluster_service_proxy_cm_request_service_ack(
    aeron_cluster_service_proxy_cm_t *proxy, int64_t log_position)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_requestServiceAck msg;
    if (NULL == aeron_cluster_client_requestServiceAck_wrap_and_apply_header(
        &msg, (char *)proxy->buffer, 0, sizeof(proxy->buffer), &hdr)) { return false; }

    aeron_cluster_client_requestServiceAck_set_logPosition(&msg, log_position);
    return svc_proxy_offer(proxy, aeron_cluster_client_requestServiceAck_encoded_length(&msg));
}
