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

#ifndef AERON_CLUSTER_CONFIGURATION_H
#define AERON_CLUSTER_CONFIGURATION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C"
{
#endif


/* -----------------------------------------------------------------------
 * Protocol version — matches AeronCluster.Configuration.PROTOCOL_*_VERSION
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_PROTOCOL_MAJOR_VERSION  0
#define AERON_CLUSTER_PROTOCOL_MINOR_VERSION  3
#define AERON_CLUSTER_PROTOCOL_PATCH_VERSION  0

int32_t aeron_cluster_semantic_version(void);

/* -----------------------------------------------------------------------
 * SBE schema
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_SCHEMA_ID  111

/* -----------------------------------------------------------------------
 * SESSION_HEADER_LENGTH
 *
 *   SBE messageHeader  (templateId 2 + version 2 + blockLength 2 + schemaId 2) = 8
 *   SessionMessageHeader blockLength (leadershipTermId 8 + clusterSessionId 8 + timestamp 8) = 24
 *   Total = 32
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_SESSION_HEADER_LENGTH  32

/* -----------------------------------------------------------------------
 * Template IDs — verified against aeron-cluster-codecs.xml
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_SESSION_MESSAGE_HEADER_TEMPLATE_ID     1   /* egress:  prefix on every app message   */
#define AERON_CLUSTER_SESSION_EVENT_TEMPLATE_ID              2   /* egress:  connect response / error      */
#define AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID    3   /* ingress: sent during handshake         */
#define AERON_CLUSTER_SESSION_CLOSE_REQUEST_TEMPLATE_ID      4   /* ingress: clean session close           */
#define AERON_CLUSTER_SESSION_KEEPALIVE_TEMPLATE_ID          5   /* ingress: periodic keepalive            */
#define AERON_CLUSTER_NEW_LEADER_EVENT_TEMPLATE_ID           6   /* egress:  leader changed                */
#define AERON_CLUSTER_CHALLENGE_TEMPLATE_ID                  7   /* egress:  auth challenge from cluster   */
#define AERON_CLUSTER_CHALLENGE_RESPONSE_TEMPLATE_ID         8   /* ingress: response to challenge         */
#define AERON_CLUSTER_ADMIN_REQUEST_TEMPLATE_ID              26  /* ingress: e.g. snapshot request         */
#define AERON_CLUSTER_ADMIN_RESPONSE_TEMPLATE_ID             27  /* egress:  response to admin request     */

/* -----------------------------------------------------------------------
 * EventCode values from SessionEvent (mirrors Java EventCode enum)
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_EVENT_CODE_OK               0
#define AERON_CLUSTER_EVENT_CODE_ERROR            1
#define AERON_CLUSTER_EVENT_CODE_REDIRECT         2
#define AERON_CLUSTER_EVENT_CODE_AUTHENTICATION_REJECTED  3

/* -----------------------------------------------------------------------
 * AdminRequestType values
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_ADMIN_REQUEST_SNAPSHOT  0

/* -----------------------------------------------------------------------
 * AdminResponseCode values
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_ADMIN_RESPONSE_OK     0
#define AERON_CLUSTER_ADMIN_RESPONSE_ERROR  1

/* -----------------------------------------------------------------------
 * Environment variable names
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_INGRESS_CHANNEL_ENV_VAR          "AERON_CLUSTER_INGRESS_CHANNEL"
#define AERON_CLUSTER_INGRESS_STREAM_ID_ENV_VAR        "AERON_CLUSTER_INGRESS_STREAM_ID"
#define AERON_CLUSTER_INGRESS_ENDPOINTS_ENV_VAR        "AERON_CLUSTER_INGRESS_ENDPOINTS"
#define AERON_CLUSTER_EGRESS_CHANNEL_ENV_VAR           "AERON_CLUSTER_EGRESS_CHANNEL"
#define AERON_CLUSTER_EGRESS_STREAM_ID_ENV_VAR         "AERON_CLUSTER_EGRESS_STREAM_ID"
#define AERON_CLUSTER_MESSAGE_TIMEOUT_ENV_VAR          "AERON_CLUSTER_MESSAGE_TIMEOUT"
#define AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_ENV_VAR   "AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS"
#define AERON_CLUSTER_CLIENT_NAME_ENV_VAR              "AERON_CLUSTER_CLIENT_NAME"

/* -----------------------------------------------------------------------
 * Default values
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_INGRESS_STREAM_ID_DEFAULT        101
#define AERON_CLUSTER_EGRESS_STREAM_ID_DEFAULT         102
#define AERON_CLUSTER_MESSAGE_TIMEOUT_NS_DEFAULT       (INT64_C(5) * 1000 * 1000 * 1000)  /* 5 seconds */
#define AERON_CLUSTER_MESSAGE_RETRY_ATTEMPTS_DEFAULT   3


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_CONFIGURATION_H */
