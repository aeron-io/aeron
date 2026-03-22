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

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include "aeron_cm_context.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_agent.h"

static int ctx_set_str(char **dst, const char *src)
{
    if (NULL != *dst) { aeron_free(*dst); *dst = NULL; }
    if (NULL == src) { return 0; }
    const size_t n = strlen(src) + 1;
    if (aeron_alloc((void **)dst, n) < 0) { return -1; }
    memcpy(*dst, src, n);
    return 0;
}

int aeron_cm_context_init(aeron_cm_context_t **ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_cm_context_init(NULL)");
        return -1;
    }

    aeron_cm_context_t *c = NULL;
    if (aeron_alloc((void **)&c, sizeof(aeron_cm_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate CM context");
        return -1;
    }

    c->aeron              = NULL;
    c->owns_aeron_client  = false;
    if (aeron_default_path(c->aeron_directory_name, sizeof(c->aeron_directory_name)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    c->member_id          = AERON_CM_MEMBER_ID_DEFAULT;
    c->appointed_leader_id = AERON_CM_APPOINTED_LEADER_DEFAULT;
    c->service_count      = AERON_CM_SERVICE_COUNT_DEFAULT;
    c->app_version        = 0;

    c->log_channel         = NULL;
    c->log_stream_id       = AERON_CM_LOG_STREAM_ID_DEFAULT;
    c->ingress_channel     = NULL;
    c->ingress_stream_id   = AERON_CM_INGRESS_STREAM_ID_DEFAULT;
    c->consensus_channel   = NULL;
    c->consensus_stream_id = AERON_CM_CONSENSUS_STREAM_ID_DEFAULT;
    c->control_channel     = NULL;
    c->consensus_module_stream_id = AERON_CM_CONSENSUS_MODULE_STREAM_ID_DEFAULT;
    c->service_stream_id   = AERON_CM_SERVICE_STREAM_ID_DEFAULT;
    c->snapshot_channel    = NULL;
    c->snapshot_stream_id  = AERON_CM_SNAPSHOT_STREAM_ID_DEFAULT;

    c->cluster_members = NULL;
    c->cluster_dir[0]  = '\0';

    c->session_timeout_ns            = AERON_CM_SESSION_TIMEOUT_NS_DEFAULT;
    c->leader_heartbeat_timeout_ns   = AERON_CM_LEADER_HEARTBEAT_TIMEOUT_NS_DEFAULT;
    c->leader_heartbeat_interval_ns  = AERON_CM_LEADER_HEARTBEAT_INTERVAL_NS_DEFAULT;
    c->startup_canvass_timeout_ns    = AERON_CM_STARTUP_CANVASS_TIMEOUT_NS_DEFAULT;
    c->election_timeout_ns           = AERON_CM_ELECTION_TIMEOUT_NS_DEFAULT;
    c->election_status_interval_ns   = AERON_CM_ELECTION_STATUS_INTERVAL_NS_DEFAULT;
    c->termination_timeout_ns        = AERON_CM_TERMINATION_TIMEOUT_NS_DEFAULT;

    c->cluster_clock_ns     = NULL;
    c->cluster_clock_clientd = NULL;
    c->idle_strategy_func   = NULL;
    c->idle_strategy_state  = NULL;
    c->owns_idle_strategy   = false;
    c->error_handler        = NULL;
    c->error_handler_clientd = NULL;

    /* Counters — not set by default */
#define INIT_COUNTER(f) do { c->f.type_id=-1; c->f.value=0; c->f.is_set=false; } while(0)
    INIT_COUNTER(module_state_counter);
    INIT_COUNTER(election_state_counter);
    INIT_COUNTER(election_counter);
    INIT_COUNTER(leadership_term_id_counter);
    INIT_COUNTER(cluster_node_role_counter);
    INIT_COUNTER(commit_position_counter);
    INIT_COUNTER(control_toggle_counter);
    INIT_COUNTER(node_control_toggle_counter);
    INIT_COUNTER(snapshot_counter);
    INIT_COUNTER(timed_out_client_counter);
#undef INIT_COUNTER
    c->max_concurrent_sessions = 0;
    c->agent_role_name[0]      = '\0';

    /* Mark file */
    c->mark_file            = NULL;
    c->owns_mark_file       = false;
    c->mark_file_dir[0]     = '\0';
    c->mark_file_timeout_ms = 5000LL;

    /* Authenticator — NULL = accept all */
    c->authenticate                    = NULL;
    c->on_challenge_response           = NULL;
    c->authenticator_clientd           = NULL;
    c->authenticator_supplier_class_name[0] = '\0';

    /* Apply env vars */
    char *v;

    if ((v = getenv(AERON_DIR_ENV_VAR)))
    { snprintf(c->aeron_directory_name, sizeof(c->aeron_directory_name), "%s", v); }

    if ((v = getenv(AERON_CM_MEMBER_ID_ENV_VAR)))
    { c->member_id = (int32_t)atoi(v); }

    if ((v = getenv(AERON_CM_LOG_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->log_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->log_channel, AERON_CM_LOG_CHANNEL_DEFAULT) < 0) goto error; }

    if ((v = getenv(AERON_CM_INGRESS_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->ingress_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->ingress_channel, AERON_CM_INGRESS_CHANNEL_DEFAULT) < 0) goto error; }

    if ((v = getenv(AERON_CM_CONSENSUS_CHANNEL_ENV_VAR)))
    { if (ctx_set_str(&c->consensus_channel, v) < 0) goto error; }
    else
    { if (ctx_set_str(&c->consensus_channel, AERON_CM_CONSENSUS_CHANNEL_DEFAULT) < 0) goto error; }

    if (ctx_set_str(&c->control_channel, AERON_CM_CONTROL_CHANNEL_DEFAULT) < 0) goto error;
    if (ctx_set_str(&c->snapshot_channel, AERON_CM_SNAPSHOT_CHANNEL_DEFAULT) < 0) goto error;

    if ((v = getenv(AERON_CM_CLUSTER_MEMBERS_ENV_VAR)))
    { if (ctx_set_str(&c->cluster_members, v) < 0) goto error; }

    if ((v = getenv(AERON_CM_CLUSTER_DIR_ENV_VAR)))
    { snprintf(c->cluster_dir, sizeof(c->cluster_dir), "%s", v); }

    if ((v = getenv(AERON_CM_SERVICE_COUNT_ENV_VAR)))
    { c->service_count = atoi(v); }

    c->consensus_stream_id = aeron_config_parse_int32(
        AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR,
        getenv(AERON_CM_CONSENSUS_STREAM_ID_ENV_VAR),
        c->consensus_stream_id, INT32_MIN, INT32_MAX);

    if ((v = getenv(AERON_CM_SESSION_TIMEOUT_ENV_VAR)))
    { c->session_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CM_SESSION_TIMEOUT_ENV_VAR, v, c->session_timeout_ns, 1000, INT64_MAX); }

    if ((v = getenv(AERON_CM_ELECTION_TIMEOUT_ENV_VAR)))
    { c->election_timeout_ns = aeron_config_parse_duration_ns(
        AERON_CM_ELECTION_TIMEOUT_ENV_VAR, v, c->election_timeout_ns, 1000, INT64_MAX); }

    *ctx = c;
    return 0;

error:
    aeron_free(c->log_channel);
    aeron_free(c->ingress_channel);
    aeron_free(c->consensus_channel);
    aeron_free(c->control_channel);
    aeron_free(c->snapshot_channel);
    aeron_free(c->cluster_members);
    aeron_free(c);
    return -1;
}

int aeron_cm_context_close(aeron_cm_context_t *ctx)
{
    if (NULL != ctx)
    {
        if (ctx->owns_aeron_client && NULL != ctx->aeron)
        {
            aeron_context_t *ac = aeron_context(ctx->aeron);
            aeron_close(ctx->aeron);
            aeron_context_close(ac);
        }
        if (ctx->owns_mark_file && NULL != ctx->mark_file)
        {
            aeron_cluster_mark_file_close(ctx->mark_file);
            ctx->mark_file = NULL;
        }
        aeron_free(ctx->log_channel);
        aeron_free(ctx->ingress_channel);
        aeron_free(ctx->consensus_channel);
        aeron_free(ctx->control_channel);
        aeron_free(ctx->snapshot_channel);
        aeron_free(ctx->cluster_members);
        if (ctx->owns_idle_strategy) { aeron_free(ctx->idle_strategy_state); }
        aeron_free(ctx);
    }
    return 0;
}

int aeron_cm_context_conclude(aeron_cm_context_t *ctx)
{
    if (NULL == ctx->cluster_members || '\0' == *ctx->cluster_members)
    {
        AERON_SET_ERR(EINVAL, "%s", "cluster_members is required");
        return -1;
    }

#define CHK_CTR(fld, exp, name) \
    if (ctx->fld.is_set && ctx->fld.type_id != (exp)) { \
        AERON_SET_ERR(EINVAL,"The type for " name " typeId=%d does not match the expected=%d", \
            ctx->fld.type_id,(exp)); return -1; }
    CHK_CTR(module_state_counter,        AERON_CM_COUNTER_CONSENSUS_MODULE_STATE_TYPE_ID, "moduleStateCounter")
    CHK_CTR(election_state_counter,      AERON_CM_COUNTER_ELECTION_STATE_TYPE_ID,         "electionStateCounter")
    CHK_CTR(election_counter,            AERON_CM_COUNTER_ELECTION_COUNT_TYPE_ID,         "electionCounter")
    CHK_CTR(leadership_term_id_counter,  AERON_CM_COUNTER_LEADERSHIP_TERM_ID_TYPE_ID,     "leadershipTermIdCounter")
    CHK_CTR(cluster_node_role_counter,   AERON_CM_COUNTER_NODE_ROLE_TYPE_ID,              "clusterNodeRoleCounter")
    CHK_CTR(commit_position_counter,     AERON_CM_COUNTER_COMMIT_POSITION_TYPE_ID,        "commitPositionCounter")
    CHK_CTR(control_toggle_counter,      AERON_CM_COUNTER_CONTROL_TOGGLE_TYPE_ID,         "controlToggleCounter")
    CHK_CTR(node_control_toggle_counter, AERON_CM_COUNTER_NODE_CONTROL_TOGGLE_TYPE_ID,    "nodeControlToggleCounter")
    CHK_CTR(snapshot_counter,            AERON_CM_COUNTER_SNAPSHOT_TYPE_ID,               "snapshotCounter")
    CHK_CTR(timed_out_client_counter,    AERON_CM_COUNTER_CLIENT_TIMEOUT_TYPE_ID,         "timedOutClientCounter")
#undef CHK_CTR

    /* Generate agent role name if not set */
    if ('\0' == ctx->agent_role_name[0])
    {
        snprintf(ctx->agent_role_name, sizeof(ctx->agent_role_name),
            "consensus-module-%d-%d", 0 /* clusterId placeholder */, ctx->member_id);
    }

    /* Validate startup_canvass_timeout_ns is a multiple of leader_heartbeat_timeout_ns */
    if (ctx->leader_heartbeat_timeout_ns > 0 &&
        ctx->startup_canvass_timeout_ns % ctx->leader_heartbeat_timeout_ns != 0)
    {
        AERON_SET_ERR(EINVAL,
            "startupCanvassTimeoutNs=%lld must be a multiple of leaderHeartbeatTimeoutNs=%lld",
            (long long)ctx->startup_canvass_timeout_ns,
            (long long)ctx->leader_heartbeat_timeout_ns);
        return -1;
    }

    /* Create/check ClusterMarkFile if cluster_dir is set and mark_file not already provided */
    if ('\0' != ctx->cluster_dir[0] && NULL == ctx->mark_file)
    {
        const char *mf_dir = ctx->mark_file_dir[0] != '\0' ? ctx->mark_file_dir : ctx->cluster_dir;

        /* Create mark file dir if needed */
        struct stat st;
        if (stat(mf_dir, &st) < 0)
        {
            if (mkdir(mf_dir, 0755) < 0 && errno != EEXIST)
            {
                AERON_SET_ERR(errno, "failed to create mark file dir: %s", mf_dir);
                return -1;
            }
        }

        char mark_path[AERON_MAX_PATH];
        snprintf(mark_path, sizeof(mark_path), "%s/%s", mf_dir, AERON_CLUSTER_MARK_FILE_FILENAME);

        int64_t now_ms = aeron_nano_clock() / 1000000LL;
        if (aeron_cluster_mark_file_open(
            &ctx->mark_file, mark_path,
            AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE,
            AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN,
            now_ms, ctx->mark_file_timeout_ms) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
        ctx->owns_mark_file = true;

        /* Signal ready immediately */
        aeron_cluster_mark_file_signal_ready(ctx->mark_file, now_ms);

        /* Create symlink in cluster_dir if markFileDir differs */
        if (ctx->mark_file_dir[0] != '\0' &&
            strcmp(ctx->mark_file_dir, ctx->cluster_dir) != 0)
        {
            char link_path[AERON_MAX_PATH];
            snprintf(link_path, sizeof(link_path), "%s/%s",
                ctx->cluster_dir, AERON_CLUSTER_MARK_FILE_LINK_FILENAME);
            /* Remove old link if exists */
            unlink(link_path);
            symlink(ctx->mark_file_dir, link_path);
        }
    }

    if (NULL == ctx->aeron)
    {
        ctx->owns_aeron_client = true;
        aeron_context_t *ac;
        if (aeron_context_init(&ac) < 0 ||
            aeron_context_set_dir(ac, ctx->aeron_directory_name) < 0 ||
            aeron_init(&ctx->aeron, ac) < 0 ||
            aeron_start(ctx->aeron) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }
    }

    if (NULL == ctx->idle_strategy_func)
    {
        ctx->owns_idle_strategy = true;
        ctx->idle_strategy_func = aeron_idle_strategy_load(
            "backoff", &ctx->idle_strategy_state,
            "AERON_CM_IDLE_STRATEGY", NULL);
        if (NULL == ctx->idle_strategy_func) { AERON_APPEND_ERR("%s", ""); return -1; }
    }

    return 0;
}
