/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#include <cstdint>
#include <cstring>

#include <gtest/gtest.h>

extern "C"
{
#include "aeronc.h"
#include "aeron_client.h"
#include "aeron_context.h"
#include "aeron_cnc_file_descriptor.h"
#include "aeron_alloc.h"
#include "client/aeron_archive.h"
#include "client/aeron_archive_context.h"
#include "client/aeron_archive_async_connect.h"
}

#define CAPACITY (16 * 1024)
#define TO_DRIVER_RING_BUFFER_LENGTH (CAPACITY + AERON_RB_TRAILER_LENGTH)
#define TO_CLIENTS_BUFFER_LENGTH (CAPACITY + AERON_BROADCAST_BUFFER_TRAILER_LENGTH)
#define COUNTER_VALUES_BUFFER_LENGTH (1024 * 1024)
#define COUNTER_METADATA_BUFFER_LENGTH (AERON_COUNTERS_METADATA_BUFFER_LENGTH(COUNTER_VALUES_BUFFER_LENGTH))
#define ERROR_BUFFER_LENGTH (CAPACITY)
#define FILE_PAGE_SIZE (4 * 1024)

static aeron_t *create_test_aeron_client(aeron_context_t **out_ctx, uint8_t *cnc_buf, size_t cnc_len)
{
    aeron_context_t *ctx = nullptr;
    if (aeron_context_init(&ctx) < 0)
    {
        return nullptr;
    }

    ctx->cnc_map.length = cnc_len;
    ctx->cnc_map.addr = cnc_buf;
    memset(cnc_buf, 0, cnc_len);

    ctx->use_conductor_agent_invoker = true;

    auto *metadata = reinterpret_cast<aeron_cnc_metadata_t *>(cnc_buf);
    metadata->to_driver_buffer_length = static_cast<int32_t>(TO_DRIVER_RING_BUFFER_LENGTH);
    metadata->to_clients_buffer_length = static_cast<int32_t>(TO_CLIENTS_BUFFER_LENGTH);
    metadata->counter_metadata_buffer_length = static_cast<int32_t>(COUNTER_METADATA_BUFFER_LENGTH);
    metadata->counter_values_buffer_length = static_cast<int32_t>(COUNTER_VALUES_BUFFER_LENGTH);
    metadata->error_log_buffer_length = static_cast<int32_t>(ERROR_BUFFER_LENGTH);
    metadata->client_liveness_timeout = INT64_C(5000000000);
    metadata->start_timestamp = 0;
    metadata->pid = 101;
    AERON_SET_RELEASE(metadata->cnc_version, AERON_CNC_VERSION);

    aeron_t *client = nullptr;
    if (aeron_alloc(reinterpret_cast<void **>(&client), sizeof(aeron_t)) < 0)
    {
        aeron_context_close(ctx);
        return nullptr;
    }
    memset(client, 0, sizeof(aeron_t));
    client->context = ctx;
    client->runner.state = AERON_AGENT_STATE_UNUSED;

    if (aeron_client_conductor_init(&client->conductor, ctx) < 0)
    {
        aeron_free(client);
        ctx->cnc_map.addr = nullptr;
        aeron_context_close(ctx);
        return nullptr;
    }

    *out_ctx = ctx;
    return client;
}

static void destroy_test_aeron_client(aeron_t *client, aeron_context_t *ctx)
{
    aeron_client_conductor_on_close(&client->conductor);
    ctx->cnc_map.addr = nullptr;  // prevent munmap on heap-allocated memory
    aeron_context_close(ctx);
    aeron_free(client);
}

/*
 * This test will always "pass"; it must be run with a leak detector
 * (e.g. "leaks" on macOS) to verify there are no leaks after running.
 */
TEST(ArchiveAsyncConnectLeakTest, shouldNotLeakPendingAsyncOpsOnDelete)
{
    const size_t cnc_len = aeron_cnc_computed_length(
        TO_DRIVER_RING_BUFFER_LENGTH +
        TO_CLIENTS_BUFFER_LENGTH +
        COUNTER_VALUES_BUFFER_LENGTH +
        COUNTER_METADATA_BUFFER_LENGTH +
        ERROR_BUFFER_LENGTH,
        FILE_PAGE_SIZE);

    auto cnc_buf = std::unique_ptr<uint8_t[]>(new uint8_t[cnc_len]);

    aeron_context_t *aeron_ctx = nullptr;
    aeron_t *client = create_test_aeron_client(&aeron_ctx, cnc_buf.get(), cnc_len);
    ASSERT_NE(nullptr, client) << aeron_errmsg();

    // Set up archive context that uses our test aeron client.
    // Use control-mode=response on the response channel to skip the session-id negotiation
    // in aeron_archive_context_conclude (which would require a real media driver).
    aeron_archive_context_t *archive_ctx = nullptr;
    ASSERT_EQ(0, aeron_archive_context_init(&archive_ctx)) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_control_request_channel(
        archive_ctx, "aeron:udp?endpoint=localhost:8010")) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_control_response_channel(
        archive_ctx, "aeron:udp?endpoint=localhost:0|control-mode=response")) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_aeron(archive_ctx, client)) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_owns_aeron_client(archive_ctx, false)) << aeron_errmsg();
    ASSERT_EQ(0, aeron_archive_context_set_message_timeout_ns(archive_ctx, 0)) << aeron_errmsg();

    // Create the archive async connect.
    // This will internally create async_add_subscription and async_add_exclusive_publication
    // commands that are processed by the conductor (invoker mode → synchronous).
    aeron_archive_async_connect_t *async = nullptr;
    ASSERT_EQ(0, aeron_archive_async_connect(&async, archive_ctx)) << aeron_errmsg();
    ASSERT_NE(nullptr, async);

    // Poll with an already-expired deadline (timeout_ns was 0).
    // This triggers the timeout → cleanup path, which calls aeron_archive_async_connect_delete.
    // Pending
    aeron_archive_t *archive = nullptr;
    ASSERT_EQ(-1, aeron_archive_async_connect_poll(&archive, async));

    // The original archive_ctx had owns_aeron_client set to false by aeron_archive_async_connect,
    // so closing it won't touch our client.
    aeron_archive_context_close(archive_ctx);

    destroy_test_aeron_client(client, aeron_ctx);
}
