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

#include <array>
#include <gtest/gtest.h>

extern "C"
{
#include "aeron_ipc_publication.h"
#include "aeron_driver_sender.h"
#include "aeron_position.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)
#define TERM_BUFFER_LENGTH UINT32_C(64 * 1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

typedef struct log_buffer_info_stct
{
    aeron_mapped_raw_log_t mapped_raw_log;
    char log_file_name[AERON_MAX_PATH];
}
log_buffer_info_t;

class IpcPublicationTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_driver_context_init(&m_context);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);

        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            &m_cached_clock,
            1000);

        aeron_system_counters_init(&m_system_counters, &m_counters_manager);

        aeron_distinct_error_log_init(
            &m_error_log, m_error_log_buffer.data(), m_error_log_buffer.size(), aeron_epoch_clock);

        m_context->error_log = &m_error_log;
        m_context->error_buffer = m_error_log_buffer.data();
        m_context->error_buffer_length = m_error_log_buffer.size();

        aeron_driver_ensure_dir_is_recreated(m_context);
    }

    void TearDown() override
    {
        for (auto publication : m_publications)
        {
            aeron_ipc_publication_close(&m_counters_manager, publication);
        }

        for (auto log_buffer : m_log_buffers)
        {
            m_context->raw_log_free_func(&log_buffer->mapped_raw_log, log_buffer->log_file_name);
            aeron_free(log_buffer);
        }

        aeron_system_counters_close(&m_system_counters);
        aeron_counters_manager_close(&m_counters_manager);
        aeron_distinct_error_log_close(&m_error_log);
        aeron_driver_context_close(m_context);
    }

    aeron_ipc_publication_t *createPublication(const char *uri)
    {
        int64_t registration_id = 1;
        int32_t stream_id = 10;
        int32_t session_id = 10;
        size_t uri_length = strlen(uri);

        aeron_position_t pub_pos_position;
        aeron_position_t pub_lmt_position;

        int64_t client_id = 42;
        bool is_exclusive = false;
        bool is_sparse = true;

        pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager,
            client_id,
            registration_id,
            session_id,
            stream_id,
            uri_length,
            uri,
            is_exclusive);
        pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
            &m_counters_manager, client_id, registration_id, session_id, stream_id, uri_length, uri);

        if (pub_pos_position.counter_id < 0 || pub_lmt_position.counter_id < 0)
        {
            return nullptr;
        }

        pub_pos_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_pos_position.counter_id);
        pub_lmt_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_lmt_position.counter_id);

        log_buffer_info_t *log_buffer_info;
        if (aeron_alloc((void **)&log_buffer_info, sizeof(log_buffer_info_t)) < 0)
        {
            return nullptr;
        }

        int path_length = aeron_ipc_publication_location(
            log_buffer_info->log_file_name,
            sizeof(log_buffer_info->log_file_name),
            m_context->aeron_dir,
            registration_id);
        if (path_length < 0)
        {
            aeron_free(log_buffer_info);
            return nullptr;
        }

        if (m_context->raw_log_map_func(
            &log_buffer_info->mapped_raw_log,
            log_buffer_info->log_file_name,
            is_sparse,
            TERM_BUFFER_LENGTH,
            m_context->file_page_size) < 0)
        {
            aeron_free(log_buffer_info);
            return nullptr;
        }

        m_log_buffers.push_back(log_buffer_info);

        aeron_driver_uri_publication_params_t params = {};
        params.term_length = TERM_BUFFER_LENGTH;
        params.is_sparse = is_sparse;

        aeron_ipc_publication_t *publication = nullptr;
        if (aeron_ipc_publication_create(
            &publication,
            m_context,
            session_id,
            stream_id,
            registration_id,
            &pub_pos_position,
            &pub_lmt_position,
            0,
            &params,
            is_exclusive,
            &m_system_counters,
            uri_length,
            uri,
            &log_buffer_info->mapped_raw_log,
            static_cast<size_t>(path_length),
            log_buffer_info->log_file_name,
            false) < 0)
        {
            return nullptr;
        }

        m_publications.push_back(publication);

        return publication;
    }

    aeron_driver_context_t *m_context = nullptr;
private:
    aeron_clock_cache_t m_cached_clock = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_system_counters_t m_system_counters = {};
    aeron_distinct_error_log_t m_error_log = {};
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_t m_error_log_buffer, 16) = {};
    std::vector<aeron_ipc_publication_t *> m_publications;
    std::vector<log_buffer_info_t *> m_log_buffers;
};

TEST_F(IpcPublicationTest, shouldCreatePublication)
{
    auto channel = "aeron:ipc?alias=test|mtu=2048";
    auto channel_length = strlen(channel);

    aeron_ipc_publication_t *publication = createPublication(channel);

    ASSERT_NE(nullptr, publication) << aeron_errmsg();
    EXPECT_EQ(static_cast<int32_t>(channel_length), publication->channel_length);
    EXPECT_EQ(0, strncmp(channel, publication->channel, channel_length));
    EXPECT_FALSE(publication->is_exclusive);
}

