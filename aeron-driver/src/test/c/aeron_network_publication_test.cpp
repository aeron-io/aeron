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
#include "aeron_network_publication.h"
#include "media/aeron_send_channel_endpoint.h"
#include "aeron_test_udp_bindings.h"
#include "aeron_driver_sender.h"
#include "aeron_position.h"

int aeron_driver_ensure_dir_is_recreated(aeron_driver_context_t *context);
}

#define CAPACITY (32 * 1024)

typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

class NetworkPublicationTest : public testing::Test
{
protected:
    void SetUp() override
    {
        aeron_test_udp_bindings_load(&m_transport_bindings);
        aeron_default_name_resolver_supplier(&m_resolver, nullptr, nullptr);

        aeron_driver_context_init(&m_context);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_congestioncontrol_supplier(
            m_context, aeron_static_window_congestion_control_strategy_supplier);
        aeron_driver_context_set_udp_channel_transport_bindings(m_context, &m_transport_bindings);

        aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            &m_cached_clock,
            1000);

        aeron_system_counters_init(&m_system_counters, &m_counters_manager);

        aeron_distinct_error_log_init(
            &m_error_log, m_error_log_buffer.data(), m_error_log_buffer.size(), aeron_epoch_clock);

        aeron_driver_sender_init(&m_sender, m_context, &m_system_counters, nullptr);

        m_sender_proxy.sender = &m_sender;
        m_context->sender_proxy = &m_sender_proxy;
        m_context->error_log = &m_error_log;
        m_context->error_buffer = m_error_log_buffer.data();
        m_context->error_buffer_length = m_error_log_buffer.size();

        aeron_driver_ensure_dir_is_recreated(m_context);
    }

    void TearDown() override
    {
        for (auto publication : m_publications)
        {
            aeron_network_publication_close(&m_counters_manager, publication);
            aeron_network_publication_free(publication);
        }

        for (auto endpoint : m_endpoints)
        {
            aeron_send_channel_endpoint_delete(&m_counters_manager, endpoint);
        }

        aeron_driver_sender_on_close(&m_sender);
        aeron_system_counters_close(&m_system_counters);
        aeron_counters_manager_close(&m_counters_manager);
        aeron_distinct_error_log_close(&m_error_log);
        aeron_driver_context_close(m_context);
    }

    aeron_send_channel_endpoint_t *createEndpoint(
            const char *uri, aeron_driver_uri_publication_params_t *params, bool is_exclusive)
    {
        aeron_udp_channel_t *channel = nullptr;
        if (0 != aeron_udp_channel_parse(strlen(uri), uri, &m_resolver, &channel, false))
        {
            return nullptr;
        }

        aeron_driver_conductor_t conductor = {};
        conductor.context = m_context;
        if (aeron_diver_uri_publication_params(&channel->uri, params, &conductor, is_exclusive) < 0)
        {
            return nullptr;
        }
        aeron_send_channel_endpoint_t *endpoint = nullptr;
        if (aeron_send_channel_endpoint_create(&endpoint, channel, params, m_context, &m_counters_manager, 1) < 0)
        {
            return nullptr;
        }
        m_endpoints.push_back(endpoint);

        return endpoint;
    }

    aeron_network_publication_t *createPublication(const char *uri)
    {
        bool is_exclusive = false;
        aeron_driver_uri_publication_params_t params = {};
        params.mtu_length = 1408;
        params.has_mtu_length = true;
        params.term_length = 65536;
        params.has_term_length = true;
        params.publication_window_length = (int32_t)(params.term_length >> 1);
        params.has_publication_window_length = true;

        aeron_send_channel_endpoint_t *endpoint = createEndpoint(uri, &params, is_exclusive);
        if (nullptr == endpoint)
        {
            return nullptr;
        }

        int64_t registration_id = 1;
        int32_t stream_id = 10;
        int32_t session_id = 10;
        size_t uri_length = strlen(uri);

        aeron_position_t pub_pos_position;
        aeron_position_t pub_lmt_position;
        aeron_position_t snd_pos_position;
        aeron_position_t snd_lmt_position;
        aeron_atomic_counter_t snd_bpe_counter;

        pub_pos_position.counter_id = aeron_counter_publisher_position_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);
        pub_lmt_position.counter_id = aeron_counter_publisher_limit_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);
        snd_pos_position.counter_id = aeron_counter_sender_position_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);
        snd_lmt_position.counter_id = aeron_counter_sender_limit_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);
        snd_bpe_counter.counter_id = aeron_counter_sender_bpe_allocate(
            &m_counters_manager, registration_id, session_id, stream_id, uri_length, uri);

        if (pub_pos_position.counter_id < 0 || pub_lmt_position.counter_id < 0 ||
            snd_pos_position.counter_id < 0 || snd_lmt_position.counter_id < 0 ||
            snd_bpe_counter.counter_id < 0)
        {
            return nullptr;
        }

        aeron_counters_manager_counter_owner_id(
            &m_counters_manager, pub_lmt_position.counter_id, 1);

        pub_pos_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_pos_position.counter_id);
        pub_lmt_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, pub_lmt_position.counter_id);
        snd_pos_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, snd_pos_position.counter_id);
        snd_lmt_position.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, snd_lmt_position.counter_id);
        snd_bpe_counter.value_addr = aeron_counters_manager_addr(
            &m_counters_manager, snd_bpe_counter.counter_id);

        if (params.has_position)
        {
            const int64_t initial_position = aeron_logbuffer_compute_position(
                    params.term_id,
                    (int32_t)params.term_offset,
                    (size_t)aeron_number_of_trailing_zeroes((int32_t)params.term_length),
                    params.initial_term_id);

            aeron_counter_set_ordered(pub_pos_position.value_addr, initial_position);
            aeron_counter_set_ordered(pub_lmt_position.value_addr, initial_position);
            aeron_counter_set_ordered(snd_pos_position.value_addr, initial_position);
            aeron_counter_set_ordered(snd_lmt_position.value_addr, initial_position);
        }

        aeron_flow_control_strategy_t *flow_control;
        aeron_unicast_flow_control_strategy_supplier(&flow_control, nullptr, nullptr, nullptr, 0, 0, 0, 0, 0);

        aeron_network_publication_t *publication = nullptr;
        if (aeron_network_publication_create(
            &publication,
            endpoint,
            m_context,
            registration_id,
            session_id,
            stream_id,
            0,
            &pub_pos_position,
            &pub_lmt_position,
            &snd_pos_position,
            &snd_lmt_position,
            &snd_bpe_counter,
            flow_control,
            &params,
            is_exclusive,
            &m_system_counters) < 0)
        {
            aeron_free(flow_control);
            return nullptr;
        }

        m_publications.push_back(publication);

        return publication;
    }

    aeron_driver_context_t *m_context = nullptr;
private:
    aeron_clock_cache_t m_cached_clock = {};
    aeron_udp_channel_transport_bindings_t m_transport_bindings = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_system_counters_t m_system_counters = {};
    aeron_distinct_error_log_t m_error_log = {};
    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_t m_error_log_buffer, 16) = {};
    std::vector<aeron_send_channel_endpoint_t *> m_endpoints;
    std::vector<aeron_network_publication_t *> m_publications;
    aeron_name_resolver_t m_resolver = {};
    aeron_driver_sender_t m_sender = {};
    aeron_driver_sender_proxy_t m_sender_proxy = {};
};

TEST_F(NetworkPublicationTest, shouldSendSetupMessageInitially)
{
    aeron_network_publication_t *publication = createPublication("aeron:udp?endpoint=localhost:23245");
    ASSERT_NE(nullptr, publication) << aeron_errmsg();

    auto *test_bindings_state =
        static_cast<aeron_test_udp_bindings_state_t *>(publication->endpoint->transport.bindings_clientd);

    aeron_network_publication_send(publication, 0);

    ASSERT_EQ(1, test_bindings_state->setup_count);
}

TEST_F(NetworkPublicationTest, shouldSendHeartbeatWhileSendingPeriodicSetups)
{
    int64_t time_ns = 0;

    aeron_driver_conductor_t conductor = {};
    aeron_driver_conductor_proxy_t proxy = {};
    proxy.conductor = &conductor;
    proxy.threading_mode = AERON_THREADING_MODE_INVOKER;

    aeron_network_publication_t *publication = createPublication("aeron:udp?endpoint=localhost:23245");
    ASSERT_NE(nullptr, publication) << aeron_errmsg();

    auto *test_bindings_state =
        static_cast<aeron_test_udp_bindings_state_t *>(publication->endpoint->transport.bindings_clientd);

    AERON_DECL_ALIGNED(buffer_t data_buffer, 16) = {};
    sockaddr_storage sockaddr = {};

    aeron_network_publication_on_status_message(
        publication, &proxy, data_buffer.data(), sizeof(aeron_status_message_header_t), &sockaddr);
    aeron_network_publication_send(publication, time_ns);

    ASSERT_TRUE(publication->has_receivers);
    ASSERT_EQ(1, test_bindings_state->heartbeat_count);

    time_ns += (AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS + 10);

    aeron_network_publication_send(publication, time_ns);
    ASSERT_EQ(2, test_bindings_state->heartbeat_count);

    time_ns += (AERON_NETWORK_PUBLICATION_SETUP_TIMEOUT_NS + 10);

    aeron_network_publication_trigger_send_setup_frame(
        publication, data_buffer.data(), sizeof(aeron_status_message_header_t), &sockaddr);
    aeron_network_publication_send(publication, time_ns);
    ASSERT_EQ(1, test_bindings_state->setup_count);
    ASSERT_EQ(3, test_bindings_state->heartbeat_count);
}

TEST_F(NetworkPublicationTest, shouldReturnStorageSpaceErrorIfNotEnoughStorageSpaceAvailable)
{
    m_context->usable_fs_space_func = [](const char* path) -> uint64_t
    {
        return 190;
    };
    m_context->perform_storage_checks = true;

    aeron_network_publication_t *publication = createPublication("aeron:udp?endpoint=localhost:23245|term-length=1m");

    ASSERT_EQ(nullptr, publication) << aeron_errmsg();
    EXPECT_EQ(-AERON_ERROR_CODE_STORAGE_SPACE, aeron_errcode());
    auto expected_error_text =
        std::string("insufficient usable storage for new log of length=3149824 usable=190 in ")
            .append(m_context->aeron_dir);
    const auto error_text = std::string(aeron_errmsg());
    EXPECT_NE(std::string::npos, error_text.find(expected_error_text));
}

TEST_F(NetworkPublicationTest, shouldWarnIfRemainingStorageSpaceIsLow)
{
    m_context->usable_fs_space_func = [](const char *path) -> uint64_t
    {
        return 1048576;
    };
    m_context->low_file_store_warning_threshold = 4194304ULL;
    m_context->perform_storage_checks = true;

    aeron_network_publication_t *publication = createPublication("aeron:udp?endpoint=localhost:23245|term-length=128k");

    ASSERT_NE(nullptr, publication) << aeron_errmsg();
    EXPECT_EQ(0, aeron_errcode());
    auto errors_list = m_context->error_log->observation_list;
    EXPECT_NE(nullptr, errors_list);
    EXPECT_NE(0, errors_list->num_observations);
    auto last_error = errors_list->observations[errors_list->num_observations - 1];
    EXPECT_EQ(-AERON_ERROR_CODE_STORAGE_SPACE, last_error.error_code);
    auto error_text = std::string(last_error.description);
    EXPECT_EQ(error_text.size(), last_error.description_length);
    auto expected_warning =
        std::string("WARNING: space is running low: threshold=4194304 usable=1048576 in ")
            .append(m_context->aeron_dir);
    EXPECT_NE(std::string::npos, error_text.find(expected_warning));
}

TEST_F(NetworkPublicationTest, shouldCleanDirtyTermBuffersOneTermBehindTheMinConsumerPosition)
{
    const int32_t term_length = 64 * 1024;
    const int32_t publication_window_length = term_length / 2;
    const int32_t initial_term_id = 5;
    const int32_t term_id = 112004;
    const int32_t term_offset = 384;
    const int64_t initial_position = (int64_t)term_length * (term_id - initial_term_id) + term_offset;
    const std::string uri = std::string("aeron:udp?endpoint=localhost:23245")
            .append("|term-length=").append(std::to_string(term_length))
            .append("|init-term-id=").append(std::to_string(initial_term_id))
            .append("|term-id=").append(std::to_string(term_id))
            .append("|term-offset=").append(std::to_string(term_offset));
    aeron_network_publication_t *publication = createPublication(uri.c_str());
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_lmt_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_lmt_position.value_addr));
    ASSERT_EQ(initial_position, publication->conductor_fields.clean_position);

    aeron_driver_conductor_t conductor = {};
    aeron_driver_conductor_proxy_t proxy = {};
    proxy.conductor = &conductor;
    proxy.threading_mode = AERON_THREADING_MODE_INVOKER;
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16) = {};
    sockaddr_storage sockaddr = {};
    aeron_network_publication_on_status_message(
            publication, &proxy, data_buffer.data(), sizeof(aeron_status_message_header_t), &sockaddr);

    ASSERT_TRUE(publication->has_receivers);

    EXPECT_EQ(1, aeron_network_publication_update_pub_pos_and_lmt(publication));

    // initial pub-lmt increase
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position, publication->conductor_fields.clean_position);

    // snd-pos increase less than a term
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 4128);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 4128 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position, publication->conductor_fields.clean_position);

    // snd-pos increase exactly one term
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + term_length);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + term_length + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position, publication->conductor_fields.clean_position);

    // snd-pos increase beyond a term
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + term_length + 192);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + term_length + 192 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position + 192, publication->conductor_fields.clean_position);

    // clean the rest of the first term
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 2 * term_length + 32);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 2 * term_length + 32 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position - term_offset + term_length, publication->conductor_fields.clean_position);

    // snd-pos didn't change => no op
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 2 * term_length + 32 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position - term_offset + term_length, publication->conductor_fields.clean_position);

    // clean the next buffer
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 2 * term_length + 8192);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 2 * term_length + 8192 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position + term_length + 8192, publication->conductor_fields.clean_position);
}

TEST_F(NetworkPublicationTest, publicationLimitShouldNotCrossIntoPreviousTermIfTheEntireTermIsDirty)
{
    const int32_t term_length = 64 * 1024;
    const int32_t publication_window_length = term_length / 2;
    const int32_t initial_term_id = 5;
    const int32_t term_id = 7;
    const int32_t term_offset = 65280;
    const int64_t initial_position = (int64_t)term_length * (term_id - initial_term_id) + term_offset;
    const std::string uri = std::string("aeron:udp?endpoint=localhost:23245")
            .append("|term-length=").append(std::to_string(term_length))
            .append("|init-term-id=").append(std::to_string(initial_term_id))
            .append("|term-id=").append(std::to_string(term_id))
            .append("|term-offset=").append(std::to_string(term_offset));
    aeron_network_publication_t *publication = createPublication(uri.c_str());
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_lmt_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_lmt_position.value_addr));
    ASSERT_EQ(initial_position, publication->conductor_fields.clean_position);

    aeron_driver_conductor_t conductor = {};
    aeron_driver_conductor_proxy_t proxy = {};
    proxy.conductor = &conductor;
    proxy.threading_mode = AERON_THREADING_MODE_INVOKER;
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16) = {};
    sockaddr_storage sockaddr = {};
    aeron_network_publication_on_status_message(
            publication, &proxy, data_buffer.data(), sizeof(aeron_status_message_header_t), &sockaddr);

    ASSERT_TRUE(publication->has_receivers);

    EXPECT_EQ(1, aeron_network_publication_update_pub_pos_and_lmt(publication));

    // pub-lmt can be in the previous term if clean position offset is not zero
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + term_length);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + term_length + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position, publication->conductor_fields.clean_position);

    // pub-lmt cannot be in the previous term if clean position points to the start of the dirty buffer
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 2 * term_length + 64);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + term_length + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position - term_offset + term_length, publication->conductor_fields.clean_position);

    // after cleanup the pub-lmt can move again
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 2 * term_length + 64 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position + term_length + 64, publication->conductor_fields.clean_position);
}

TEST_F(NetworkPublicationTest, publicationLimitShouldNotCrossIntoTheDirtyTerm)
{
    const int32_t term_length = 64 * 1024;
    const int32_t publication_window_length = term_length / 2;
    const int64_t initial_position = 0;
    const std::string uri = std::string("aeron:udp?endpoint=localhost:23245")
            .append("|term-length=").append(std::to_string(term_length));
    aeron_network_publication_t *publication = createPublication(uri.c_str());
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->pub_lmt_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_pos_position.value_addr));
    ASSERT_EQ(initial_position, aeron_counter_get(publication->snd_lmt_position.value_addr));
    ASSERT_EQ(initial_position, publication->conductor_fields.clean_position);

    aeron_driver_conductor_t conductor = {};
    aeron_driver_conductor_proxy_t proxy = {};
    proxy.conductor = &conductor;
    proxy.threading_mode = AERON_THREADING_MODE_INVOKER;
    AERON_DECL_ALIGNED(buffer_t data_buffer, 16) = {};
    sockaddr_storage sockaddr = {};
    aeron_network_publication_on_status_message(
            publication, &proxy, data_buffer.data(), sizeof(aeron_status_message_header_t), &sockaddr);

    ASSERT_TRUE(publication->has_receivers);

    EXPECT_EQ(1, aeron_network_publication_update_pub_pos_and_lmt(publication));

    // initial pub-lmt
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 256);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 256 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position, publication->conductor_fields.clean_position);

    // new pub-lmt intersects with the clean position
    aeron_counter_set_ordered(publication->snd_pos_position.value_addr, initial_position + 2 * term_length + 192 + publication_window_length);
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 256 + publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position + term_length, publication->conductor_fields.clean_position);

    // after cleanup the pub-lmt can move again
    aeron_network_publication_update_pub_pos_and_lmt(publication);
    EXPECT_EQ(initial_position + 2 * term_length + 192 + 2 * publication_window_length, aeron_counter_get(publication->pub_lmt_position.value_addr));
    EXPECT_EQ(initial_position + term_length + 192 + publication_window_length, publication->conductor_fields.clean_position);
}
