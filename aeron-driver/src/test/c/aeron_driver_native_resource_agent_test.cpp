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
#include "aeron_driver_native_resource_agent.h"
}

#define CAPACITY (32 * 1024)
#define COMMAND_RB_CAPACITY (AERON_COMMAND_RB_CAPACITY + AERON_RB_TRAILER_LENGTH)
typedef std::array<std::uint8_t, COMMAND_RB_CAPACITY> rb_buffer_t;
typedef std::array<std::uint8_t, CAPACITY> buffer_t;
typedef std::array<std::uint8_t, 4 * CAPACITY> buffer_4x_t;

class NativeResourceAgentTest : public testing::Test
{
protected:
    void SetUp()
    {
        m_counter_value_buffer.fill(0);
        m_counter_meta_buffer.fill(0);

        if (aeron_counters_manager_init(
            &m_counters_manager,
            m_counter_meta_buffer.data(), m_counter_meta_buffer.size(),
            m_counter_value_buffer.data(), m_counter_value_buffer.size(),
            &m_cached_clock,
            1000) < 0)
        {
            throw std::logic_error(aeron_errmsg());
        }

        if (aeron_system_counters_init(&m_system_counters, &m_counters_manager) < 0)
        {
            throw std::logic_error(aeron_errmsg());
        }

        if (aeron_spsc_rb_init(&m_context.native_resource_agent_command_queue, &m_command_buffer, COMMAND_RB_CAPACITY) < 0)
        {
            throw std::logic_error(aeron_errmsg());
        }

        m_context.counters_manager = &m_counters_manager;
        m_context.system_counters = &m_system_counters;
        m_context.name_resolver_supplier_func = [](
            aeron_name_resolver_t *resolver,
            const char *args,
            aeron_driver_context_t *context)
        {
            resolver->close_func = [](aeron_name_resolver_t *resolver)
            {
                return 0;
            };
            return 0;
        };

        if (aeron_driver_native_resource_agent_init(&m_native_resource_agent, &m_context) < 0)
        {
            throw std::logic_error(aeron_errmsg());
        }
    }

    void TearDown()
    {
        aeron_driver_native_resource_agent_on_close(&m_native_resource_agent);
        aeron_system_counters_close(&m_system_counters);
        aeron_counters_manager_close(&m_counters_manager);
    }

    AERON_DECL_ALIGNED(buffer_t m_counter_value_buffer, 16) = {};
    AERON_DECL_ALIGNED(buffer_4x_t m_counter_meta_buffer, 16) = {};
    AERON_DECL_ALIGNED(rb_buffer_t m_command_buffer, 16) = {};
    aeron_clock_cache_t m_cached_clock = {};
    aeron_counters_manager_t m_counters_manager = {};
    aeron_system_counters_t m_system_counters = {};
    aeron_driver_context_t m_context = {};
    aeron_driver_native_resource_agent_t m_native_resource_agent = {};
};

TEST_F(NativeResourceAgentTest, shouldDecrementMappedBytesCounterByTheLogSizeIfFreeIsSuccessful)
{
    int64_t *freeFails = aeron_system_counter_addr(&m_system_counters, AERON_SYSTEM_COUNTER_FREE_FAILS);
    int64_t *bytesMapped = aeron_system_counter_addr(&m_system_counters, AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED);
    EXPECT_EQ(0, aeron_counter_get_acquire(freeFails));
    EXPECT_EQ(0, aeron_counter_get_acquire(bytesMapped));

    int64_t mapped_file_length = (int64_t)3 * 1024 * 1024 * 1024 + 8192;
    aeron_counter_set_release(bytesMapped, mapped_file_length);

    aeron_mapped_raw_log_t *mapped_raw_log;
    EXPECT_EQ(0, aeron_alloc((void **)&mapped_raw_log, sizeof(aeron_mapped_raw_log_t)));
    mapped_raw_log->term_length = 1024 * 1024 * 1024;
    mapped_raw_log->mapped_file.length = mapped_file_length;
    mapped_raw_log->mapped_file.addr = nullptr;
    const char *log_file_name = strdup("test.dat");

    m_native_resource_agent.context->raw_log_free_func = [](aeron_mapped_raw_log_t *raw_log, const char *filename)
    {
        raw_log->mapped_file.length = 0; // indicate that the file has been freed
        return true;
    };

    aeron_driver_native_resource_agent_proxy_cmd_free_log_buffer_t cmd = {};
    cmd.mapped_raw_log = mapped_raw_log;
    cmd.log_file_name = log_file_name;
    aeron_driver_native_resource_agent_on_free_log_buffer(&m_native_resource_agent, &cmd);

    EXPECT_EQ(0, aeron_counter_get_acquire(freeFails));
    EXPECT_EQ(0, aeron_counter_get_acquire(bytesMapped));
}
