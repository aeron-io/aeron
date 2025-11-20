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

#include <gtest/gtest.h>

extern "C"
{
#include <stdlib.h>
#include "util/aeron_parse_util.h"
#include "util/aeron_env.h"
#include "aeron_name_resolver.h"
#include "aeron_name_resolver_cache.h"
#include "aeron_driver_name_resolver.h"
#include "aeron_system_counters.h"
#include "agent/aeron_driver_agent.h"
#include "aeron_csv_table_name_resolver.h"
}

#define METADATA_LENGTH (32 * 1024)
#define VALUES_LENGTH (METADATA_LENGTH / 4)
#define ERROR_LOG_LENGTH (8192)

class NameResolverTest : public testing::Test
{
public:
    NameResolverTest()
    {
        m_a.context = nullptr;
        m_b.context = nullptr;
        m_c.context = nullptr;
    }

protected:
    typedef struct resolver_fields_stct
    {
        aeron_driver_context_t *context;
        aeron_name_resolver_t resolver;
        aeron_counters_manager_t counters;
        aeron_counters_reader_t counters_reader;
        aeron_system_counters_t system_counters;
        aeron_distinct_error_log_t error_log;
        uint8_t counters_buffer[METADATA_LENGTH + VALUES_LENGTH];
        uint8_t error_log_buffer[ERROR_LOG_LENGTH];
    }
    resolver_fields_t;

    void SetUp() override
    {
        unresolvable_address = nullptr;
        aeron_err_clear();
    }

    void TearDown() override
    {
        close(&m_a);
        close(&m_b);
        close(&m_c);
    }

    void initResolver(
        resolver_fields_t *resolver_fields,
        const char *resolver_supplier_name,
        const char *args,
        int64_t now_ms,
        const char *driver_resolver_name = nullptr,
        const char *driver_resolver_interface = nullptr,
        const char *driver_bootstrap_neighbour = nullptr,
        const aeron_name_resolver_supplier_func_t name_resolver_bootstrap_supplier_func = nullptr)
    {
        aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(resolver_supplier_name);
        ASSERT_NE(nullptr, supplier_func);

        aeron_driver_context_init(&resolver_fields->context);

        aeron_clock_update_cached_time(resolver_fields->context->cached_clock, now_ms, now_ms + 1000000);
        aeron_driver_context_set_resolver_name(resolver_fields->context, driver_resolver_name);
        aeron_driver_context_set_resolver_interface(resolver_fields->context, driver_resolver_interface);
        aeron_driver_context_set_resolver_bootstrap_neighbor(resolver_fields->context, driver_bootstrap_neighbour);
        resolver_fields->context->driver_name_resolver_bootstrap_resolver_supplier_func =
            name_resolver_bootstrap_supplier_func;

        aeron_counters_manager_init(
            &resolver_fields->counters,
            &resolver_fields->counters_buffer[0], METADATA_LENGTH,
            &resolver_fields->counters_buffer[METADATA_LENGTH], VALUES_LENGTH,
            &m_cached_clock, 1000);
        aeron_system_counters_init(&resolver_fields->system_counters, &resolver_fields->counters);

        aeron_counters_reader_init(
            &resolver_fields->counters_reader,
            resolver_fields->counters.metadata,
            resolver_fields->counters.metadata_length,
            resolver_fields->counters.values,
            resolver_fields->counters.values_length);

        aeron_distinct_error_log_init(
            &resolver_fields->error_log, resolver_fields->error_log_buffer, ERROR_LOG_LENGTH, aeron_epoch_clock);

        resolver_fields->context->counters_manager = &resolver_fields->counters;
        resolver_fields->context->system_counters = &resolver_fields->system_counters;
        resolver_fields->context->error_log = &resolver_fields->error_log;

        ASSERT_EQ(0, supplier_func(&resolver_fields->resolver, args, resolver_fields->context)) << aeron_errmsg();
    }

    typedef struct counters_clientd_stct
    {
        int32_t type_id;
        int64_t value;
    }
    counters_clientd_t;

    typedef struct counter_label_clientd_stct
    {
        int32_t type_id;
        size_t label_length;
        char label[sizeof(aeron_counter_metadata_descriptor_t::label)];
    }
    counter_label_clientd_t;

    typedef struct find_name_counter_clientd_stct
    {
        const char *name;
        int32_t counter_id;
    }
    find_name_counter_clientd_t;

    static void findNameCounterCallback(
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const uint8_t *label,
        size_t label_length,
        void *clientd)
    {
        auto *find_name_clientd = (find_name_counter_clientd_t *)clientd;
        size_t query_name_length = strlen(find_name_clientd->name);

        if (AERON_NAME_RESOLVER_CSV_ENTRY_COUNTER_TYPE_ID == type_id)
        {
            uint32_t key_name_length;
            memcpy(&key_name_length, key, sizeof(key_name_length));
            if (query_name_length == key_name_length &&
                0 == memcmp(find_name_clientd->name, &key[sizeof(key_name_length)], key_name_length))
            {
                find_name_clientd->counter_id = id;
            }
        }
    }

    static int64_t *nameCounterAddrByHostname(resolver_fields_t *resolverFields, const char *name)
    {
        find_name_counter_clientd_t clientd = { name, -1 };

        aeron_counters_reader_foreach_metadata(
            resolverFields->counters_reader.metadata,
            resolverFields->counters_reader.metadata_length,
            findNameCounterCallback,
            (void *)&clientd);

        if (-1 < clientd.counter_id)
        {
            return aeron_counters_reader_addr(&resolverFields->counters_reader, clientd.counter_id);
        }

        return nullptr;
    }

    static void foreachFilterByTypeId(
        int64_t value,
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const char *label,
        size_t label_length,
        void *clientd)
    {
        auto *counters_clientd = static_cast<NameResolverTest::counters_clientd_t *>(clientd);
        if (counters_clientd->type_id == type_id)
        {
            counters_clientd->value = value;
        }
    }

    static void foreachFilterByTypeIdGetLabel(
        int64_t value,
        int32_t id,
        int32_t type_id,
        const uint8_t *key,
        size_t key_length,
        const char *label,
        size_t label_length,
        void *clientd)
    {
        auto *label_clientd = static_cast<NameResolverTest::counter_label_clientd_t *>(clientd);
        if (label_clientd->type_id == type_id)
        {
            label_clientd->label_length = label_length;
            strncpy(label_clientd->label, label, label_length);
        }
    }

    static int64_t readCounterByTypeId(const aeron_counters_reader_t *counters_reader, int32_t type_id)
    {
        counters_clientd_t clientd = { type_id, -1 };

        aeron_counters_reader_foreach_counter(
            (aeron_counters_reader_t *)counters_reader, foreachFilterByTypeId, &clientd);

        return clientd.value;
    }

    static counter_label_clientd_t readCounterLabelByTypeId(
        const aeron_counters_reader_t *counters_reader, int32_t type_id)
    {
        counter_label_clientd_t clientd = { type_id, 0, { '\0' }};

        aeron_counters_reader_foreach_counter(
            (aeron_counters_reader_t *)counters_reader, foreachFilterByTypeIdGetLabel, &clientd);

        return clientd;
    }

    static int64_t readNeighborCounter(const resolver_fields_t *resolver)
    {
        return readCounterByTypeId(&resolver->counters_reader, AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID);
    }

    static int64_t readCacheEntriesCounter(const resolver_fields_t *resolver)
    {
        return readCounterByTypeId(
            &resolver->counters_reader, AERON_COUNTER_NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID);
    }

    static int64_t readSystemCounter(const resolver_fields_t *resolver, aeron_system_counter_enum_t counter)
    {
        return aeron_counter_get_plain(aeron_system_counter_addr(resolver->context->system_counters, counter));
    }

    static int64_t shortSends(const resolver_fields_t *resolver)
    {
        return readSystemCounter(resolver, AERON_SYSTEM_COUNTER_SHORT_SENDS);
    }

    static void printCounters(std::ostream &output, const resolver_fields_t *resolver, const char *name)
    {
        if (nullptr != resolver->context)
        {
            output
                << " " << name << "(" << shortSends(resolver) << "," << readNeighborCounter(resolver)
                << "," << readCacheEntriesCounter(resolver) << ")";
        }
    }

    static void assert_neighbor_counter_label_is(const resolver_fields_t *resolver, const char *expected_label)
    {
        auto result = readCounterLabelByTypeId(
            &resolver->counters_reader, AERON_COUNTER_NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID);
        EXPECT_EQ(result.label_length, strlen(expected_label));
        ASSERT_EQ(0, strncmp(
            expected_label, result.label,
            result.label_length)) << "Expected: " << expected_label << ", actual: " << result.label;
    }

    static int ignore_unreslovable_address_lookup_function(
        aeron_name_resolver_t *resolver,
        const char *name,
        const char *uri_param_name,
        bool is_re_resolution,
        const char **resolved_name)
    {
        if (nullptr != unresolvable_address &&
            0 == strncmp(unresolvable_address, name, strlen(unresolvable_address) + 1))
        {
            *resolved_name = nullptr;
            return -1;
        }
        *resolved_name = name;
        return 0;
    }

    static int bootstrap_name_resolver_supplier(
        aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
    {
        if (0 == aeron_default_name_resolver_supplier(resolver, args, context))
        {
            resolver->lookup_func = ignore_unreslovable_address_lookup_function;
            resolver->name = "test-bootstrap";
            return 0;
        }

        return -1;
    }

    friend std::ostream &operator<<(std::ostream &output, const NameResolverTest &t)
    {
        printCounters(output, &t.m_a, "A");
        printCounters(output, &t.m_b, "B");
        printCounters(output, &t.m_c, "C");
        return output;
    }

    static void close(resolver_fields_t *resolver_fields)
    {
        if (nullptr != resolver_fields->context)
        {
            resolver_fields->resolver.close_func(&resolver_fields->resolver);
            aeron_system_counters_close(&resolver_fields->system_counters);
            aeron_counters_manager_close(&resolver_fields->counters);
            aeron_distinct_error_log_close(&resolver_fields->error_log);
            aeron_driver_context_close(resolver_fields->context);
        }
    }

    resolver_fields_t m_a = {};
    resolver_fields_t m_b = {};
    resolver_fields_t m_c = {};
    aeron_clock_cache_t m_cached_clock = {};
    static const char *unresolvable_address;
};

const char *NameResolverTest::unresolvable_address;

#define NAME_0 "server0"
#define HOST_0A "127.0.0.1"
#define HOST_0B "127.0.0.2"

#define NAME_1 "server1"
#define HOST_1A "127.0.0.3"
#define HOST_1B "127.0.0.4"

TEST_F(NameResolverTest, shouldUseStaticLookupTable)
{
    in_addr host_0a = {};
    inet_pton(AF_INET, HOST_0A, &host_0a);
    in_addr host_0b = {};
    inet_pton(AF_INET, HOST_0B, &host_0b);
    in_addr host_1a = {};
    inet_pton(AF_INET, HOST_1A, &host_1a);
    in_addr host_1b = {};
    inet_pton(AF_INET, HOST_1B, &host_1b);

    const char *config_param =
        NAME_0 "," HOST_0A "," HOST_0B "|"
        NAME_1 "," HOST_1A "," HOST_1B "|"
        "NAME_2" "," HOST_1A "," HOST_1B "|"
        "NAME_3" "," HOST_1A "," HOST_1B "|"
        "NAME_4" "," HOST_1A "," HOST_1B "|"
        "NAME_5" "," HOST_1A "," HOST_1B "|"
        "NAME_6" "," HOST_1A "," HOST_1B "|"
        "NAME_7" "," HOST_1A "," HOST_1B "|"
        "NAME_8" "," HOST_1A "," HOST_1B "|"
        "NAME_9" "," HOST_1A "," HOST_1B "|";

    initResolver(&m_a, AERON_NAME_RESOLVER_CSV_TABLE, config_param, 0);

    int64_t *name0ToggleAddr = nameCounterAddrByHostname(&m_a, NAME_0);
    int64_t *name1ToggleAddr = nameCounterAddrByHostname(&m_a, NAME_1);

    struct sockaddr_in address = {};
    address.sin_family = AF_INET;
    auto *addr_ptr = (struct sockaddr_storage *)&address;

    ASSERT_EQ(0, m_a.resolver.resolve_func(&m_a.resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, addr_ptr));
    ASSERT_EQ(host_0a.s_addr, address.sin_addr.s_addr);

    aeron_counter_set_release(name0ToggleAddr, AERON_NAME_RESOLVER_CSV_USE_RE_RESOLUTION_HOST_OP);

    ASSERT_EQ(0, m_a.resolver.resolve_func(&m_a.resolver, NAME_0, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, addr_ptr));
    ASSERT_EQ(host_0b.s_addr, address.sin_addr.s_addr);

    ASSERT_EQ(0, m_a.resolver.resolve_func(&m_a.resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, false, addr_ptr));
    ASSERT_EQ(host_1a.s_addr, address.sin_addr.s_addr);

    aeron_counter_set_release(name1ToggleAddr, AERON_NAME_RESOLVER_CSV_USE_RE_RESOLUTION_HOST_OP);

    ASSERT_EQ(0, m_a.resolver.resolve_func(&m_a.resolver, NAME_1, AERON_UDP_CHANNEL_ENDPOINT_KEY, true, addr_ptr));
    ASSERT_EQ(host_1b.s_addr, address.sin_addr.s_addr);
}

TEST_F(NameResolverTest, shouldSeeNeighborFromBootstrapAndHandleIPv4WildCard)
{
    int64_t timestamp_ms = INTMAX_C(8932472347945);

    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");
    initResolver(&m_b, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "just:wrong,non_existing_host:8050,localhost:8050");

    timestamp_ms += 2000;

    int64_t deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver b to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver a to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    struct sockaddr_storage resolved_address_of_b = {};
    resolved_address_of_b.ss_family = AF_INET;
    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "B", "endpoint", false, &resolved_address_of_b));
    ASSERT_EQ(AF_INET, resolved_address_of_b.ss_family);
    auto *in_addr_b = (struct sockaddr_in *)&resolved_address_of_b;
    ASSERT_NE(INADDR_ANY, in_addr_b->sin_addr.s_addr);

    assert_neighbor_counter_label_is(&m_a, "Resolver neighbors: bound 0.0.0.0:8050");
    assert_neighbor_counter_label_is(&m_b, "Resolver neighbors: bound 0.0.0.0:8051 bootstrap 127.0.0.1:8050");
}

TEST_F(NameResolverTest, DISABLED_shouldSeeNeighborFromBootstrapAndHandleIPv6WildCard)
{
    int64_t timestamp_ms = INTMAX_C(8932472347945);

    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "[::]:8050");
    initResolver(&m_b, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "[::]:8051", "localhost:8050");

    timestamp_ms += 2000;

    int64_t deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver b to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver a to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    struct sockaddr_storage resolved_address_of_b = {};
    resolved_address_of_b.ss_family = AF_INET6;
    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "B", "endpoint", false, &resolved_address_of_b));
    ASSERT_EQ(AF_INET6, resolved_address_of_b.ss_family);
    auto *in_addr_b = (struct sockaddr_in6 *)&resolved_address_of_b;
    ASSERT_NE(0, memcmp(&in6addr_any, &in_addr_b->sin6_addr, sizeof(in6addr_any)));
}

TEST_F(NameResolverTest, shouldSeeNeighborFromGossip)
{
    int64_t timestamp_ms = INTMAX_C(8932472347945);
    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");
    initResolver(&m_b, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "localhost:8050");
    initResolver(&m_c, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "C", "0.0.0.0:8052", "localhost:8051");

    int64_t deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (2 > readNeighborCounter(&m_a) || 2 > readNeighborCounter(&m_b) || 2 > readNeighborCounter(&m_c))
    {
        timestamp_ms += 1000;
        aeron_clock_update_cached_epoch_time(m_a.context->cached_clock, timestamp_ms);
        aeron_clock_update_cached_epoch_time(m_b.context->cached_clock, timestamp_ms);
        aeron_clock_update_cached_epoch_time(m_c.context->cached_clock, timestamp_ms);

        int work_done;
        do
        {
            work_done = 0;
            work_done += m_c.resolver.do_work_func(&m_c.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            work_done += m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            work_done += m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            aeron_micro_sleep(10000);
            timestamp_ms += 10;

            aeron_clock_update_cached_epoch_time(m_a.context->cached_clock, timestamp_ms);
            aeron_clock_update_cached_epoch_time(m_b.context->cached_clock, timestamp_ms);
            aeron_clock_update_cached_epoch_time(m_c.context->cached_clock, timestamp_ms);
        }
        while (0 != work_done);

        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for neighbors" << *this;
    }

    struct sockaddr_storage resolved_address = {};
    resolved_address.ss_family = AF_INET;

    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "B", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_b.resolver.resolve_func(&m_b.resolver, "B", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_c.resolver.resolve_func(&m_c.resolver, "B", "endpoint", false, &resolved_address));

    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "C", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_b.resolver.resolve_func(&m_b.resolver, "C", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_c.resolver.resolve_func(&m_c.resolver, "C", "endpoint", false, &resolved_address));

    ASSERT_LE(0, m_c.resolver.resolve_func(&m_c.resolver, "A", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_b.resolver.resolve_func(&m_b.resolver, "A", "endpoint", false, &resolved_address));
    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "A", "endpoint", false, &resolved_address));
}

TEST_F(NameResolverTest, shouldUseAnotherNeighborIfCurrentBecomesUnavailable)
{
    int64_t timestamp_ms = INTMAX_C(8932472347945);
    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");
    initResolver(
        &m_b,
        AERON_NAME_RESOLVER_DRIVER,
        "",
        timestamp_ms,
        "B",
        "0.0.0.0:8051",
        "localhost:8050,test,localhost:8052,more garbage",
        bootstrap_name_resolver_supplier);
    initResolver(
        &m_c,
        AERON_NAME_RESOLVER_DRIVER,
        "",
        timestamp_ms,
        "C",
        "0.0.0.0:8052",
        "localhost:8050,x:y,localhost:8051,here too",
        bootstrap_name_resolver_supplier);

    int64_t deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (2 > readNeighborCounter(&m_a) || 2 > readNeighborCounter(&m_b) || 2 > readNeighborCounter(&m_c))
    {
        timestamp_ms += 1000;
        aeron_clock_update_cached_epoch_time(m_a.context->cached_clock, timestamp_ms);
        aeron_clock_update_cached_epoch_time(m_b.context->cached_clock, timestamp_ms);
        aeron_clock_update_cached_epoch_time(m_c.context->cached_clock, timestamp_ms);

        int work_done;
        do
        {
            work_done = 0;
            work_done += m_c.resolver.do_work_func(&m_c.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            work_done += m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            work_done += m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms);
            ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

            aeron_micro_sleep(10000);
            timestamp_ms += 10;

            aeron_clock_update_cached_epoch_time(m_a.context->cached_clock, timestamp_ms);
            aeron_clock_update_cached_epoch_time(m_b.context->cached_clock, timestamp_ms);
            aeron_clock_update_cached_epoch_time(m_c.context->cached_clock, timestamp_ms);
        }
        while (0 != work_done);

        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for neighbors" << *this;
    }

    assert_neighbor_counter_label_is(&m_a, "Resolver neighbors: bound 0.0.0.0:8050");
    assert_neighbor_counter_label_is(&m_b, "Resolver neighbors: bound 0.0.0.0:8051 bootstrap 127.0.0.1:8050");
    assert_neighbor_counter_label_is(&m_c, "Resolver neighbors: bound 0.0.0.0:8052 bootstrap 127.0.0.1:8050");

    close(&m_a);
    m_a.context = nullptr;

    timestamp_ms += AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;

    unresolvable_address = "localhost:8050"; // ensure that A is now unresolvable

    m_c.resolver.do_work_func(&m_c.resolver, timestamp_ms);
    ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

    m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms);
    ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();

    assert_neighbor_counter_label_is(&m_b, "Resolver neighbors: bound 0.0.0.0:8051 bootstrap 127.0.0.1:8052");
    assert_neighbor_counter_label_is(&m_c, "Resolver neighbors: bound 0.0.0.0:8052 bootstrap 127.0.0.1:8051");
}

TEST_F(NameResolverTest, shouldHandleSettingNameOnHeader)
{
    uint8_t buffer[1024] = { 0 };
    const char *hostname = "this.is.the.hostname";
    auto *resolution_header = (aeron_resolution_header_t *)&buffer[0];
    uint8_t flags = 0;
    struct sockaddr_storage address = {};

    address.ss_family = AF_INET6;
    ASSERT_EQ(48, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(48, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, 48, flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(0, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, 47, flags, &address, hostname, strlen(hostname)));

    address.ss_family = AF_INET;
    ASSERT_EQ(40, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(40, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, 40, flags, &address, hostname, strlen(hostname)));
    ASSERT_EQ(0, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, 39, flags, &address, hostname, strlen(hostname)));

    address.ss_family = AF_UNIX;
    ASSERT_EQ(-1, aeron_driver_name_resolver_set_resolution_header_from_sockaddr(
        resolution_header, sizeof(buffer), flags, &address, hostname, strlen(hostname)));
}

TEST_F(NameResolverTest, shouldTimeoutNeighbor)
{
    aeron_name_resolver_supplier_func_t supplier_func = aeron_name_resolver_supplier_load(AERON_NAME_RESOLVER_DRIVER);
    ASSERT_NE(nullptr, supplier_func);
    struct sockaddr_storage address = {};
    int64_t timestamp_ms = INTMAX_C(8932472347945);

    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "A", "0.0.0.0:8050");

    initResolver(&m_b, AERON_NAME_RESOLVER_DRIVER, "", timestamp_ms, "B", "0.0.0.0:8051", "localhost:8050");

    int64_t deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_b.resolver.do_work_func(&m_b.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver b to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    deadline_ms = aeron_epoch_clock() + (5 * 1000);
    while (m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms) <= 0)
    {
        ASSERT_EQ(0, aeron_errcode()) << aeron_errmsg();
        ASSERT_LT(aeron_epoch_clock(), deadline_ms) << "Timed out waiting for resolver a to do work" << *this;
        aeron_micro_sleep(10000);
        timestamp_ms += 10;
    }

    // A sees B.
    ASSERT_LE(0, m_a.resolver.resolve_func(&m_a.resolver, "B", "endpoint", false, &address));

    ASSERT_EQ(1, readCacheEntriesCounter(&m_a));
    ASSERT_EQ(1, readNeighborCounter(&m_a));

    timestamp_ms += AERON_NAME_RESOLVER_DRIVER_TIMEOUT_MS;
    timestamp_ms += 2000;

    // B's not pushed it self resolution recently enough
    ASSERT_LT(0, m_a.resolver.do_work_func(&m_a.resolver, timestamp_ms));

    ASSERT_EQ(-1, m_a.resolver.resolve_func(&m_a.resolver, "B", "endpoint", false, &address));
    ASSERT_EQ(0, readCacheEntriesCounter(&m_a));
    ASSERT_EQ(0, readNeighborCounter(&m_a));
}

TEST_F(NameResolverTest, shouldHandleDissection)
{
    uint8_t buffer[128] = { 0 };
    initResolver(&m_a, AERON_NAME_RESOLVER_DRIVER, "", 0, "A", "[::1]:8050");

    auto *log_header = reinterpret_cast<aeron_driver_agent_frame_log_header_t *>(&buffer[0]);
    log_header->sockaddr_len = sizeof(struct sockaddr_in);

    size_t socket_offset = sizeof(aeron_driver_agent_frame_log_header_t);
    auto *socket_address = reinterpret_cast<sockaddr_in *>(&buffer[socket_offset]);
    ASSERT_EQ(1, inet_pton(AF_INET, "127.0.0.1", &socket_address->sin_addr));
    socket_address->sin_port = htons(5555);
    socket_address->sin_family = AF_INET;

    size_t frame_offset = socket_offset + log_header->sockaddr_len;
    auto *frame = reinterpret_cast<aeron_frame_header_t *>(&buffer[frame_offset]);

    size_t res_offset = sizeof(aeron_frame_header_t) + frame_offset;
    auto *res = reinterpret_cast<aeron_resolution_header_ipv6_t *>(&buffer[res_offset]);

    res->resolution_header.res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP6_MD;
    res->resolution_header.res_flags = AERON_RES_HEADER_SELF_FLAG;
    res->resolution_header.age_in_ms = 100;
    res->resolution_header.udp_port = 9872;
    inet_pton(AF_INET6, "::1", &res->addr);
    res->name_length = 8;
    memcpy(&buffer[res_offset + sizeof(aeron_resolution_header_ipv6_t)], "ABCDEFHG", res->name_length);
    res_offset += aeron_res_header_entry_length_ipv6(res);

    auto *res2 = reinterpret_cast<aeron_resolution_header_ipv4_t *>(&buffer[res_offset]);

    res2->resolution_header.res_type = AERON_RES_HEADER_TYPE_NAME_TO_IP4_MD;
    res2->resolution_header.res_flags = 0b00110011;
    res2->resolution_header.age_in_ms = 333;
    res2->resolution_header.udp_port = 8080;
    inet_pton(AF_INET, "127.0.0.1", &res2->addr);
    res2->name_length = 4;
    memcpy(&buffer[res_offset + sizeof(aeron_resolution_header_ipv4_t)], "test", res2->name_length);
    res_offset += aeron_res_header_entry_length_ipv4(res2);

    frame->type = AERON_HDR_TYPE_RES;
    frame->frame_length = (int32_t)res_offset;
    frame->flags = 0b10101011;
    log_header->message_len = frame->frame_length;

    aeron_env_set(AERON_EVENT_LOG_ENV_VAR, AERON_DRIVER_AGENT_ALL_EVENTS);
    aeron_driver_agent_context_init(m_a.context);

    testing::internal::CaptureStdout();
    aeron_driver_agent_log_dissector(AERON_DRIVER_EVENT_FRAME_IN, buffer, res_offset, nullptr);

#if AERON_COMPILER_MSVC
    GTEST_SKIP();
#endif
    std::string output = testing::internal::GetCapturedStdout();
    EXPECT_EQ("[0.000000000] DRIVER: FRAME_IN [104/104]: address=127.0.0.1:5555 type=RES flags=10101011 frameLength=104 [resType=2 flags=10000000 port=9872 ageInMs=100 address=::1 name=ABCDEFHG] [resType=1 flags=00110011 port=8080 ageInMs=333 address=127.0.0.1 name=test]\n", output);
}
