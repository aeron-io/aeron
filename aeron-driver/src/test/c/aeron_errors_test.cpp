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

#include <functional>
#include <utility>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "aeron_test_base.h"

extern "C"
{
#include "aeron_system_counters.h"
#include "command/aeron_control_protocol.h"
#include "aeron_csv_table_name_resolver.h"
#include "util/aeron_error.h"
}

#define URI_RESERVED "aeron:udp?endpoint=localhost:24325"
#define STREAM_ID (117)

class ErrorCallbackValidation
{
public:
    explicit ErrorCallbackValidation(std::vector<std::string> expectedSubstrings) : 
        m_expectedSubstrings(std::move(expectedSubstrings))
    {}

    void reset()
    {
        m_observations.clear();
    }

    void validate(std::string &errorStr) 
    {
        m_observations.push_back(errorStr);

        for (auto &subString : m_expectedSubstrings)
        {
            if (std::string::npos == errorStr.find(subString))
            {
                return;
            }
        }
        
        m_validated = true;
    }
    
    bool validated() const
    {
        return m_validated;
    }

    friend std::ostream& operator<<(std::ostream& os, const ErrorCallbackValidation& bar)
    {
        os << "Unable to find: ";
        os << "{ ";
        for (auto &subString : bar.m_expectedSubstrings)
        {
            os << subString << "; ";
        }
        os << "} in:";
        os << std::endl;
        for (auto &subString : bar.m_observations)
        {
            os << subString;
        }

        return os;
    }

private:
    std::vector<std::string> m_expectedSubstrings;
    std::vector<std::string> m_observations;
    bool m_validated = false;
};

static const char *EXPECTED_RESOLVER_ERROR = "Unable to resolve host";

static int resolveLocalhostOnly(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    if (!is_re_resolution && 0 == strcmp("localhost", name))
    {
        auto *address_in = reinterpret_cast<sockaddr_in *>(address);
        inet_pton(AF_INET, "127.0.0.1", &address_in->sin_addr);
        address_in->sin_family = AF_INET;
        return 0;
    }
    else
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_UNKNOWN_HOST, "%s", EXPECTED_RESOLVER_ERROR);
        return -1;
    }
}

static int testResolverSupplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    int i = aeron_default_name_resolver_supplier(resolver, args, context);
    resolver->resolve_func = resolveLocalhostOnly;
    return i;
}

class CErrorsTest : public CSystemTestBase, public testing::Test
{
public:
    CErrorsTest() : CSystemTestBase(
        std::vector<std::pair<std::string, std::string>>{
            { "AERON_COUNTERS_BUFFER_LENGTH", "32768" },
        },
        [](aeron_driver_context_t *ctx)
        {
            aeron_driver_context_set_name_resolver_supplier(ctx, testResolverSupplier);
        })
    {
    }

protected:
    volatile std::int64_t *m_errorCounter = nullptr;
    std::int64_t m_initialErrorCount = 0;
    aeron_counters_reader_t *m_countersReader = nullptr;

    aeron_t *connect() override
    {
        aeron_t *aeron = CSystemTestBase::connect();

        m_countersReader = aeron_counters_reader(aeron);
        m_errorCounter = aeron_counters_reader_addr(m_countersReader, AERON_SYSTEM_COUNTER_ID_ERRORS);
        AERON_GET_ACQUIRE(m_initialErrorCount, *m_errorCounter);

        return aeron;
    }

    void waitForErrorCounterIncrease()
    {
        int64_t currentErrorCount = 0;
        do
        {
            std::this_thread::yield();
            AERON_GET_ACQUIRE(currentErrorCount, *m_errorCounter);
        }
        while (currentErrorCount <= m_initialErrorCount);
    }
    
    void verifyDistinctErrorLogContains(const char *text, std::int64_t timeoutMs = 0)
    {
        aeron_cnc_t *aeronCnc = nullptr;
        const char *aeron_dir = aeron_context_get_dir(m_context);
        int result = aeron_cnc_init(&aeronCnc, aeron_dir, 1000);
        ASSERT_EQ(0, result) << "CnC file not available: " << aeron_dir;

        ErrorCallbackValidation errorCallbackValidation
        {
            std::vector<std::string>{ text }
        };

        std::int64_t deadlineMs = aeron_epoch_clock() + timeoutMs;
        do
        {
            std::this_thread::yield();
            errorCallbackValidation.reset();
            aeron_cnc_error_log_read(aeronCnc, errorCallback, &errorCallbackValidation, 0);
        }
        while (!errorCallbackValidation.validated() && aeron_epoch_clock() <= deadlineMs);

        EXPECT_TRUE(errorCallbackValidation.validated()) << errorCallbackValidation;
        
        aeron_cnc_close(aeronCnc);
    }

    static void errorCallback(
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        auto *callbackValidation = reinterpret_cast<ErrorCallbackValidation *>(clientd);
        std::string errorStr = std::string(error, error_length);
        callbackValidation->validate(errorStr);
    }

};

TEST_F(CErrorsTest, shouldErrorOnAddCounterPoll)
{
    ASSERT_EQ(-1, aeron_async_add_counter_poll(nullptr, nullptr));
    std::string errorMessage = std::string(aeron_errmsg());
    ASSERT_THAT(errorMessage, testing::HasSubstr("Invalid argument"));
    ASSERT_THAT(errorMessage, testing::HasSubstr("Parameters must not be null"));
}

TEST_F(CErrorsTest, shouldValidatePollType)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async = nullptr;
    aeron_publication_t *pub = nullptr;
    aeron_counter_t *counter = nullptr;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:ipc", 1001));
    ASSERT_EQ(-1, aeron_async_add_counter_poll(&counter, (aeron_async_add_counter_t *)pub_async)) << aeron_errmsg();
    std::string errorMessage = std::string(aeron_errmsg());
    ASSERT_THAT(errorMessage, testing::HasSubstr("Invalid argument"));
    ASSERT_THAT(errorMessage, testing::HasSubstr("Parameters must be valid, async->type"));

    while (1 != aeron_async_add_publication_poll(&pub, pub_async))
    {
        std::this_thread::yield();
    }

    aeron_publication_close(pub, nullptr, nullptr);
}

TEST_F(CErrorsTest, publicationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async = nullptr;
    aeron_publication_t *pub = nullptr;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(errorMessage, testing::HasSubstr("async_add_publication registration"));
    ASSERT_THAT(errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, exclusivePublicationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_exclusive_publication_t *pub_async = nullptr;
    aeron_exclusive_publication_t *pub = nullptr;

    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&pub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001));

    int result;
    while (0 == (result = aeron_async_add_exclusive_publication_poll(&pub, pub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(errorMessage, testing::HasSubstr("async_add_exclusive_publication registration"));
    ASSERT_THAT(errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, subscriptionErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_subscription_t *sub_async = nullptr;
    aeron_subscription_t *sub = nullptr;

    ASSERT_EQ(0, aeron_async_add_subscription(
        &sub_async, aeron, "aeron:tcp?endpoint=localhost:21345", 1001, nullptr, nullptr, nullptr, nullptr));

    int result;
    while (0 == (result = aeron_async_add_subscription_poll(&sub, sub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(-1, result) << aeron_errmsg();
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(errorMessage, testing::HasSubstr("async_add_subscription registration"));
    ASSERT_THAT(errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, destinationErrorIncludesClientAndDriverErrorAndReportsInDistinctLog)
{
    aeron_t *aeron = connect();
    aeron_async_add_exclusive_publication_t *pub_async = nullptr;
    aeron_async_destination_t *dest_async = nullptr;
    aeron_exclusive_publication_t *pub = nullptr;

    ASSERT_EQ(0, aeron_async_add_exclusive_publication(&pub_async, aeron, "aeron:udp?control-mode=manual", 1001));

    int result;
    while (0 == (result = aeron_async_add_exclusive_publication_poll(&pub, pub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(1, result) << aeron_errmsg();

    ASSERT_EQ(0, aeron_exclusive_publication_async_add_destination(
        &dest_async, aeron, pub, "aeron:tcp?endpoint=localhost:21345"));

    while (0 == (result = aeron_exclusive_publication_async_destination_poll(dest_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());

    const char *expectedDriverMessage = "invalid URI scheme or transport: aeron:tcp?endpoint=localhost:21345";

    ASSERT_THAT(-AERON_ERROR_CODE_INVALID_CHANNEL, aeron_errcode());
    ASSERT_THAT(errorMessage, testing::HasSubstr("async_add_destination registration"));
    ASSERT_THAT(errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, shouldFailToResovleNameOnPublication)
{
    aeron_t *aeron = connect();
    aeron_async_add_publication_t *pub_async = nullptr;
    aeron_publication_t *pub = nullptr;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:udp?endpoint=foo.example.com:20202", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(-1, result);
    std::string errorMessage = std::string(aeron_errmsg());
    const char *expectedDriverMessage = "Unable to resolve host";

    ASSERT_THAT(-AERON_ERROR_CODE_UNKNOWN_HOST, aeron_errcode());
    ASSERT_THAT(errorMessage, testing::HasSubstr("async_add_publication registration"));
    ASSERT_THAT(errorMessage, testing::HasSubstr(expectedDriverMessage));

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(expectedDriverMessage);
}

TEST_F(CErrorsTest, shouldRecordDistinctErrorCorrectlyOnReresolve)
{
    aeron_t *aeron = connect();

    aeron_async_add_publication_t *pub_async = nullptr;
    aeron_publication_t *pub = nullptr;

    ASSERT_EQ(0, aeron_async_add_publication(&pub_async, aeron, "aeron:udp?endpoint=localhost:5656", 1001));

    int result;
    while (0 == (result = aeron_async_add_publication_poll(&pub, pub_async)))
    {
        std::this_thread::yield();
    }

    ASSERT_EQ(1, result) << aeron_errmsg();

    waitForErrorCounterIncrease();
    verifyDistinctErrorLogContains(EXPECTED_RESOLVER_ERROR, 10000);
}