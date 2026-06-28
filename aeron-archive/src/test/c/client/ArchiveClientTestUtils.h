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

#ifndef AERON_ARCHIVECLIENTTESTUTILS_H
#define AERON_ARCHIVECLIENTTESTUTILS_H

#include <stdexcept>
#include <string>
#include "EmbeddedMediaDriver.h"

extern "C"
{
#include "aeronc.h"
#include "client/aeron_archive.h"
#include "util/aeron_bitutil.h"
}

class DriverResource
{
public:
    DriverResource()
    {
        char path[AERON_MAX_PATH];
        aeron_default_path(path, sizeof(path));
        const std::string aeronDir = std::string(path) + "-" + std::to_string(aeron_randomised_int32());
        m_driver.aeronDir(aeronDir);
        m_driver.start();
    }

    ~DriverResource()
    {
        m_driver.stop();
    }

    std::string aeronDir()
    {
        return m_driver.aeronDir();
    }

protected:
    aeron::EmbeddedMediaDriver m_driver;
};

class AeronResource
{
public:
    explicit AeronResource(const std::string& aeronDir)
    {
        if (aeron_context_init(&m_aeron_ctx) < 0)
        {
            throw std::runtime_error("aeron_context_init failed: " + std::string(aeron_errmsg()));
        }
        aeron_context_set_dir(m_aeron_ctx, aeronDir.c_str());
        if (aeron_init(&m_aeron, m_aeron_ctx) < 0)
        {
            throw std::runtime_error("aeron_init failed: " + std::string(aeron_errmsg()));
        }
        if (aeron_start(m_aeron) < 0)
        {
            throw std::runtime_error("aeron_start failed: " + std::string(aeron_errmsg()));
        }
    }

    ~AeronResource()
    {
        aeron_close(m_aeron);
        aeron_context_close(m_aeron_ctx);
    }

    aeron_t* aeron() const
    {
        return m_aeron;
    }

    int64_t* findCounterByType(int32_t typeId) const
    {
        std::pair<int32_t, int32_t> ids{typeId, AERON_NULL_COUNTER_ID};
        aeron_counters_reader_t *counters_reader = aeron_counters_reader(m_aeron);
        aeron_counters_reader_foreach_counter(
            counters_reader,
            [](int64_t, int32_t id, int32_t type_id, const uint8_t*, size_t, const char*, size_t, void* clientd)
            {
                std::pair<int32_t, int32_t> *ids = static_cast<std::pair<int32_t, int32_t>*>(clientd);
                if (type_id == ids->first)
                {
                    ids->second = id;
                }
            },
            &ids);
        return ids.second == AERON_NULL_COUNTER_ID ? nullptr : aeron_counters_reader_addr(counters_reader, ids.second);
    }

private:
    aeron_context_t *m_aeron_ctx = nullptr;
    aeron_t *m_aeron = nullptr;
};

class Credentials
{
public:
    explicit Credentials(std::string credentials) :
        m_credentials(std::move(credentials)),
        m_encodedCredentials{m_credentials.c_str(), static_cast<uint32_t>(m_credentials.length())}
    {
    }

    static Credentials& defaultCredentials()
    {
        static Credentials defaultCredentials("admin:admin");
        return defaultCredentials;
    }

    int configure(aeron_archive_context_t *ctx)
    {
        return aeron_archive_context_set_credentials_supplier(ctx, supplier, nullptr, nullptr, this);
    }

private:
    std::string m_credentials;
    aeron_archive_encoded_credentials_t m_encodedCredentials;

    static aeron_archive_encoded_credentials_t *supplier(void *clientd)
    {
        Credentials *receiver = static_cast<Credentials*>(clientd);
        return &receiver->m_encodedCredentials;
    }
};

#endif //AERON_ARCHIVECLIENTTESTUTILS_H
