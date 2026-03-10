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

#include <string>

extern "C"
{
#include "aeronc.h"
#include "client/aeron_archive.h"
}

class AeronResource
{
public:
    explicit AeronResource(const std::string& aeronDir)
    {
        aeron_context_t *aeron_ctx;
        aeron_context_init(&aeron_ctx);
        aeron_context_set_dir(aeron_ctx, aeronDir.c_str());
        aeron_init(&m_aeron, aeron_ctx);
        aeron_start(m_aeron);
    }

    ~AeronResource()
    {
        aeron_close(m_aeron);
    }

    aeron_t* aeron() const
    {
        return m_aeron;
    }

private:
    aeron_t *m_aeron = nullptr;
};

class Credentials
{
public:
    explicit Credentials(std::string credentials) :
        m_credentials(std::move(credentials)),
        m_encodedCredentials(new aeron_archive_encoded_credentials_t {
            m_credentials.c_str(),
            static_cast<uint32_t>(m_credentials.length())
        })
    {
    }

    ~Credentials()
    {
        delete m_encodedCredentials;
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
    aeron_archive_encoded_credentials_t *m_encodedCredentials;

    static aeron_archive_encoded_credentials_t *supplier(void *clientd)
    {
        const auto receiver = static_cast<Credentials*>(clientd);
        return receiver->m_encodedCredentials;
    }
};

#endif //AERON_ARCHIVECLIENTTESTUTILS_H
