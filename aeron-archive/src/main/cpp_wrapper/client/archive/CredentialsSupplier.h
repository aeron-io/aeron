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
#ifndef AERON_ARCHIVE_WRAPPER_CREDENTIALS_SUPPLIER_H
#define AERON_ARCHIVE_WRAPPER_CREDENTIALS_SUPPLIER_H

#include "AeronArchive.h"
#include "Context.h"

namespace aeron { namespace archive { namespace client
{

typedef std::function<std::pair<const char *, std::uint32_t>()> credentials_encoded_credentials_supplier_t;

inline std::pair<const char *, std::uint32_t> defaultCredentialsEncodedCredentials() noexcept
{
    return { nullptr, 0 };
}

typedef std::function<std::pair<const char *, std::uint32_t>(
    std::pair<const char *, std::uint32_t> encodedChallenge)> credentials_challenge_supplier_t;

inline std::pair<const char *, std::uint32_t> defaultCredentialsOnChallenge(
    std::pair<const char *, std::uint32_t> encodedChallenge) noexcept
{
    return { nullptr, 0 };
}

typedef std::function<void(std::pair<const char *, std::uint32_t> encodedCredentials)> credentials_free_t;

inline void defaultCredentialsOnFree(std::pair<const char *, std::uint32_t> credentials) noexcept
{
    delete[] credentials.first;
}

struct CredentialsSupplier
{
    credentials_encoded_credentials_supplier_t m_encodedCredentials = defaultCredentialsEncodedCredentials;
    credentials_challenge_supplier_t m_onChallenge = defaultCredentialsOnChallenge;
    credentials_free_t m_onFree = defaultCredentialsOnFree;

    explicit CredentialsSupplier(
        credentials_encoded_credentials_supplier_t encodedCredentials = defaultCredentialsEncodedCredentials,
        credentials_challenge_supplier_t onChallenge = defaultCredentialsOnChallenge,
        credentials_free_t onFree = defaultCredentialsOnFree) :
        m_encodedCredentials(std::move(encodedCredentials)),
        m_onChallenge(std::move(onChallenge)),
        m_onFree(std::move(onFree))
    {
    }
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_CREDENTIALS_SUPPLIER_H
