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

#ifndef AERON_CNCFILEREADER_H
#define AERON_CNCFILEREADER_H

#include "CncFileDescriptor.h"
#include "concurrent/CountersReader.h"
#include "concurrent/errors/ErrorLogReader.h"

#include "aeronc.h"

namespace aeron
{
using namespace aeron::util;
using namespace aeron::concurrent;
using namespace aeron::concurrent::errors;

class CncFileReader
{
public:
    static CncFileReader mapExisting(const char *baseDirectory)
    {
        aeron_cnc_t *aeron_cnc;
        if (aeron_cnc_init(&aeron_cnc, baseDirectory, 10000) < 0)
        {
            throw IOException(
                std::string("failed to open existing file cnc file in: ") + baseDirectory, SOURCEINFO, EINVAL);
        }

        return { aeron_cnc };
    }

    CountersReader countersReader() const
    {
        return CountersReader(aeron_cnc_counters_reader(m_aeron_cnc));
    }

    int readErrorLog(const ErrorLogReader::error_consumer_t &consumer, std::int64_t sinceTimestamp) const
    {
        void *clientd = const_cast<void *>(reinterpret_cast<const void *>(&consumer));
        return (int)aeron_cnc_error_log_read(m_aeron_cnc, errorCallback, clientd, sinceTimestamp);
    }

    ~CncFileReader()
    {
        aeron_cnc_close(m_aeron_cnc);
    }

private:
    aeron_cnc_t *m_aeron_cnc;

    CncFileReader(aeron_cnc_t *aeron_cnc) : m_aeron_cnc(aeron_cnc)
    {
    }

    static void errorCallback(
        int32_t observation_count,
        int64_t first_observation_timestamp,
        int64_t last_observation_timestamp,
        const char *error,
        size_t error_length,
        void *clientd)
    {
        ErrorLogReader::error_consumer_t &consumer = *reinterpret_cast<ErrorLogReader::error_consumer_t *>(clientd);
        consumer(observation_count, first_observation_timestamp, last_observation_timestamp, std::string(error, error_length));
    }
};
}

#endif //AERON_CNCFILEREADER_H
