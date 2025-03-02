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
#ifndef AERON_CONCURRENT_DISTINCT_ERROR_LOG_H
#define AERON_CONCURRENT_DISTINCT_ERROR_LOG_H

#include <functional>
#include <chrono>
#include "concurrent/AtomicBuffer.h"
extern "C"
{
#include "util/aeron_error.h"
#include "concurrent/aeron_distinct_error_log.h"
}

namespace aeron { namespace concurrent { namespace errors {

class DistinctErrorLog
{
public:
    inline DistinctErrorLog(AtomicBuffer &buffer)
    {
        if (aeron_distinct_error_log_init(
            &m_log, buffer.buffer(), buffer.capacity(), epochTime) < 0)
        {
            std::string errMsg = aeron_errmsg();
            aeron_err_clear();
            throw util::SourcedException(errMsg, SOURCEINFO);
        }
    }

    inline ~DistinctErrorLog()
    {
        aeron_distinct_error_log_close(&m_log);
    }

    inline bool record(const std::exception &observation)
    {
        return record(typeid(observation).hash_code(), observation.what(), "no message");
    }

    inline bool record(const util::SourcedException &observation)
    {
        return record(typeid(observation).hash_code(), observation.where(), observation.what());
    }

    bool record(std::size_t errorCode, const std::string &description, const std::string &message)
    {
        return aeron_distinct_error_log_record(
            &m_log,
            static_cast<int>(errorCode),
            encodeObservation(description, message).c_str()) == 0;
    }

    static std::int64_t epochTime()
    {
        return std::chrono::system_clock::now().time_since_epoch() / std::chrono::milliseconds(1);
    }

private:
    aeron_distinct_error_log_t m_log {};

    static std::string encodeObservation(const std::string &description, const std::string &message)
    {
        return description + " " + message;
    }
};

}}}

#endif