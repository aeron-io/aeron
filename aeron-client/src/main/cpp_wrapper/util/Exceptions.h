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
#ifndef AERON_UTIL_EXCEPTIONS_FILE_H
#define AERON_UTIL_EXCEPTIONS_FILE_H

#include <cstdint>
#include <string>
#include <stdexcept>
#include <functional>

#include "util/MacroUtil.h"

#include "aeronc.h"

namespace aeron { namespace util
{

#ifdef _MSC_VER
#define AERON_FILE_SEP '\\'
#else
#define AERON_FILE_SEP '/'
#endif

static constexpr const char *past_prefix(const char * const prefix, const char * const filename)
{
    return *prefix == *filename ?
        past_prefix(prefix + 1, filename + 1) : *filename == AERON_FILE_SEP ? filename + 1 : filename;
}

#ifdef __PROJECT_SOURCE_DIR__
    #define __SHORT_FILE__ aeron::util::past_prefix(__PROJECT_SOURCE_DIR__,__FILE__)
#else
    #define __SHORT_FILE__ __FILE__
#endif

#ifdef _MSC_VER
    #define SOURCEINFO __FUNCTION__,  __SHORT_FILE__, __LINE__
#else
    #define SOURCEINFO  __PRETTY_FUNCTION__,  __SHORT_FILE__, __LINE__
#endif

/**
 * Callback to indicate an exception has occurred.
 *
 * This handler may be called in a context of noexcept so the handler can not safely throw.
 *
 * @param exception that has occurred.
 */
typedef std::function<void(const std::exception &exception)> exception_handler_t;

enum class ExceptionCategory : std::int64_t
{
    EXCEPTION_CATEGORY_FATAL = 0,
    EXCEPTION_CATEGORY_ERROR = 1,
    EXCEPTION_CATEGORY_WARN = 2
};

class SourcedException : public std::exception
{
private:
    const std::string m_where;
    const std::string m_what;
    const ExceptionCategory m_category;

public:
    SourcedException(
        ExceptionCategory category,
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        m_where(function + " : " + file + " : " + std::to_string(line)),
        m_what(what),
        m_category(category)
    {
    }

    SourcedException(const std::string &what, const std::string &function, const std::string &file, const int line) :
        SourcedException(ExceptionCategory::EXCEPTION_CATEGORY_ERROR, what, function, file, line)
    {
    }

    const char *what() const noexcept override
    {
        return m_what.c_str();
    }

    const char *where() const noexcept
    {
        return m_where.c_str();
    }

    ExceptionCategory category() const noexcept
    {
        return m_category;
    }
};

#define AERON_DECLARE_SOURCED_EXCEPTION(exceptionName, category) \
class exceptionName : public aeron::util::SourcedException       \
{                                                                \
public:                                                          \
    exceptionName(                                               \
        const std::string &what,                                 \
        const std::string &function,                             \
        const std::string &file,                                 \
        const int line) :                                        \
        SourcedException(category, what, function, file, line)   \
    {                                                            \
    }                                                            \
}                                                                \

AERON_DECLARE_SOURCED_EXCEPTION(IOException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(FormatException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(OutOfBoundsException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(ParseException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(ElementNotFound, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(IllegalArgumentException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(IllegalStateException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(DriverTimeoutException, ExceptionCategory::EXCEPTION_CATEGORY_FATAL);
AERON_DECLARE_SOURCED_EXCEPTION(ConductorServiceTimeoutException, ExceptionCategory::EXCEPTION_CATEGORY_FATAL);
AERON_DECLARE_SOURCED_EXCEPTION(ClientTimeoutException, ExceptionCategory::EXCEPTION_CATEGORY_FATAL);
AERON_DECLARE_SOURCED_EXCEPTION(AeronException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(UnknownSubscriptionException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(ReentrantException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);
AERON_DECLARE_SOURCED_EXCEPTION(UnsupportedOperationException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);

#define AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW_WITH_DEFAULT(code, message, defaultException) \
do                                                                                             \
{                                                                                              \
    switch ((code))                                                                            \
    {                                                                                          \
        case EINVAL:                                                                           \
            throw IllegalArgumentException((message), SOURCEINFO);                             \
                                                                                               \
        case EPERM:                                                                            \
        case -AERON_CLIENT_ERROR_BUFFER_FULL:                                                  \
        case -AERON_CLIENT_ERROR_DRIVER_BUFFER_FULL:                                           \
            throw IllegalStateException((message), SOURCEINFO);                                \
                                                                                               \
        case EIO:                                                                              \
        case ENOENT:                                                                           \
            throw IOException((message), SOURCEINFO);                                          \
                                                                                               \
        case -AERON_CLIENT_ERROR_DRIVER_TIMEOUT:                                               \
            throw DriverTimeoutException((message), SOURCEINFO);                               \
                                                                                               \
        case -AERON_CLIENT_ERROR_CLIENT_TIMEOUT:                                               \
            throw ClientTimeoutException((message), SOURCEINFO);                               \
                                                                                               \
        case -AERON_CLIENT_ERROR_CONDUCTOR_SERVICE_TIMEOUT:                                    \
            throw ConductorServiceTimeoutException((message), SOURCEINFO);                     \
                                                                                               \
        case ETIMEDOUT:                                                                        \
        default:                                                                               \
            throw defaultException((message), SOURCEINFO);                                     \
    }                                                                                          \
}                                                                                              \
while (0)

#define AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW(code, message) AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW_WITH_DEFAULT(code, message, AeronException)

#define AERON_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW(aeron_errcode(), aeron_errmsg())

class RegistrationException : public SourcedException
{
private:
    std::int32_t m_errorCode;

public:
    RegistrationException(
        std::int32_t errorCode,
        ExceptionCategory exceptionCategory,
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        SourcedException(exceptionCategory, what, function, file, line),
        m_errorCode(errorCode)
    {
    }

    std::int32_t errorCode() const
    {
        return m_errorCode;
    }
};

class TimeoutException : public AeronException
{
public:
    TimeoutException(
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        AeronException(what, function, file, line)
    {
    }
};

class ChannelEndpointException : public AeronException
{
private:
    std::int32_t m_statusIndicatorCounterId;

public:
    ChannelEndpointException(
        std::int32_t statusIndicatorCounterId,
        const std::string &what,
        const std::string &function,
        const std::string &file,
        const int line) :
        AeronException(what, function, file, line),
        m_statusIndicatorCounterId(statusIndicatorCounterId)
    {
    }

    std::int32_t statusIndicatorId() const
    {
        return m_statusIndicatorCounterId;
    }
};

}}
#endif
