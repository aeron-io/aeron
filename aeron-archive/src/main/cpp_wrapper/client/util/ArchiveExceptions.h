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
#ifndef AERON_ARCHIVE_WRAPPER_UTIL_EXCEPTIONS_H
#define AERON_ARCHIVE_WRAPPER_UTIL_EXCEPTIONS_H

#include "util/Exceptions.h"
#include "aeronc.h"

namespace aeron { namespace util
{

AERON_DECLARE_SOURCED_EXCEPTION(ArchiveException, ExceptionCategory::EXCEPTION_CATEGORY_ERROR);

#define ARCHIVE_MAP_TO_SOURCED_EXCEPTION_AND_THROW(code, message) AERON_MAP_TO_SOURCED_EXCEPTION_AND_THROW_WITH_DEFAULT(code, message, ArchiveException)

#define ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW ARCHIVE_MAP_TO_SOURCED_EXCEPTION_AND_THROW(aeron_errcode(), aeron_errmsg())

}}

#endif // AERON_ARCHIVE_WRAPPER_UTIL_EXCEPTIONS_H
