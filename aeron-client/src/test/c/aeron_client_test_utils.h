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

#ifndef AERON_CLIENT_TEST_UTILS_H
#define AERON_CLIENT_TEST_UTILS_H

#include <string>

extern "C"
{
#include "util/aeron_fileutil.h"
#include "util/aeron_error.h"
#include "aeron_log_buffer.h"
}

#define FILE_PAGE_SIZE (4 * 1024)

#define INITIAL_TERM_ID (1234)

namespace aeron
{
namespace test
{

std::string tempFileName()
{
    char filename[AERON_MAX_PATH] = { 0 };

    aeron_temp_filename(filename, sizeof(filename));
    return { filename };
}

void createLogFile(const std::string &filename, const int32_t term_length, const int32_t term_id)
{
    aeron_mapped_file_t mappedFile =
        { nullptr, term_length * 3 + AERON_LOGBUFFER_META_DATA_LENGTH };

    if (aeron_map_new_file(&mappedFile, filename.c_str(), false, FILE_PAGE_SIZE) < 0)
    {
        throw std::runtime_error("could not create log file: " + std::string(aeron_errmsg()));
    }

    auto metadata = reinterpret_cast<aeron_logbuffer_metadata_t *>((uint8_t *)mappedFile.addr +
        (mappedFile.length - AERON_LOGBUFFER_META_DATA_LENGTH));

    metadata->term_length = term_length;
    metadata->page_size = FILE_PAGE_SIZE;
    metadata->initial_term_id = term_id;
    metadata->term_tail_counters[0] = (int64_t)term_id << 32;
    metadata->term_tail_counters[1] = (int64_t)(term_id + 1) << 32;
    metadata->term_tail_counters[2] = (int64_t)(term_id + 2) << 32;

    aeron_unmap(&mappedFile);
}

void createLogFile(const std::string &filename)
{
    createLogFile(filename, AERON_LOGBUFFER_TERM_MIN_LENGTH, INITIAL_TERM_ID);
}

}
}

#endif //AERON_CLIENT_TEST_UTILS_H
