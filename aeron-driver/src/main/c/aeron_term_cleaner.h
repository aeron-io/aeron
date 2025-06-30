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

#ifndef AERON_TERM_CLEANER_H
#define AERON_TERM_CLEANER_H

#include <stddef.h>
#include <stdint.h>
#include "util/aeron_bitutil.h"

#define AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH (4096u)

inline int64_t aeron_term_cleaner_block_start_position(int64_t start_position)
{
    return start_position - (start_position & (AERON_TERM_CLEANER_TERM_CLEANUP_BLOCK_LENGTH - 1));
}

#endif //AERON_TERM_CLEANER_H
