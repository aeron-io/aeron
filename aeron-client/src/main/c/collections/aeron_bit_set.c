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

#include "collections/aeron_bit_set.h"

extern int aeron_bit_set_stack_alloc(
    size_t bit_set_length, uint64_t *static_array, size_t static_array_len, aeron_bit_set_t *bit_set);

extern int aeron_bit_set_stack_init(
    size_t bit_set_length,
    uint64_t *static_array,
    size_t static_array_len,
    bool initial_value,
    aeron_bit_set_t *bit_set);

extern void aeron_bit_set_heap_free(aeron_bit_set_t *bit_set);

extern int aeron_bit_set_heap_alloc(size_t bit_set_length, aeron_bit_set_t **bit_set);

extern int aeron_bit_set_heap_init(size_t bit_set_length, bool initial_value, aeron_bit_set_t **bit_set);

extern void aeron_bit_set_stack_free(aeron_bit_set_t *bit_set);

extern int aeron_bit_set_init(aeron_bit_set_t *bit_set, bool initial_value);

extern int aeron_bit_set_get(aeron_bit_set_t *bit_set, size_t bit_index, bool *value);

extern int aeron_bit_set_set(aeron_bit_set_t *bit_set, size_t bit_index, bool value);

extern int aeron_bit_set_find_first(aeron_bit_set_t *bit_set, bool value, size_t *bit_index);
