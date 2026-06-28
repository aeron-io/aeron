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

#ifndef AERON_ERROR_H
#define AERON_ERROR_H

#include <string.h>
#include <stdbool.h>

#include "aeron_common.h"
#include "util/aeron_platform.h"

#define AERON_ERROR_MAX_TOTAL_LENGTH (8192)

typedef struct aeron_per_thread_error_stct
{
    int errcode;
    size_t offset;
    char errmsg[AERON_ERROR_MAX_TOTAL_LENGTH];
}
aeron_per_thread_error_t;

int aeron_errcode(void);
const char *aeron_errmsg(void);
void aeron_set_errno(int errcode);
const char *aeron_error_code_str(int errcode);
void aeron_err_set(int errcode, const char *function, const char *filename, int line_number, const char *format, ...);
void aeron_err_append(const char *function, const char *filename, int line_number, const char *format, ...);
void aeron_err_clear(void);

#if defined(AERON_COMPILER_MSVC)
bool aeron_error_dll_process_attach();
void aeron_error_dll_thread_detach();
void aeron_error_dll_process_detach();
void aeron_err_set_windows(
    int errcode, const char *function, const char *filename, int line_number, const char *format, ...);
void aeron_err_append_windows(
    int errcode, const char *function, const char *filename, int line_number, const char *format, ...);
#define __FILENAME__ (strrchr(__FILE__, '\\') ? strrchr(__FILE__, '\\') + 1 : __FILE__)
#define AERON_SET_ERR_WIN(errcode, fmt, ...) aeron_err_set_windows(errcode, __func__, __FILENAME__, __LINE__, fmt, __VA_ARGS__)
#define AERON_APPEND_ERR_WIN(errcode, fmt, ...) aeron_err_append_windows(errcode, __func__, __FILENAME__, __LINE__, fmt, __VA_ARGS__)
#else
#define __FILENAME__ (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__)
#endif

#define AERON_SET_ERR(errcode, fmt, ...) aeron_err_set(errcode, __func__, __FILENAME__, __LINE__, fmt, __VA_ARGS__)
#define AERON_APPEND_ERR(fmt, ...) aeron_err_append(__func__, __FILENAME__, __LINE__, fmt, __VA_ARGS__)
#define AERON_NULL_STR(v) NULL == v ? "NULL" : "OK"

#endif //AERON_ERROR_H
