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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "util/aeron_dlopen.h"
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "aeron_alloc.h"

#if defined(AERON_COMPILER_GCC)

const char *aeron_dlinfo(const void *addr, char *buffer, size_t max_buffer_length)
{
    buffer[0] = '\0';
    Dl_info info;

    if (dladdr(addr, &info) <= 0)
    {
        return buffer;
    }

    snprintf(buffer, max_buffer_length - 1, "(%s:%s)", info.dli_fname, info.dli_sname);
    return buffer;
}

const char *aeron_dlinfo_func(void (*func)(void), char *buffer, size_t max_buffer_length)
{
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#endif
    void *addr = (void *)func;
#if defined(AERON_COMPILER_GCC)
#pragma GCC diagnostic pop
#endif
    return aeron_dlinfo(addr, buffer, max_buffer_length);
}

#elif defined(AERON_COMPILER_MSVC)

#include "concurrent/aeron_counters_manager.h"
#include "aeronc.h"
#include <Windows.h>

void *aeron_dlsym_fallback(LPCSTR name)
{
    return NULL;
}

static HMODULE *modules = NULL;
static size_t modules_size = 0;
static size_t modules_capacity = 10;

HMODULE GetCurrentModule()
{
    HMODULE hModule = NULL;
    if (GetModuleHandleEx(GET_MODULE_HANDLE_EX_FLAG_FROM_ADDRESS, (LPCTSTR)GetCurrentModule, &hModule))
    {
        return hModule;
    }

    return NULL;
}

void aeron_init_dlopen_support()
{
    if (NULL == modules)
    {
        modules = (HMODULE*)malloc(sizeof(HMODULE) * modules_capacity);
        memset(modules, 0, sizeof(HMODULE) * modules_capacity);
        modules[0] = GetCurrentModule();
        modules_size = modules[0] != NULL;
    }
}

void *aeron_dlsym(void *module, const char *name)
{
    aeron_init_dlopen_support();

    if (RTLD_DEFAULT == module)
    {
        for (size_t i = 1; i <= modules_size; i++)
        {
            void *res = aeron_dlsym(modules[modules_size - i], name);
            if (NULL != res)
            {
                return res;
            }
        }

        return aeron_dlsym_fallback(name);
    }

    if (RTLD_NEXT == module)
    {
        BOOL firstFound = FALSE;
        for (size_t i = 1; i <= modules_size; i++)
        {
            void *res = aeron_dlsym(modules[modules_size - i], name);
            if (NULL != res && firstFound)
            {
                return res;
            }

            if (NULL != res && !firstFound)
            {
                firstFound = TRUE;
            }
        }

        return aeron_dlsym_fallback(name);
    }

    return GetProcAddress((HMODULE)module, name);
}

void *aeron_dlopen(const char *filename)
{
    aeron_init_dlopen_support();

    HMODULE module = LoadLibraryA(filename);

    if (modules_size == modules_capacity)
    {
        modules_capacity = modules_capacity * 2;
        modules = (HMODULE*)realloc(modules, sizeof(HMODULE) * modules_capacity);
    }

    modules[modules_size++] = module;

    return module;
}

char *aeron_dlerror()
{
    DWORD errorMessageID = GetLastError();
    LPSTR messageBuffer = NULL;
    FormatMessageA(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL,
        errorMessageID,
        MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        (LPSTR)&messageBuffer,
        0,
        NULL);

    // Leak

    return messageBuffer;
}

const char *aeron_dlinfo_func(void (*func)(void), char *buffer, size_t max_buffer_length)
{
    buffer[0] = '\0';
    return buffer;
}

const char *aeron_dlinfo(const void *addr, char *buffer, size_t max_buffer_length)
{
    buffer[0] = '\0';
    return buffer;
}

#else
#error Unsupported platform!
#endif

#define AERON_MAX_DL_LIBS_LEN (4094)
#define AERON_MAX_DL_LIB_NAMES (10)

int aeron_dl_load_libs(aeron_dl_loaded_libs_state_t **state, const char *libs)
{
    char libs_dup[AERON_MAX_DL_LIBS_LEN] = { 0 };
    char *lib_names[AERON_MAX_DL_LIB_NAMES] = { 0 };
    aeron_dl_loaded_libs_state_t *_state;
    const size_t libs_length = strlen(libs);

    *state = NULL;

    if (NULL == aeron_dlsym(RTLD_DEFAULT, "aeron_driver_context_init"))
    {
        AERON_SET_ERR(EPERM, "%s", "dynamic loading of libraries not supported with a statically link driver");
        return -1;
    }

    if (libs_length >= (size_t)AERON_MAX_DL_LIBS_LEN)
    {
        AERON_SET_ERR(
            EINVAL,
            "dl libs list too long, must have: %" PRIu64 " < %d",
            (uint64_t)libs_length, AERON_MAX_DL_LIBS_LEN);
        return -1;
    }

    strncpy(libs_dup, libs, AERON_MAX_DL_LIBS_LEN - 1);

    const int num_libs = aeron_tokenise(libs_dup, ',', AERON_MAX_DL_LIB_NAMES, lib_names);

    if (-ERANGE == num_libs)
    {
        AERON_SET_ERR(EINVAL, "Too many dl libs defined, limit %d: %s", AERON_MAX_DL_LIB_NAMES, libs);
        return -1;
    }
    else if (num_libs < 0)
    {
        AERON_SET_ERR(EINVAL, "Failed to parse dl libs: %s", libs != NULL ? libs : "(null)");
        return -1;
    }

    if (aeron_alloc((void **)&_state, sizeof(aeron_dl_loaded_libs_state_t)) < 0 ||
        aeron_alloc((void **)&_state->libs, sizeof(aeron_dl_loaded_lib_state_t) * num_libs) < 0)
    {
        AERON_APPEND_ERR("could not allocate dl_loaded_libs, num_libs: %d", num_libs);
        return -1;
    }
    _state->num_libs = (size_t)num_libs;

    for (int i = 0; i < num_libs; i++)
    {
        const char *lib_name = lib_names[i];
        aeron_dl_loaded_lib_state_t *lib = &_state->libs[i];

        if (NULL == (lib->handle = aeron_dlopen(lib_name)))
        {
            AERON_SET_ERR(EINVAL, "failed to load dl_lib %s: %s", lib_name, aeron_dlerror());
            return -1;
        }
    }

    *state = _state;
    return 0;
}

int aeron_dl_load_libs_delete(aeron_dl_loaded_libs_state_t *state)
{
    if (NULL != state)
    {
        for (size_t i = 0; i < state->num_libs; i++)
        {
            aeron_dl_loaded_lib_state_t *lib = &state->libs[i];

#if defined(AERON_COMPILER_GCC)
            dlclose(lib->handle);
#elif defined(AERON_COMPILER_MSVC)
            FreeLibrary(lib->handle);
#else
#error Unsupported platform!
#endif
        }

        aeron_free(state->libs);
        aeron_free(state);
    }

    return 0;
}
