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

#ifndef AERON_EMBEDDED_MEDIA_DRIVER_H
#define AERON_EMBEDDED_MEDIA_DRIVER_H

#if defined(__linux__)
#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#endif

#include <string>
#include <thread>
#include <atomic>
#include <stdexcept>
#include <functional>

extern "C"
{
#include "aeronmd.h"
}

namespace aeron
{

const char *TERMINATION_KEY = "Exit please";

class EmbeddedMediaDriver
{
public:
    explicit EmbeddedMediaDriver(std::function<void(aeron_driver_context_t *)> setContextFunc = [](aeron_driver_context_t *) {})
        : m_setContextFunc(setContextFunc)
    {
    }

    ~EmbeddedMediaDriver()
    {
        if (nullptr != m_driver && 0 != aeron_driver_close(m_driver))
        {
            fprintf(stderr, "ERROR: driver close (%d) %s\n", aeron_errcode(), aeron_errmsg());
        }

        if (nullptr != m_context && 0 != aeron_driver_context_close(m_context))
        {
            fprintf(stderr, "ERROR: driver context close (%d) %s\n", aeron_errcode(), aeron_errmsg());
        }
    }

    void driverLoop()
    {
        while (m_running)
        {
            aeron_driver_main_idle_strategy(m_driver, aeron_driver_main_do_work(m_driver));
        }
    }

    void stop()
    {
        m_running = false;
        if (m_thread.joinable())
        {
            m_thread.join();
        }
    }

    void joinAndClose()
    {
        m_running = false;
        if (m_thread.joinable())
        {
            m_thread.join();
        }

        aeron_driver_close(m_driver);
        aeron_driver_context_close(m_context);

        m_driver = nullptr;
        m_context = nullptr;
    }

    void start()
    {
        if (init() < 0)
        {
            throw std::runtime_error("failed to initialize");
        }

        m_thread = std::thread(
            [&]()
            {
                driverLoop();
            });
    }

    const char *directory()
    {
        return aeron_driver_context_get_dir(m_context);
    }

protected:
    int init()
    {
        if (aeron_driver_context_init(&m_context) < 0)
        {
            fprintf(stderr, "ERROR: context init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        aeron_driver_context_set_threading_mode(m_context, AERON_THREADING_MODE_SHARED);
        aeron_driver_context_set_dir_delete_on_start(m_context, true);
        aeron_driver_context_set_dir_delete_on_shutdown(m_context, true);
        aeron_driver_context_set_shared_idle_strategy(m_context, "sleep-ns");
        aeron_driver_context_set_term_buffer_sparse_file(m_context, true);
        aeron_driver_context_set_term_buffer_length(m_context, 64 * 1024);
        aeron_driver_context_set_driver_termination_validator(m_context, validateTermination, nullptr);
        aeron_driver_context_set_driver_termination_hook(m_context, terminationHook, this);

        m_setContextFunc(m_context);

        if (aeron_driver_init(&m_driver, m_context) < 0)
        {
            fprintf(stderr, "ERROR: driver init (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        if (aeron_driver_start(m_driver, true) < 0)
        {
            fprintf(stderr, "ERROR: driver start (%d) %s\n", aeron_errcode(), aeron_errmsg());
            return -1;
        }

        return 0;
    }

private:
    std::atomic<bool> m_running = { true };
    std::thread m_thread;
    aeron_driver_context_t *m_context = nullptr;
    aeron_driver_t *m_driver = nullptr;
    std::function<void(aeron_driver_context_t *)> m_setContextFunc;

    static bool validateTermination(void *state, uint8_t *buffer, int32_t length)
    {
        auto key_length = static_cast<int32_t>(strlen(TERMINATION_KEY));
        return key_length == length && 0 == memcmp(TERMINATION_KEY, buffer, static_cast<size_t>(length));
    }

    static void terminationHook(void *clientd)
    {
        auto *driver = static_cast<EmbeddedMediaDriver *>(clientd);
        driver->m_running = false;
    }
};

}

#endif //AERON_EMBEDDED_MEDIA_DRIVER_H
