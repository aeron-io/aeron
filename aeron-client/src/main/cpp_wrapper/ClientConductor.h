/*
 * Copyright 2014-2024 Real Logic Limited.
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

#ifndef AERON_CLIENT_CONDUCTOR_H
#define AERON_CLIENT_CONDUCTOR_H

#include <chrono>
#include "aeronc.h"

namespace aeron
{

typedef std::function<long long()> epoch_clock_t;
typedef std::function<long long()> nano_clock_t;

class ClientConductor
{
public:
    explicit ClientConductor(aeron_t *aeron) : m_aeron(aeron)
    {
    }

    inline void onStart()
    {
    }

    int doWork()
    {
        return aeron_main_do_work(m_aeron);
    }

    void onClose()
    {

    }

private:
    aeron_t *m_aeron;
};

inline long long currentTimeMillis()
{
    using namespace std::chrono;

    system_clock::time_point now = system_clock::now();
    milliseconds ms = duration_cast<milliseconds>(now.time_since_epoch());

    return ms.count();
}

inline long long systemNanoClock()
{
    using namespace std::chrono;

    high_resolution_clock::time_point now = high_resolution_clock::now();
    nanoseconds ns = duration_cast<nanoseconds>(now.time_since_epoch());

    return ns.count();
}

}

#endif
