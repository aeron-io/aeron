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

#ifndef AERON_COUNTER_H
#define AERON_COUNTER_H

#include <cstdint>
#include <memory>

#include "concurrent/AtomicCounter.h"
#include "concurrent/CountersReader.h"

namespace aeron
{

using namespace aeron::concurrent;

class Aeron;
class ClientConductor;

class Counter : public AtomicCounter
{
public:
    /// @cond HIDDEN_SYMBOLS
    Counter(
        const std::shared_ptr<Aeron> &aeronRef,
        CountersReader &reader,
        aeron_counter_t *counter,
        std::int64_t registrationId) :
        AtomicCounter(counter),
        m_aeronRef(aeronRef),
        m_reader(reader),
        m_registrationId(registrationId)
    {
    }
    /// @endcond

    Counter(CountersReader &reader, std::int64_t registrationId, std::int32_t counterId) :
        AtomicCounter(reader.getCounterAddress(counterId), registrationId, counterId),
        m_reader(reader),
        m_registrationId(registrationId)
    {
    }

    inline std::int64_t registrationId() const
    {
        return m_registrationId;
    }

    std::int32_t state() const
    {
        return m_reader.getCounterState(id());
    }

    std::string label() const
    {
        return m_reader.getCounterLabel(id());
    }

    bool isClosed() const
    {
        return aeron_counter_is_closed(counter());
    }

    /// @cond HIDDEN_SYMBOLS
    aeron_counter_t *c_counter() const
    {
        return counter();
    }
    /// @endcond

private:
    std::shared_ptr<Aeron> m_aeronRef; // ensure Aeron instance is being deleted after its children
    CountersReader &m_reader;
    std::int64_t m_registrationId;
};

}
#endif //AERON_COUNTER_H
