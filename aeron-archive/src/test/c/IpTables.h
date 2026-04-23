/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

#ifndef AERON_IPTABLES_H
#define AERON_IPTABLES_H

#include <stdexcept>
#include <string>
#include <utility>

class IpTables
{
    std::string m_chainName;

    static bool runCmd(const bool ignoreError, const std::string& cmd)
    {
        const auto rc = std::system(cmd.c_str());
        if (!ignoreError && rc != 0)
        {
            throw std::runtime_error(cmd + " returned " + std::to_string(rc));
        }
        return rc == 0;
    }

    void createChain() const
    {
        runCmd(true, "sudo -n iptables --new-chain " + m_chainName);
        runCmd(false, "sudo -n iptables --append " + m_chainName + " --jump RETURN");
    }

    void deleteChain() const
    {
        runCmd(true, "sudo -n iptables -X " + m_chainName);
    }

    void addToInput() const
    {
        runCmd(true, "sudo -n iptables --insert INPUT --jump " + m_chainName);
    }

    void removeFromInput() const
    {
        const std::string cmd = "sudo -n iptables --delete INPUT --jump " + m_chainName + " >/dev/null 2>&1";
        while (runCmd(true, cmd))
            ;
    }

    void setUpChain() const
    {
        createChain();
        flushChain();
        addToInput();
    }

    void tearDownChain() const
    {
        flushChain();
        removeFromInput();
        deleteChain();
    }

public:
    explicit IpTables(std::string chainName) : m_chainName(std::move(chainName))
    {
        setUpChain();
    }

    ~IpTables()
    {
        tearDownChain();
    }

    void dropUdpTrafficBetweenHosts(
        const std::string& srcHost,
        const int srcPort,
        const std::string& dstHost,
        const int dstPort) const
    {
        runCmd(false, "sudo -n iptables --insert " + m_chainName + " --ipv4 --protocol udp" +
            " --source " + srcHost + (srcPort > 0 ? " --source-port " + std::to_string(srcPort) : "") +
            " --destination " + dstHost + (dstPort > 0 ? " --destination-port " + std::to_string(dstPort) : "") +
            " --jump DROP");
    }

    void flushChain() const
    {
        runCmd(true, "sudo -n iptables --flush " + m_chainName);
    }
};

#endif //AERON_IPTABLES_H
