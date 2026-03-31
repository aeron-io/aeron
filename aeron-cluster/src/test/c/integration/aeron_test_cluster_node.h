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

#ifndef AERON_TEST_CLUSTER_NODE_H
#define AERON_TEST_CLUSTER_NODE_H

#include "aeron_test_clustered_media_driver.h"

/**
 * Thin wrapper around TestClusteredMediaDriver for backward compatibility.
 *
 * In the old pattern, TestClusterNode only launched driver+archive (no CM).
 * Now it delegates to TestClusteredMediaDriver for both modes:
 *   - start()  = launch driver+archive only (legacy, for tests that create CM manually)
 *   - stop()   = close the underlying TestClusteredMediaDriver
 *
 * For 3-node tests, each node has its own TestClusterNode wrapping its
 * own TestClusteredMediaDriver instance.
 */
class TestClusterNode
{
public:
    TestClusterNode(
        int node_index,
        int node_count,
        const std::string &base_dir,
        std::ostream &stream)
        : m_cmd(node_index, node_count, base_dir, stream),
          m_node_index(node_index)
    {
    }

    /**
     * Start driver+archive only (no CM). This is the legacy behavior
     * used by tests that manually create the CM agent.
     */
    void start()
    {
        if (m_cmd.launch_driver_only() < 0)
        {
            ::exit(EXIT_FAILURE);
        }
    }

    void stop()
    {
        m_cmd.close();
    }

    /** Access the underlying ClusteredMediaDriver (for full launch mode). */
    TestClusteredMediaDriver &clustered_media_driver() { return m_cmd; }

    const std::string &aeron_dir() const { return m_cmd.aeron_dir(); }
    const std::string &archive_dir() const { return m_cmd.archive_dir(); }
    const std::string &cluster_dir() const { return m_cmd.cluster_dir(); }
    const std::string &cluster_members() const { return m_cmd.cluster_members(); }
    const std::string &ingress_endpoints() const { return m_cmd.ingress_endpoints(); }
    int node_index() const { return m_node_index; }
    int consensus_port() const { return 20220 + m_node_index; }

private:
    TestClusteredMediaDriver m_cmd;
    int m_node_index;
};

#endif /* AERON_TEST_CLUSTER_NODE_H */
