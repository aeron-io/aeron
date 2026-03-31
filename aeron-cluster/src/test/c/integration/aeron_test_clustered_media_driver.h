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

#ifndef AERON_TEST_CLUSTERED_MEDIA_DRIVER_H
#define AERON_TEST_CLUSTERED_MEDIA_DRIVER_H

#include <string>
#include <iostream>

/**
 * C equivalent of Java ClusteredMediaDriver: bundles MediaDriver + Archive + ConsensusModule.
 *
 * This is a TEST HELPER. It:
 * 1. Launches an aeron_archiving_media_driver_t (driver + archive, with background threads)
 * 2. Optionally creates and starts an aeron_consensus_module_agent_t (CM agent, with background thread)
 *
 * The CM agent creates its own Aeron client from aeron_dir and its own archive client
 * connected via IPC to the in-process archive, matching Java's pattern where each component
 * manages its own connections.
 *
 * The header deliberately does NOT include service/consensus headers to avoid typedef
 * conflicts with the cluster client module. Implementation is in the .cpp file.
 *
 * Usage:
 *   TestClusteredMediaDriver cmd(0, 1, baseDir, std::cout);
 *   cmd.launch();           // full: driver + archive + CM
 *   cmd.launch_driver_only(); // partial: driver + archive only
 *   cmd.is_leader();
 *   cmd.close();
 */
class TestClusteredMediaDriver
{
public:
    TestClusteredMediaDriver(
        int node_index,
        int node_count,
        const std::string &base_dir,
        std::ostream &stream);

    ~TestClusteredMediaDriver();

    /** Launch only MediaDriver + Archive (no CM). Returns 0 on success. */
    int launch_driver_only();

    /** Launch all three: MediaDriver + Archive + ConsensusModule. Returns 0 on success. */
    int launch();

    /** Close all components (CM thread, CM agent, archiving driver). */
    void close();

    /** Returns true if CM agent is leader. */
    bool is_leader() const;

    const std::string &aeron_dir() const;
    const std::string &archive_dir() const;
    const std::string &cluster_dir() const;
    const std::string &cluster_members() const;
    const std::string &ingress_endpoints() const;
    int node_index() const;

private:
    struct Impl;
    Impl *m_impl;
};

#endif /* AERON_TEST_CLUSTERED_MEDIA_DRIVER_H */
