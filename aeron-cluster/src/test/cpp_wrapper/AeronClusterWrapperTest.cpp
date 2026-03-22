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

#include <gtest/gtest.h>

#include "client/cluster/AeronCluster.h"
#include "client/cluster/EgressPoller.h"

using namespace aeron::cluster::client;

/**
 * Compile-only / unit tests that do not require a running cluster.
 * System tests that connect to a real cluster node will be added in a
 * separate test suite once the embedded Java cluster infrastructure is in place.
 */

TEST(AeronClusterWrapperTest, shouldConstructContextWithDefaultValues)
{
    Context ctx;

    EXPECT_EQ(101, ctx.ingressStreamId());
    EXPECT_EQ(102, ctx.egressStreamId());
    EXPECT_EQ(5000000000ULL, ctx.messageTimeoutNs());
    EXPECT_EQ(3U, ctx.messageRetryAttempts());
    EXPECT_TRUE(ctx.ingressChannel().empty());
    EXPECT_TRUE(ctx.egressChannel().empty());
}

TEST(AeronClusterWrapperTest, shouldSetAndGetContextFields)
{
    Context ctx;

    ctx.ingressChannel("aeron:udp?endpoint=localhost:20121")
       .ingressStreamId(101)
       .egressChannel("aeron:udp?endpoint=localhost:20122")
       .egressStreamId(102)
       .messageTimeoutNs(10'000'000'000ULL)
       .clientName("test-client");

    EXPECT_EQ("aeron:udp?endpoint=localhost:20121", ctx.ingressChannel());
    EXPECT_EQ(101, ctx.ingressStreamId());
    EXPECT_EQ("aeron:udp?endpoint=localhost:20122", ctx.egressChannel());
    EXPECT_EQ(102, ctx.egressStreamId());
    EXPECT_EQ(10'000'000'000ULL, ctx.messageTimeoutNs());
    EXPECT_EQ("test-client", ctx.clientName());
}

TEST(AeronClusterWrapperTest, shouldSetIngressEndpoints)
{
    Context ctx;
    ctx.ingressEndpoints("0=localhost:20121,1=localhost:20131,2=localhost:20141");
    EXPECT_EQ("0=localhost:20121,1=localhost:20131,2=localhost:20141", ctx.ingressEndpoints());
}
