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

/**
 * Inter-cluster bridge service for deterministic, bidirectional messaging
 * between a Matching Engine (ME) cluster and a Risk Management System (RMS)
 * cluster using Aeron UDP publications, Aeron Archive recording, and
 * {@link io.aeron.archive.client.ReplayMerge} for catch-up after failure.
 *
 * <h2>Design Principles (SOLID)</h2>
 * <ul>
 *   <li><b>Single Responsibility</b>: Each class owns exactly one concern —
 *       encoding ({@link io.aeron.cluster.bridge.BridgeMessageCodec}),
 *       persistence ({@link io.aeron.cluster.bridge.BridgeCheckpoint}),
 *       publishing ({@link io.aeron.cluster.bridge.BridgeSender}),
 *       consuming ({@link io.aeron.cluster.bridge.BridgeReceiver}),
 *       orchestration ({@link io.aeron.cluster.bridge.ClusterBridge}).</li>
 *   <li><b>Open/Closed</b>: {@link io.aeron.cluster.bridge.BridgeReceiver}
 *       accepts a {@link java.util.function.BiConsumer} handler, enabling new
 *       behaviours without modifying the receiver class.</li>
 *   <li><b>Dependency Inversion</b>: All components depend on Aeron
 *       abstractions ({@link io.aeron.Aeron}, {@link io.aeron.archive.client.AeronArchive}),
 *       not concrete driver implementations.</li>
 * </ul>
 *
 * <h2>Assumptions</h2>
 * <ul>
 *   <li>Both clusters run on the same host for the demo; production would use
 *       separate hosts with UDP multicast.</li>
 *   <li>Each direction has a single sender (no concurrent publishers on the
 *       same stream).</li>
 *   <li>Aeron Archive is co-located with the sender for {@code LOCAL} source
 *       recording.</li>
 * </ul>
 */
package io.aeron.cluster.bridge;
