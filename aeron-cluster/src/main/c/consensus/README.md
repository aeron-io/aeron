# Aeron Cluster -- Consensus Module (C)

This directory implements the `ConsensusModuleAgent` and its supporting
components, the C equivalent of Java's `io.aeron.cluster` consensus
infrastructure.

**Note:** This is internal cluster infrastructure. Applications interact with
the cluster through the client API (`client/`) or the service API (`service/`).
The consensus module is started as a standalone agent that manages Raft-based
leader election, log replication, and session routing.

## Key Components

| Component | Purpose |
|-----------|---------|
| `ConsensusModuleAgent` | Main duty-cycle: polls ingress, consensus, and service adapters; drives election and session management |
| `Election` | 18-state Raft variant implementing leader election, canvassing, nomination, voting, and log catch-up |
| `SessionManager` | Tracks client sessions, manages open/close lifecycle, and routes messages to the log |
| `RecordingLog` | Memory-mapped persistent log of leadership terms and snapshots (`recording.log`) |
| `ClusterMember` | Per-member state: publications, positions, liveness tracking |
| `ConsensusPublisher` | Sends inter-node consensus messages (canvass, vote, append/commit position) |
| `LogPublisher` / `LogAdapter` | Writes to and reads from the replicated log |
| `EgressPublisher` | Sends responses and events to external clients |
| `TimerService` | Ordered timer queue for scheduled callbacks |
| `CmSnapshotTaker` / `CmSnapshotLoader` | Consensus module snapshot serialisation and recovery |
| `RecordingReplication` | Replicates archive recordings between nodes for catch-up |
| `PendingMessageTracker` | Tracks service messages awaiting commit |

## Lifecycle

1. The consensus module context is configured and concluded.
2. `aeron_consensus_module_agent_create()` allocates the agent, opens archive
   connections, and loads the recording log.
3. The agent enters its duty-cycle (`do_work`), which drives the election state
   machine, polls adapters, and manages sessions.
4. On election completion, the leader begins accepting client sessions and
   replicating the log to followers.
5. `aeron_consensus_module_agent_close()` terminates the agent and releases
   resources.

## Source Files

All files in this directory follow the `aeron_cluster_` or `aeron_consensus_module_`
prefix convention. Header files declare the public structures and functions;
implementation files contain the logic. See `STATUS.md` in the parent directory
for per-component completion estimates.
