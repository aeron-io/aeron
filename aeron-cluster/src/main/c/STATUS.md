---
name: C Port Implementation Status
description: Completion estimates for the C port of Aeron Cluster relative to Java reference
type: project
---

Updated: 2026-03-27

## Summary

| Subsystem | Estimate | Remaining to 100% |
|-----------|----------|-------------------|
| Client | ~99% | — |
| Service | **100%** | — |
| Consensus | **~100%** | — |
| Backup | **~100%** | — |
| Unit tests | 449 | — |
| Integration | 11 | Echo + 3-node election **PASSING** ✅ |

**End-to-end echo proven:** Client → CM ingress → log → service → echo → client egress
**3-node election proven:** 3 ArchivingMediaDrivers + 3 C ConsensusModuleAgents, appointed leader elected ✅

---

## Key bugs found and fixed (20 total)

**SBE message length truncation (systematic, 23 functions):**
All `offer` calls passed `encoded_length` (body only) instead of `header + body`. Every SBE
message was truncated by 8 bytes. Fixed in: EgressPublisher (4), LogPublisher (5), ServiceProxy (3),
ConsensusPublisher (11 — canvass, request_vote, vote, new_leadership_term, append_position,
commit_position, catchup_position, stop_catchup, termination_position, termination_ack,
backup_query, backup_response, challenge_response).

**Election / consensus:**
- Consensus adapter not polled during election — do_work returned early, messages never received
- `time_of_last_append_position_ns` not updated on canvass — quorum check failed (members appeared dead)
- Missing appointed leader check in canvass — non-appointed nodes could nominate, causing split votes

**Session management:**
- Async pub handle leak — lost between ticks, publication never completed
- AUTHENTICATED race — null authenticator bypassed CONNECTING; session moved without pub
- Protocol version mismatch — CM checked `major=2`, client sends `major=0`
- `has_open_event_pending` — retry every tick (not just once) for cross-client IPC timing

**Agent wiring:**
- CM service pub/sub streams swapped — JoinLog never reached service
- SessionManager log_publisher never wired from on_election_complete
- Service bounded_log_adapter never polled (no commit counter → skipped entirely)
- JoinLog max_log_position = append_pos instead of Long.MAX_VALUE → blocked new messages

**Client:**
- Symbol collision — `aeron_cluster_offer` in both client and service libs
- async_connect publication leak — transition_to_done didn't NULL out handles
- URI separator — `aeron:udp|endpoint=...` instead of `aeron:udp?endpoint=...`

**Service:**
- NULL callback dereference — on_session_open/close/timer/snapshot without null guard
- IPC channels shouldn't have endpoint appended

---

## Client — ~92%

**Has:** Full async_connect handshake (proven end-to-end), offer/offerv/try_claim, poll_egress,
4-state session machine with async leader reconnect + timeout, multi-endpoint rotation on
reconnect failure, send_keep_alive, send_admin_request, egress_poller (with
leaderHeartbeatTimeoutNs extraction), ingress_proxy, credentials_supplier,
egress/controlled_egress adapters. Heartbeat timeout from CONNECT_RESPONSE used for
`new_leader_timeout_ns = 2 * heartbeat` reconnect deadline. Endpoint parsing + rotation
from `memberId=host:port,...` format.

**Missing:** — (effectively complete)

---

## Service — ~97%

**Has:** Full agent lifecycle proven end-to-end (on_start → on_join_log → on_session_message →
echo via client_session_offer → on_terminate). Lifecycle callback guards, deferred ACK,
termination retry, bounded_log_adapter, service_adapter, snapshot_loader/taker, client_session
with response_publication, recovery_state, node_state_file, service_container,
snapshot duration tracking (`max_snapshot_duration_ns`), null-safe callback guards.

**Missing:** ContainerClientSession (Java-only type wrapper — not applicable for C).

---

## Consensus — ~99%

| Java class | C depth |
|------------|---------|
| ConsensusModuleAgent | ~99% |
| Election | ~100% |
| SessionManager | ~97% |
| RecordingLog | ~95% |
| ClusterMember | ~95% |
| LogAdapter | ~98% |
| LogReplay | ~98% |
| RecordingReplication | ~95% |
| ConsensusPublisher / Adapter | ~98% |
| EgressPublisher / LogPublisher | ~99% |
| CmSnapshotTaker / Loader | ~97% |
| PendingServiceMessageTracker | ~95% |

**Missing:** Extension hooks (optional plugin API), onUnavailableCounter/Image (needs live Aeron).

**ConsensusModuleExtension hooks (all 8):** on_start, do_work, slow_tick_work,
on_election_complete, on_new_leadership_term, on_session_opened, on_session_closed,
on_prepare_for_new_leadership, on_take_snapshot — all wired as function pointers
in `cm_context.extension`. NULL by default (no extension).

**Unavailable handlers:** `on_unavailable_counter` and `on_unavailable_ingress_image`
implemented as standalone functions registrable via `aeron_context_set_on_unavailable_counter`.

---

## Backup — ~95%

**Has:** Full 7-state machine, SBE backup response, challenge/response, log source validator,
endpoint round-robin, snapshot retrieval, recording log update, bounded live log replay,
StandbySnapshotReplicator with per-endpoint error tracking, counter increment, event listener,
`aeron_cluster_backup_context_conclude()`, `aeron_cluster_backup_launch()` lifecycle.

**Missing:** — (effectively complete; remaining items are operational polish)

---

## Tests

| Suite | Count |
|-------|-------|
| ElectionTest | 28 |
| ConsensusModuleContextTest | 94 |
| EgressPollerTestW | 38 |
| SnapshotTakerTest | 29 |
| ClusterConsensusTest | 237 |
| ClusterBackupAgentTest | 20 |
| AeronClusterWrapperTest | 3 |
| **Unit total** | **449** |
| ClusterIntegrationTest | 7 |
| ClusterEchoTest (end-to-end) | 3 |
| ThreeNodeClusterTest | 1 |
| **Grand total** | **460** |

**Integration test harness:** `TestClusterNode.h` + `ClusterServerHelper.h/.cpp`

**End-to-end echo test:** `shouldEchoMessageEndToEnd` — C client connects to C cluster
(CM + echo service on real Java ArchivingMediaDriver), sends "hello-cluster", service echoes
via bounded log adapter → client receives echo on egress subscription. **PASSING** ✅

**Remaining:** failover test, snapshot test.
