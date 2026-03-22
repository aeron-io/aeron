# Aeron Cluster C/C++ — Test Migration Guide

Every Java test must have a C/C++ equivalent.  This document maps each Java test
file and method to its C counterpart, classifies the test type, and tracks
implementation status.

**Test type legend:**
- `U` Unit — no I/O, no Aeron driver (fastest, implement first)
- `M` Mock/isolated — needs synthetic SBE buffers or fake publications
- `I` Integration — needs a real Aeron driver (IPC, no cluster peer)
- `S` System — needs embedded Java cluster node

---

## Test file mapping

| Java test file | C test file | Status |
|----------------|------------|--------|
| `client/AeronClusterAsyncConnectTest` | `client/AeronClusterAsyncConnectTest.cpp` | ⬜ |
| `client/AeronClusterContextTest` | `client/AeronClusterContextTest.cpp` | ⬜ |
| `client/AeronClusterTest` | `client/AeronClusterTest.cpp` | ⬜ |
| `client/EgressAdapterTest` | `client/EgressAdapterTest.cpp` | ⬜ |
| `client/EgressPollerTest` | `client/EgressPollerTest.cpp` | ⬜ |
| `RecordingLogTest` | `consensus/ClusterConsensusTest.cpp` (partial) | 🔶 |
| `ElectionTest` | `consensus/ClusterConsensusTest.cpp` (partial) | 🔶 |
| `ClusterMemberTest` | `consensus/ClusterConsensusTest.cpp` (partial) | 🔶 |
| `PriorityHeapTimerServiceTest` | `consensus/ClusterConsensusTest.cpp` (partial) | 🔶 |
| `PendingServiceMessageTrackerTest` | `consensus/ClusterConsensusTest.cpp` (partial) | 🔶 |
| `ConsensusModuleContextTest` | `consensus/ConsensusModuleContextTest.cpp` | ⬜ |
| `ConsensusModuleAgentTest` | `consensus/ConsensusModuleAgentTest.cpp` | ⬜ |
| `ConsensusModuleSnapshotTakerTest` | `consensus/ConsensusModuleSnapshotTakerTest.cpp` | ⬜ |
| `SessionManagerTest` | `consensus/ClusterConsensusTest.cpp` | ⬜ |
| `service/ClusteredServiceContainerContextTest` | `service/ClusteredServiceContextTest.cpp` | ⬜ |
| `service/ClusteredServiceAgentTest` | `service/ClusteredServiceAgentTest.cpp` | ⬜ |
| `service/ServiceSnapshotTakerTest` | `service/ServiceSnapshotTakerTest.cpp` | ⬜ |

---

## 1. `RecordingLogTest` → `consensus/ClusterConsensusTest.cpp`

| # | Java method | C name | Type | Done |
|---|-------------|--------|------|------|
| 1 | `shouldCreateNewIndex` | `shouldCreateNewIndex` | U | ✅ (via shouldCreateAndReloadTermEntry) |
| 2 | `shouldAppendAndThenReloadLatestSnapshot` | `shouldAppendAndThenReloadLatestSnapshot` | U | ✅ |
| 3 | `shouldIgnoreIncompleteSnapshotInRecoveryPlan` | `shouldIgnoreIncompleteSnapshotInRecoveryPlan` | U | ⬜ |
| 4 | `shouldIgnoreInvalidMidSnapshotInRecoveryPlan` | `shouldIgnoreInvalidMidSnapshotInRecoveryPlan` | U | ⬜ |
| 5 | `shouldIgnoreInvalidTermInRecoveryPlan` | `shouldIgnoreInvalidTermInRecoveryPlan` | U | ⬜ |
| 6 | `shouldAppendAndThenCommitTermPosition` | `shouldAppendAndThenCommitTermPosition` | U | ✅ |
| 7 | `shouldRemoveEntry` | `shouldRemoveEntry` | U | ⬜ |
| 8 | `shouldCorrectlyOrderSnapshots` | `shouldCorrectlyOrderSnapshots` | U | ⬜ |
| 9 | `shouldInvalidateLatestSnapshot` | `shouldInvalidateLatestSnapshot` | U | ⬜ |
| 10 | `shouldRecoverSnapshotsMidLogMarkedInvalid` | `shouldRecoverSnapshotsMidLogMarkedInvalid` | U | ⬜ |
| 11 | `shouldRecoverSnapshotsLastInLogMarkedWithInvalid` | `shouldRecoverSnapshotsLastInLogMarkedWithInvalid` | U | ⬜ |
| 12 | `shouldNotAllowInvalidateOfSnapshotWithoutParentTerm` | `shouldNotAllowInvalidateOfSnapshotWithoutParentTerm` | U | ⬜ |
| 13 | `shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly` | `shouldFailToRecoverSnapshotsMarkedInvalidIfFieldsDoNotMatchCorrectly` | U | ⬜ |
| 14 | `shouldFindSnapshotAtOrBeforeOrLowest` | `shouldFindSnapshotAtOrBeforeOrLowest` | U | ⬜ |
| 15 | `shouldAppendTermWithLeadershipTermIdOutOfOrder` | `shouldAppendTermWithLeadershipTermIdOutOfOrder` | U | ⬜ |
| 16 | `shouldAppendSnapshotWithLeadershipTermIdOutOfOrder` | `shouldAppendSnapshotWithLeadershipTermIdOutOfOrder` | U | ⬜ |
| 17 | `appendTermShouldRejectNullValueAsRecordingId` | `appendTermShouldRejectNullValueAsRecordingId` | U | ⬜ |
| 18 | `appendSnapshotShouldRejectNullValueAsRecordingId` | `appendSnapshotShouldRejectNullValueAsRecordingId` | U | ⬜ |
| 19 | `appendTermShouldNotAcceptDifferentRecordingIds` | `appendTermShouldNotAcceptDifferentRecordingIds` | U | ⬜ |
| 20 | `appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId` | `appendTermShouldOnlyAllowASingleValidTermForTheSameLeadershipTermId` | U | ⬜ |
| 21 | `entriesInTheRecordingLogShouldBeSorted` | `entriesInTheRecordingLogShouldBeSorted` | U | ⬜ |
| 22 | `shouldNotCreateInitialTermWithMinusOneTermId` | `shouldNotCreateInitialTermWithMinusOneTermId` | U | ⬜ |
| 23 | `shouldBackFillPriorTerm` | `shouldBackFillPriorTerm` | U | ⬜ |
| 24 | `shouldThrowIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified` | `shouldReturnErrorIfLastTermIsUnfinishedAndTermBaseLogPositionIsNotSpecified` | U | ⬜ |
| 25 | `shouldDetermineIfSnapshotIsInvalid` | `shouldDetermineIfSnapshotIsInvalid` | U | ⬜ |
| 26 | `shouldInsertStandbySnapshotInRecordingLog` | `shouldInsertStandbySnapshotInRecordingLog` | U | ⬜ |
| 27 | `shouldInvalidateLatestAnySnapshots` | `shouldInvalidateLatestAnySnapshots` | U | ⬜ |
| 28 | `shouldNotIncludeStandbySnapshotInRecoveryPlan` | `shouldNotIncludeStandbySnapshotInRecoveryPlan` | U | ⬜ |
| 29 | `shouldGetLatestStandbySnapshotsGroupedByEndpoint` | `shouldGetLatestStandbySnapshotsGroupedByEndpoint` | U | ⬜ |
| 30 | `shouldInvalidateLatestSnapshotIgnoringStandbySnapshots` | `shouldInvalidateLatestSnapshotIgnoringStandbySnapshots` | U | ⬜ |
| 31 | `shouldHandleEntriesStraddlingPageBoundary` | `shouldHandleEntriesStraddlingPageBoundary` | U | ⬜ |
| 32 | `shouldRejectSnapshotEntryIfEndpointIsTooLong` | `shouldRejectSnapshotEntryIfEndpointIsTooLong` | U | ⬜ |
| 33 | `shouldCreateRecoveryPlan` | `shouldCreateRecoveryPlan` | U | ✅ |

---

## 2. `PriorityHeapTimerServiceTest` → `consensus/ClusterConsensusTest.cpp`

| # | Java method | C name | Type | Done |
|---|-------------|--------|------|------|
| 1 | `pollIsANoOpWhenNoTimersWhereScheduled` | `pollIsANoOpWhenNoTimersWhereScheduled` | U | ⬜ |
| 2 | `pollIsANoOpWhenNoScheduledTimersAreExpired` | `pollIsANoOpWhenNoScheduledTimersAreExpired` | U | ⬜ |
| 3 | `pollShouldExpireSingleTimer` | `pollShouldExpireSingleTimer` | U | ✅ (shouldScheduleAndFireTimer) |
| 4 | `pollShouldRemovedExpiredTimers` | `pollShouldRemovedExpiredTimers` | U | ⬜ |
| 5 | `pollShouldExpireTimersInOrderOfDeadlineButWithinTheDeadlineOrderIsUndefined` | `pollShouldExpireTimersInOrder` | U | ⬜ |
| 6 | `cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId` | `cancelTimerReturnsFalseForUnknown` | U | ⬜ |
| 7 | `cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer` | `cancelTimerReturnsTrueAfterCancel` | U | ✅ (shouldCancelTimer) |
| 8 | `cancelTimerByCorrelationIdAfterPoll` | `cancelTimerAfterPoll` | U | ⬜ |
| 9 | `scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing` | `scheduleTimerShiftsUpWhenDeadlineDecreases` | U | ⬜ |
| 10 | `scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing` | `scheduleTimerShiftsDownWhenDeadlineIncreases` | U | ✅ (shouldUpdateExistingTimer) |
| 11 | `pollShouldStopAfterPollLimitIsReached` | `pollShouldStopAfterPollLimitIsReached` | U | ⬜ |
| 12 | `snapshotProcessesAllScheduledTimers` | `snapshotProcessesAllScheduledTimers` | U | ⬜ |
| 13 | `expireThanCancelTimer` | `expireThenCancelTimer` | U | ⬜ |
| 14 | `moveUpAnExistingTimerAndCancelAnotherOne` | `moveUpTimerAndCancelAnother` | U | ⬜ |
| 15 | `moveDownAnExistingTimerAndCancelAnotherOne` | `moveDownTimerAndCancelAnother` | U | ⬜ |
| 16 | `cancelExpiredTimerIsANoOp` | `cancelExpiredTimerIsANoOp` | U | ⬜ |
| 17 | `scheduleMustRetainOrderBetweenDeadlines` | `scheduleMustRetainOrderBetweenDeadlines` | U | ⬜ |
| 18 | `shouldReuseExpiredEntriesFromAFreeList` | `shouldReuseExpiredEntriesFromAFreeList` | U | ⬜ |
| 19 | `shouldReuseCanceledTimerEntriesFromAFreeList` | `shouldReuseCanceledTimerEntriesFromAFreeList` | U | ⬜ |
| 20 | `manyRandomOperations` | `manyRandomOperations` | U | ⬜ |

---

## 3. `ClusterMemberTest` → `consensus/ClusterConsensusTest.cpp`

| # | Java method | C name | Type | Done |
|---|-------------|--------|------|------|
| 1 | `shouldParseCorrectly` | `shouldParseSingleMember` | U | ✅ |
| 2 | `shouldParseCorrectlyOptionalArchiveResponse` | `shouldParseCorrectlyOptionalArchiveResponse` | U | ⬜ |
| 3 | `shouldParseCorrectlyOptionalEgressResponseEntry` | `shouldParseCorrectlyOptionalEgressResponseEntry` | U | ⬜ |
| 4 | `shouldRankClusterStart` | `shouldRankClusterStart` | U | ⬜ |
| 5 | `shouldDetermineQuorumSize` | `shouldComputeQuorumThreshold` | U | ✅ |
| 6 | `shouldDetermineQuorumPosition` | `shouldComputeQuorumPosition` | U | ⬜ |
| 7 | `shouldOnlyConsiderActiveNodesWhenDeterminingQuorumPosition` | `shouldOnlyConsiderActiveNodesForQuorumPosition` | U | ⬜ |
| 8 | `shouldNotVoteIfHasNoPosition` | `shouldNotVoteIfHasNoPosition` | U | ⬜ |
| 9 | `shouldNotVoteIfHasMoreLog` | `shouldNotVoteIfHasMoreLog` | U | ⬜ |
| 10 | `shouldVoteIfHasLessOrTheSameAmountOfLog` | `shouldVoteIfHasLessOrSameLog` | U | ⬜ |
| 11 | `isQuorumCandidateReturnTrueWhenQuorumIsReached` | `isQuorumCandidateReturnTrueWhenQuorumIsReached` | U | ⬜ |
| 12 | `isQuorumLeaderReturnsTrueWhenQuorumIsReached` | `isQuorumLeaderReturnsTrueWhenQuorumIsReached` | U | ⬜ |
| 13 | `shouldCheckMemberIsActive` | `shouldCheckMemberIsActive` | U | ⬜ |
| 14 | `shouldBaseStartupValueOnLeader` | `shouldBaseStartupValueOnLeader` | U | ⬜ |
| 15-24 | (remaining variants) | see Java names | U | ⬜ |

---

## 4. `ElectionTest` → `consensus/ElectionTest.cpp` *(new file)*

All 25 tests require a mock `ConsensusModuleAgent` and synthetic peer messages.
These are `M` (mock) tests — no real Aeron driver needed.

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `shouldElectSingleNodeClusterLeader` | M | ⬜ |
| 2 | `shouldElectAppointedLeader` | M | ⬜ |
| 3 | `shouldVoteForAppointedLeader` | M | ⬜ |
| 4 | `shouldCanvassMembersInSuccessfulLeadershipBid` | M | ⬜ |
| 5 | `shouldVoteForCandidateDuringNomination` | M | ⬜ |
| 6 | `shouldTimeoutCanvassWithMajority` | M | ⬜ |
| 7 | `shouldWinCandidateBallotWithMajority` | M | ⬜ |
| 8 | `shouldElectCandidateWithFullVote` | M | ⬜ |
| 9 | `shouldTimeoutCandidateBallotWithoutMajority` | M | ⬜ |
| 10 | `shouldTimeoutFailedCandidateBallotOnSplitVoteThenSucceedOnRetry` | M | ⬜ |
| 11 | `shouldTimeoutFollowerBallotWithoutLeaderEmerging` | M | ⬜ |
| 12 | `shouldBecomeFollowerIfEnteringNewElection` | M | ⬜ |
| 13 | `followerShouldReplicateLogBeforeReplayDuringElection` | M | ⬜ |
| 14 | `followerShouldTimeoutLeaderIfReplicateLogPositionIsNotCommittedByLeader` | M | ⬜ |
| 15 | `followerShouldProgressThroughFailedElectionsTermsImmediatelyPriorToCurrent` | M | ⬜ |
| 16 | `followerShouldProgressThroughInterimElectionsTerms` | M | ⬜ |
| 17 | `followerShouldReplayAndCatchupWhenLateJoiningClusterInSameTerm` | M | ⬜ |
| 18 | `followerShouldReplicateReplayAndCatchupWhenLateJoiningClusterInLaterTerm` | M | ⬜ |
| 19 | `followerShouldUseInitialLeadershipTermIdAndInitialTermBaseLogPositionWhenRecordingLogIsEmpty` | M | ⬜ |
| 20 | `followerShouldReplicateAndSendAppendPositionWhenLogReplicationDone` | M | ⬜ |
| 21 | `leaderShouldMoveToLogReplicationThenWaitForCommitPosition` | M | ⬜ |
| 22 | `shouldSendCommitPositionAndNewLeadershipTermEventsWithTheSameLeadershipTerm` | M | ⬜ |
| 23 | `notifiedCommitPositionCannotGoBackwardsUponReceivingCommitPosition` | M | ⬜ |
| 24 | `notifiedCommitPositionCannotGoBackwardsUponReceivingNewLeadershipTerm` | M | ⬜ |
| 25 | `shouldThrowNonZeroLogPositionAndNullRecordingIdSpecified` | U | ⬜ |

**Mock requirement**: inject synthetic `CanvassPosition`, `Vote`, `NewLeadershipTerm`,
`AppendPosition`, `CommitPosition` byte buffers directly into the
`aeron_cluster_consensus_adapter_t` fragment handler — no real Aeron publication needed.

---

## 5. `PendingServiceMessageTrackerTest` → `consensus/ClusterConsensusTest.cpp`

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `snapshotEmpty` | U | ⬜ |
| 2 | `snapshotAfterEnqueueBeforePollAndSweep` | U | ⬜ |
| 3 | `snapshotAfterEnqueueAndPollBeforeSweep` | U | ⬜ |
| 4 | `snapshotAfterEnqueuePollAndSweepForLeader` | U | ⬜ |
| 5 | `snapshotAfterEnqueuePollAndSweepForFollower` | U | ⬜ |
| 6 | `loadInvalid` | U | ⬜ |
| 7 | `loadValid` | U | ⬜ |

---

## 6. `ConsensusModuleContextTest` (45) → `consensus/ConsensusModuleContextTest.cpp`

Most tests verify field defaults, env-var overrides, and conclude() validation.
All are type `U` (no Aeron driver needed for field validation).

Key groups:
- **Defaults** (10): timer supplier, auth, log channel params, session/heartbeat timeouts
- **Validation** (15): counter type checks, throw on bad config
- **conclude() lifecycle** (8): mark file creation, archive context, cluster dir
- **Explicit overrides** (12): assigned clock, explicit counters, archive context

All 45 require implementing corresponding field setters/validation in
`aeron_cm_context_conclude()`.  **Note**: many tests reference `ClusterMarkFile`
which is not yet implemented in C — those tests are blocked until ClusterMarkFile
is added.

---

## 7. `ConsensusModuleAgentTest` (15) → `consensus/ConsensusModuleAgentTest.cpp`

Requires a mock `ConsensusModuleAgent` fixture with synthetic subscriptions.
All type `M`.

| # | Java method | Key behaviour |
|---|-------------|--------------|
| 1 | `shouldLimitActiveSessions` | max session count enforced |
| 2 | `shouldCloseInactiveSession` | timeout triggers close |
| 3 | `shouldCloseTerminatedSession` | TERMINATED event closes session |
| 4 | `shouldSuspendThenResume` | state machine SUSPENDED→ACTIVE |
| 5 | `onNewLeadershipTermShouldUpdateTimeOfLastLeaderMessageReceived` | heartbeat timeout reset |
| 6 | `onCommitPositionShouldUpdateTimeOfLastLeaderMessageReceived` | heartbeat timeout reset |
| 7 | `notifiedCommitPositionShouldNotGoBackwards*` (×3) | monotone commit position |
| 8 | `shouldPublishLogMessageButNotSnapshotOnStandbySnapshot` | standby flag |
| 9 | `shouldHandlePaddingMessageAtEndOfTerm` | padding SBE template |

---

## 8. `ConsensusModuleSnapshotTakerTest` (6) → `consensus/ConsensusModuleSnapshotTakerTest.cpp`

All type `M` — encode to a buffer, decode back, verify bytes.

| # | Java method |
|---|-------------|
| 1 | `snapshotConsensusModuleState` |
| 2 | `snapshotPendingServiceMessageTracker` |
| 3 | `snapshotPendingServiceMessageTrackerWithServiceMessagesMissedByFollower` |
| 4 | `snapshotTimer` |
| 5 | `snapshotSessionShouldUseTryClaimIfDataFitsIntoMaxPayloadSize` |
| 6 | `snapshotSessionShouldUseOfferIfDataDoesNotFitIntoMaxPayloadSize` |

---

## 9. Client tests

### `AeronClusterContextTest` (2) → `client/AeronClusterContextTest.cpp`

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified` | U | ⬜ |
| 2 | `clientNameMustNotExceedMaxLength` | U | ⬜ |

### `AeronClusterAsyncConnectTest` (7) → `client/AeronClusterAsyncConnectTest.cpp`

All type `M` — use pre-recorded SBE byte sequences.

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `initialState` | U | ⬜ |
| 2 | `shouldCloseAsyncSubscription` | M | ⬜ |
| 3 | `shouldCloseEgressSubscription` | M | ⬜ |
| 4 | `shouldCloseAsyncPublication` | M | ⬜ |
| 5 | `shouldCloseIngressPublication` | M | ⬜ |
| 6 | `shouldCloseIngressPublicationsOnMembers` | M | ⬜ |
| 7 | `shouldConnectViaIngressChannel` | M | ⬜ |

### `AeronClusterTest` (2) → `client/AeronClusterTest.cpp`

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `shouldCloseIngressPublicationWhenEgressImageCloses` | M | ⬜ |
| 2 | `shouldCloseItselfAfterReachingMaxPositionOnTheIngressPublication` | M | ⬜ |

### `EgressAdapterTest` (10) + `EgressPollerTest` (2) → `client/EgressPollerTest.cpp`

All type `M` — encode SBE messages into a buffer, feed to poller/adapter, verify callbacks.

| # | Java method | Done |
|---|-------------|------|
| 1–4 | session_id match/no-match for `onMessage`, `onSessionEvent` | ⬜ |
| 5–8 | session_id match/no-match for `onNewLeader`, `onAdminResponse` | ⬜ |
| 9 | `onFragmentShouldDelegateToEgressListenerOnUnknownSchemaId` | ⬜ |
| 10 | `shouldIgnoreUnknownMessageSchema` | ⬜ |
| 11 | `shouldHandleSessionMessage` | ⬜ |
| 12 | `defaultEgressListenerBehaviourShouldThrowClusterExceptionOnUnknownSchemaId` | ⬜ |

---

## 10. Service tests

### `ClusteredServiceContainerContextTest` (11) → `service/ClusteredServiceContextTest.cpp`

Mirrors ConsensusModuleContextTest pattern — validate defaults and conclude().

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `throwsIllegalStateExceptionIfAnActiveMarkFileExists` | U | ⬜ (blocked: no ClusterMarkFile) |
| 2 | `concludeShouldCreateMarkFileDirSetViaSystemProperty` | U | ⬜ |
| 3 | `concludeShouldCreateMarkFileDirSetDirectly` | U | ⬜ |
| 4 | `shouldInitializeClusterDirectoryNameFromTheAssignedClusterDir` | U | ⬜ |
| 5 | `shouldInitializeClusterDirectoryFromTheGivenDirectoryName` | U | ⬜ |
| 6–11 | archive context, mark file, align | U | ⬜ |

### `ServiceSnapshotTakerTest` (2) → `service/ServiceSnapshotTakerTest.cpp`

| # | Java method | Type | Done |
|---|-------------|------|------|
| 1 | `snapshotSessionUsesTryClaimIfDataFitIntoMaxPayloadSize` | M | ⬜ |
| 2 | `snapshotSessionUsesOfferIfDataDoesNotIntoMaxPayloadSize` | M | ⬜ |

---

## 11. System tests (require embedded Java cluster)

These tests need a running cluster peer.  Port after unit/mock tests are complete.

| Java file | Count |
|-----------|-------|
| `ClusterNodeTest` | 5 |
| `ClusterNodeRestartTest` | 10 |
| `AuthenticationTest` | 5 |
| `ClusterTimerTest` | 3 |
| `ClusterWithNoServicesTest` | 4 |

**C equivalent**: `ClusterSystemTest.cpp` (single file, mirrors `aeron_archive_test.cpp` pattern
with `TestCluster.h` infrastructure embedding a Java cluster node).

---

## Summary

| Category | Total tests | Done | Remaining |
|----------|------------|------|-----------|
| RecordingLog (U) | 33 | 4 | **29** |
| TimerService (U) | 25 | 3 | **22** |
| ClusterMember (U) | 24 | 3 | **21** |
| Election (M) | 25 | 0 | **25** |
| PendingMessageTracker (U) | 7 | 2 | **5** |
| ConsensusModuleContext (U) | 45 | 0 | **45** |
| ConsensusModuleAgent (M) | 15 | 0 | **15** |
| ConsensusModuleSnapshotTaker (M) | 6 | 0 | **6** |
| Client context/async (U/M) | 11 | 0 | **11** |
| Client egress poller/adapter (M) | 12 | 0 | **12** |
| Service context (U) | 11 | 0 | **11** |
| Service snapshot taker (M) | 2 | 0 | **2** |
| System tests (S) | 27 | 0 | **27** |
| **Total** | **243** | **12** | **231** |

---

## Implementation order

```
Phase 1  Unit tests (U) — no Aeron driver, implement immediately
         RecordingLog × 29 remaining
         TimerService × 22 remaining
         ClusterMember × 21 remaining
         PendingMessageTracker × 5 remaining
         Client context × 2

Phase 2  Mock tests (M) — encode SBE bytes directly, no real Aeron
         EgressPoller/Adapter × 12
         ConsensusModuleSnapshotTaker × 6
         ServiceSnapshotTaker × 2
         AeronCluster mock × 7
         Election × 25
         ConsensusModuleAgent × 15

Phase 3  Context tests (U) — need conclude() validation
         ConsensusModuleContext × 45
         ClusteredServiceContainerContext × 11

Phase 4  System tests (S) — need TestCluster.h infrastructure
         ClusterNode × 5
         ClusterNodeRestart × 10
         Auth × 5
         Timer × 3
         WithNoServices × 4
```

## Key implementation note for mock (M) tests

The Java Election and ConsensusModuleAgent tests use Mockito to inject
synthetic publications.  The C equivalent pattern:

```c
/* Encode a CanvassPosition message directly into a stack buffer */
uint8_t buf[256];
struct aeron_cluster_client_messageHeader hdr;
struct aeron_cluster_client_canvassPosition msg;
aeron_cluster_client_canvassPosition_wrap_and_apply_header(&msg, (char*)buf, 0, sizeof(buf), &hdr);
aeron_cluster_client_canvassPosition_set_followerMemberId(&msg, 1);
// ... set other fields ...

/* Feed it directly to the fragment handler */
on_fragment(election, buf, aeron_cluster_client_canvassPosition_encoded_length(&msg), NULL);
```

This avoids needing a real Aeron driver for the mock tests.
