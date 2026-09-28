# Follow-up: regressions, consecutive elections, and independent review

The requested follow-up found and corrected another acknowledged-write-loss
defect on `cluster-election-fix` at `0861a99a81`. The original model passes were
insufficient to establish correctness of repeated elections. The new evidence
includes real client acknowledgements and recovery from surviving recordings,
two additional bounded models, and an independent agent review.

## New defect: catch-up recording metadata lagged quorum support

`ConsensusModuleAgent.catchupPoll` reports a recorded position using the
accepted election term, even when replay is waiting for a commit notification.
That reporting is necessary to break the replay/commit dependency cycle.
However, `Election.followerCatchupAwait` previously began that reporting
without advancing recording-log metadata. Live join already updated metadata
at the corresponding point.

A real system test produced this sequence:

1. The established term-0 cluster commits a baseline at position 384.
2. A new leader wins term 1. Its live-joining voter persists term-1 metadata
   while the term event is held by simulated publication backpressure. The
   voter stops at position 384, before recording the event.
3. The leader appends the term event and a client target at position 576.
   Neither is yet committed. A transport-only subscriber keeps UDP connected;
   it supplies no archive recording, vote, or consensus position report.
4. The late old member catches up and records the target. Commit messages to
   it are dropped, so it reports `(accepted term 1, position 576)` while replay
   and its recording metadata remain in term 0. The leader acknowledges the
   target to the actual client.
5. The leader stops and the earlier voter returns. Its tuple `(metadata term
   1, position 384)` outranks the late member's `(term 0, position 576)`.
   Subsequent election/truncation discards the acknowledged target.
6. Both survivors apply a later recovery marker, but neither has the target.

The uncorrected Java run failed both recovered-identity assertions; it did not
merely time out. After correction, catch-up metadata is term 1 before reporting,
the longer `(1,576)` recording wins the log comparison, and the target survives.

The correction calls `updateRecordingLog(nowNs)` after successful
`tryJoinLogAsFollower` in `followerCatchupAwait`, before entering
`FOLLOWER_CATCHUP`. It matches the existing live-join ordering. Archive bytes
may arrive before that call returns, but this node cannot supply accepted-term
catch-up quorum support before the metadata update completes. The existing
replication path handles a prefix below the accepted term's base before this
join. The added parameterized `RecordingLogTest` checks that updating metadata
when rejoining an already-known term preserves its original base position.

See [Election.java](../../../aeron-cluster/src/main/java/io/aeron/cluster/Election.java)
and [StalePositionQuorumTest.java](../../../aeron-system-tests/src/test/java/io/aeron/cluster/StalePositionQuorumTest.java).

## Original stale-position defect: real acknowledgement and recovery control

The other new system test creates an authentic unreplicated old-term tail,
elects the shorter pair, stops the current voter, and lets the old member
canvass while receive-side consensus drops keep it out of the new log.
Reported positions and log bytes are never invented or edited.

| Implementation | Old tail | Current voter | Target | Premature client ACK | Target after failover |
|---|---:|---:|---:|---|---|
| Term filter present | 8800 | 864 | 960 | No | Present, after a real replica returns and ACKs |
| Only quorum term filter removed | 8800 | 864 | 960 | Yes | Absent from both recovered survivors |

The fixed run must execute actual quorum calculations during the observation
window, then acknowledge after a current-term replica returns. The negative
control removes the sole target source before healing the other members; a
later recovery marker proves that their services progressed. Both tests use
retained-file stop/restart, not machine or power failure. The transport-only
subscriber models transport connectivity without a progressing archive.

## Consecutive model: scope and reductions

`Consecutive.tla` has three members, two successive new terms, one client entry
`x`, one nomination per candidate/term, and one crash/restart of the configured
member. The three positive cases cover each crash target. Candidate 1 must
nominate, persist its ballot, obtain eligible votes, and win term 1; candidate
2 must do the same for term 2. The second ballot may begin before the first
leader completes joining or appends a term event. A safe winning prefix is
checked against historical acknowledgement, not assumed in initialization.
Candidate identities are fixed; competing candidates are checked separately.

Persistent ballot, volatile candidate promise, accepted term, recording
metadata, and physical entry identities are separate. A retained-file crash
restores the accepted term from recording metadata and the promise from
`max(persisted ballot, metadata)`. Acceptance alone is not a durable ballot.
Metadata can advance before a term event; in the old-catch-up control, physical
recording can advance before metadata. An inferior-log NO may persist a higher
ballot while a node continues to follow a lower accepted term.

Early followers take live join (`J`), which updates metadata before new
recording. Late followers take catch-up (`A` then `K`); the corrected join
updates metadata before its reports support a new commitment. Disabling only
`StampCatchupMetadata` produces the newly found loss. Replay separately
advances metadata from a recorded term event. The agent's replay term is
projected out: it does not affect these vote/report/entry-preservation checks,
and the leader sets its agent term before appending the new event. `Replay.tla`
retains the separate applied/notified/agent-term progress obligation.

Requests retain their nomination-time log tuple and may arrive after timeout
or victory. Winning terms' vote caches are cleared because they cannot be
used in another winner calculation; delayed requests still alter promises and
persisted ballots. This removes irrelevant histories while retaining their
protocol effects. It reduced the measured crash-0 search from 6,488,949 to
1,342,833 states without a state/action constraint or VIEW.

Historical leadership announcements remain deliverable. One arbitrarily
chosen real position report can be retained and reused across both handoffs;
other reports arrive directly. This is an explicit **one retained-report
bound**, not an overapproximation of arbitrary message histories. Reports may
remain active. The common committed prefix is omitted; the old-filter control
adds two unrelated old-tail entries. Compatible archive prefix copying is
atomic. Partial archive writes, multi-term metadata repair, snapshots,
membership changes, and recovery deadlines are outside this cut.

Reachability witnesses require an ACK, a crash **between** victories, and the
second leader publishing its term event after quorum support for its base;
actual loss of a volatile acceptance at crash;
and a crash after metadata advancement but before the term event. These are
nonvacuity checks, not recovery-liveness proofs. Safety assumes no fairness.

## Competing ballots and persistence

`Ballots.tla` allows all three members to nominate in two bounded terms,
delayed request and positive-vote histories, timeout, volatile acceptance,
separate metadata advancement, and one crash at member 0. The initial members
are symmetric. Equal log eligibility isolates durable voting; changing log
comparison is covered in `Consecutive`.

Omitting delivery of negative replies overapproximates opportunities to win:
a delivered NO can only prevent victory. The checks require at most one vote
per member/term and one leader per term. Removing ballot persistence produces
two leaders in one term. That control tests an essential assumption, not an
additional claimed defect in the original Java branch.

## Review and confidence

[INDEPENDENT-REVIEW.md](INDEPENDENT-REVIEW.md) records the separate agent's
source review, independent rerun of the original 34 checks, discovery and
refinement of the catch-up counterexample, and inspection of the Java
reproductions and correction. It is an independent agent review, not an
external human review or machine-checked refinement proof.

The main review findings are resolved by separating durable ballots from
volatile acceptance, separating metadata from recorded term events, covering
the next actual ballot, preserving live-join versus catch-up ordering, checking
specific crash witnesses, and pinning Java source hashes in runner evidence.
The new Java defect is covered by both a model control and a real system test.

This supports substantially stronger confidence in the identified mechanisms.
It still does not prove arbitrary election histories, composition of all seven
models, progress under arbitrary failures, or filesystem/power-loss durability.
Default file synchronization and retained-file process crashes are different
claims. See [RESULTS.md](RESULTS.md) for exact completed checks and limitations.
