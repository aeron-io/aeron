# Full-cluster controls for the smaller election corrections

The smaller fixes now have 11 additional Java system-test cases and nine
independent source controls. They run real consensus agents, services, Aeron
transports, archives, elections, and retained-file restarts. Each corrected
case must reach its distinguishing state, make the correct decision, and
subsequently acknowledge a client write. Each control removes one correction
from the otherwise corrected implementation and must fail its designated
assertion. An arbitrary failing build or timeout is not a reproduction.

These complement the acknowledged-write-loss and stale-incumbent regressions
described in [FOLLOW-UP.md](FOLLOW-UP.md). They add tests and a runner; they do
not change production election code or the TLA+ models.

## Coverage and distinguishing evidence

| Control | Java scenario | Fixed versus reverted observation |
|---|---|---|
| `early-self-term` | Restart a nonempty one-member cluster and a two-of-three quorum. | The elected leader counts its own recording in its accepted term. Removing the early stamp excludes it and produces a zero quorum. |
| `replication-quorum-term` | The same two restart cases, before the leader joins its log. | Actual replication-phase quorum is the recorded prefix; using the old agent term instead produces zero. |
| `replay-quorum-term` | The same two cases during real archive replay. | Actual replay-phase quorum is the recorded prefix; reverting only this call site produces zero. |
| `canvass-quorum-term` | Hold the elected leader before replay and restart the third member with an older log term. | Its genuine canvass triggers a leadership response using the recorded current-term quorum; the implicit agent-term calculation returns zero. |
| `catchup-report-term` | A late follower records a new-term target while commit-message loss keeps replay in the old term. | An actual successfully published report carries accepted term 1 at position 576; the control publishes agent term 0. The fixed run ACKs the target before replay and then resumes service. |
| `unfinished-leader` | Pause an elected leader in each of `LEADER_LOG_REPLICATION`, `LEADER_REPLAY`, `LEADER_INIT`, and `LEADER_READY`; the other members generate a newer ballot. | The real higher-term request triggers election restart; the control fails to restart on that request. Fixed cases subsequently ACK a new write. |
| `partial-replay-report` | Replay a real committed prefix at 384 while a recorded tail extends to 480; retain that nonzero notification across CANVASS and a newer ballot. | The follower publishes its recorded tail and remains in replay; the control returns to CANVASS without publishing. |
| `replay-commit-timeout` | Reach that same replay wait, lose its leader, and later restart the appointed leader. | The follower times out, re-enters CANVASS, participates in the next election, and resumes client service. The control stays in FOLLOWER_REPLAY through the bounded observation window. |
| `nomination-log-term` | A shorter voter accepts term 1 without replaying it, times out, and receives genuine term-0 canvasses including an old uncommitted tail at 8800. | Its actual unanimous-candidate predicate rejects its shorter term-0 recording. The control stamps self with accepted term 1 and incorrectly returns true. |

The shared restart method has two parameterized cases and is used by three
separate controls. Consequently one complete matrix executes 15 fixed cases
and 15 reverted cases, covering 11 distinct added Java cases. Existing loss
regressions remain separate.

The `LEADER_READY` quorum call is not claimed as another independent defect:
`leaderInit` calls `joinLogAsLeader`, which aligns the agent term before entering
that state. The suite isolates the three call paths where the two terms actually
differ. It also does not treat removal of the now-unreachable leadership reply
from the inferior-vote branch as an additional protocol defect.

## Fixtures and limits

The tests are in:

- [ElectionTermProgressTest.java](../../../aeron-system-tests/src/test/java/io/aeron/cluster/ElectionTermProgressTest.java): quorum accounting and unfinished-leader reactions.
- [ElectionReplayProgressTest.java](../../../aeron-system-tests/src/test/java/io/aeron/cluster/ElectionReplayProgressTest.java): partial replay and timeout recovery.
- [StalePositionQuorumTest.java](../../../aeron-system-tests/src/test/java/io/aeron/cluster/StalePositionQuorumTest.java): catch-up reporting and nomination, sharing the existing real-tail and catch-up fixtures.

ByteBuddy probes observe production methods on their consensus threads. Fault
injection drops selected incoming messages or temporarily skips selected work
methods, including commit processing, leader replay, or canvass evaluation.
These controlled internal pauses make the relevant schedule repeatable; they
are more specific than stopping an entire process. No fixture writes election
fields, invents archive bytes, fabricates votes or positions, or substitutes
commit notifications. The transport-only subscriber used to create an old
uncommitted tail supplies neither a recording nor quorum support.

Destination setup is queued onto the owning consensus agent's thread. A
development repetition exposed a `DriverTimeoutException` when the old helper
called the agent's invoker-mode, `NoOpLock` Aeron client from the JUnit thread.
That run was rejected as a fixture failure. The helper now submits the command
on the agent thread and waits for the real transport connection before proceeding.
Another rejected development run exposed an assertion racing the service
thread after the consensus module finished partial replay. The fixture now
waits for both that transition and actual baseline application by the service.
`LogReplay.isDone()` observes the consensus log adapter's position; it does
not establish that the service thread has applied the last message.

The partial-replay fixture pauses the old leader's commit-processing work while
its actual archives continue recording. Its unchanged commit position is sent
through the normal leadership protocol. The restarted follower really replays
that prefix and applies the baseline service message. The appointed-leader
configuration makes the subsequent ballot repeatable; after the timeout case,
that leader returns so the follower must vote and recover.

The nomination fixture checks an incorrect suitability decision, not data loss
or an infinite election loop. Similarly, an incorrect quorum result or ignored
vote request can be repaired by a later event. The control assertions identify
the mechanism without claiming that every isolated revert inevitably causes
permanent unavailability. Three repetitions are a modest reproducibility check,
not a statistical reliability estimate or exhaustive scheduling coverage.

## Running and interpreting the controls

The positive regressions use the existing `@SlowTest` classification and run
with the normal system-test task. To run the new and existing election cases
together:

```sh
./gradlew :aeron-system-tests:slowTest \
  --tests '*ElectionTermProgressTest' --tests '*ElectionReplayProgressTest' \
  --tests '*StalePositionQuorumTest' --tests '*AcknowledgedWriteDurabilityTest'
```

Use the repository's supported build JDK (validation here uses Zulu JDK 21):

```sh
python3 docs/formal/election/run-system-controls.py --repeat 3
```

The runner inherits `JAVA_HOME`, `BUILD_JAVA_HOME`, and `BUILD_JAVA_VERSION`.
To select one control:

```sh
python3 docs/formal/election/run-system-controls.py --case partial-replay-report
```

It creates a private detached worktree at the caller's HEAD, overlays snapshots
of the relevant current Java sources, and restores those same fixed snapshots
before applying each individual mutation. Mutation anchors must match exactly
once. The quorum controls restore an implicit-agent-term helper for only the
selected call site; the term filter and other corrections remain present.
The partial-replay control restores the old nonzero-notification rejection
while preserving the separately tested timeout.

Every invocation has a deadline and requires the expected test class, case
count, no skipped tests, and the expected result. A reverted run must fail only
with the designated assertion marker; suppressed extra failures are rejected.
Gradle build caching is disabled, test output is removed between invocations,
and cached/up-to-date test tasks are rejected. Runs are sequential because the
clusters use shared test ports. Do not run another system-test process alongside
this runner.

Ignored `results/system-controls-<timestamp>/` directories contain the runner
snapshot, source hashes and snapshots, production patches, exact commands,
Gradle logs, JUnit XML, compact event/thread diagnostics, and `summary.json`.
Both `complete` and every result's `valid` must be true. The final
`caller_sources_unchanged` flag distinguishes an exact-source validation from
a development run during which the caller edited files. The private worktree
is removed afterwards; caller source files are never mutated by the runner.

## Validation

The final matrix on 2026-09-28 used production code at `77b24f29ea` plus the
new test-source snapshots. All nine controls completed three fixed/reverted
repetitions: **54 Gradle invocations, 45 passing fixed cases, and 45 cases that
failed with their designated reverted-fix assertions**. There were no skipped
cases, unexpected failures, or deadline expirations in this final matrix.
Summed Gradle process duration was **1,151.799 seconds** (about 19.2 minutes).

The authoritative local evidence is
[`results/system-controls-20260928T173409Z/summary.json`](results/system-controls-20260928T173409Z/summary.json).
`complete`, every result's `valid`, and `caller_sources_unchanged` are true.
All fixed-run Java hashes and the runner hash were checked against the current
files after completion. Earlier development attempts, including the two
rejected fixture failures described above, are excluded from these totals.

Representative reverted observations from the third repetition:

- Catch-up: `(accepted=1, agent=0, reported=0, position=576)`.
- Partial replay: `(applied=384, notified=384, recorded=480, reportSent=false,
  nextState=CANVASS)`.
- Nomination: `(logTerm=0, acceptedTerm=1, selfTerm=1, position=384,
  longerPeerPosition=8800, unanimous=true)`.

The final combined normal-suite run passed **15/15 cases, with zero skips**:
seven in `ElectionTermProgressTest`, two in `ElectionReplayProgressTest`,
four in `StalePositionQuorumTest`, and the two existing
`AcknowledgedWriteDurabilityTest` cases. System-test Checkstyle also passed.
This includes both older acknowledged-write-loss regressions after changing
the shared transport helper. The normal run finished in approximately three
minutes. Evidence, source snapshots, and hashes are in
[`results/system-suite-20260928-final/summary.json`](results/system-suite-20260928-final/summary.json).

The production implementation and TLA+ models were unchanged in this work.
The earlier unit/model validation remains recorded in [RESULTS.md](RESULTS.md);
those unrelated suites were not rerun for these test-only additions.
