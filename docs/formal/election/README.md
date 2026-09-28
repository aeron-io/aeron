# Election models and regressions

These seven TLA+ models check specific Aeron Cluster election failure mechanisms:
competing ballots, stale leadership messages, quorum accounting, nomination,
replay progress, and preservation of acknowledged writes across failover.
Separating these obligations keeps the state spaces small enough for completed
bounded checks, with controls that reproduce the corresponding incorrect
behaviour. Java system tests exercise the same mechanisms in real clusters.

## Running the models

From the repository root, with Python 3, Java, and a local `tla2tools.jar`:

```sh
# Run all checks.
python3 docs/formal/election/run.py --jar /path/to/tla2tools.jar

# List checks and their expected outcomes; no jar is needed.
python3 docs/formal/election/run.py --list

# Run a corrected case and its defect control.
python3 docs/formal/election/run.py --jar /path/to/tla2tools.jar \
  handoff-base-0 handoff-old-guard
```

`TLA2TOOLS_JAR` can supply the jar path. `--java` or `TLC_JAVA` selects the Java
executable. Defaults are one TLC worker, a 1 GiB heap, and a 120-second timeout
**per check**. Use `--workers` and `--timeout` to change the worker count and
time limit. The timeout is an operational limit, not a model bound.

[run.py](run.py) defines the configurations and expected outcomes. The matrix
contains 22 positive checks, 15 defect or persistence controls, and 10 witnesses.
Positive checks must finish without errors and with an empty search queue.
Controls must produce the specified invariant or temporal counterexample.
Witnesses deliberately negate a reachable event or fair successful execution;
their counterexamples show that the intended scenario can occur. Expected
counterexamples are not safety passes. Timeouts, parse errors, and unexpected
failures fail the runner.

Each invocation creates an ignored `results/<run>/` directory containing
`summary.json`, TLC logs and traces, generated configurations, frozen inputs,
hashes, commands, timings, and Java source revision information. Check every
case's `matched` field and the `inputs_unchanged` and `implementation_unchanged`
flags when interpreting a run. Keep run-specific evidence there rather than
adding logs, local paths, or result tables to the source documentation.

## What the models cover

| Model | Checked behaviour | Corresponding Java code |
|---|---|---|
| [Ballots](Ballots.tla) | At most one vote per member/term and one leader per term, with competing candidates and crash recovery. | `Election.nominate`, `onRequestVote`, `init`; `NodeStateFile` |
| [Consecutive](Consecutive.tla) | Historical acknowledgements survive two successive elected terms, delayed messages, metadata changes, and one crash. | Ballot log comparison; catch-up/live join; commitment and truncation |
| [Handoff](Handoff.tla) | A delayed winner and an old leader cannot lose or truncate acknowledged entries. | `Election.onNewLeadershipTerm`; position reporting, quorum commitment, and truncation |
| [Quorum](Quorum.tla) | Ranked positions have current-term, active majority support and are maximal within the leader's recording bound. | `ClusterMember.quorumPosition`; `ConsensusModuleAgent.quorumPositionBoundedByLeaderLog` |
| [Nomination](Nomination.tla) | Self-term stamping agrees with the log comparison used to assess candidates. | `Election.resetMembers`; `ClusterMember.willVoteFor`, `isQuorumCandidate`, `isUnanimousCandidate` |
| [StaleRecovery](StaleRecovery.tla) | A stale incumbent opens an election after a surviving voter has promised a newer term. | `Election.canvass`, `onRequestVote`; `ConsensusModuleAgent.onRequestVote` |
| [Replay](Replay.tla) | A stable quorum commits and applies a fresh write; leader loss releases a waiting follower to CANVASS. | `Election.leaderLogReplication`, `followerReplay`; `ConsensusModuleAgent.catchupPoll`; election quorum call sites |

### Bounds and assumptions

- **Ballots:** three equally log-eligible members, two terms, and one crash at
  member 0. Requests and positive votes remain deliverable; omitting negative
  replies increases opportunities to win. Removing ballot persistence tests an
  essential protocol assumption, not an additional Java bug.
- **Consecutive:** three members, fixed candidates for two successive terms,
  one client entry, one nomination per candidate/term, and one crash/restart.
  Separate cases choose each crash target. One real position report may be
  retained across both handoffs; other reports arrive directly. This is a
  bound on message history, not arbitrary delayed transport. Compatible prefix
  copying is atomic. The second-handoff witness reaches the second leader's
  term event after quorum support for its base; it does not require a second
  client acknowledgement or recovery at every node.
- **Handoff:** starts after a valid winning ballot with a safe immutable prefix
  of zero, one, or two old entries. The winner cannot crash. All enabled
  interleavings after that cut are explored. Per-source, per-term publication
  maxima allow any lower position to arrive repeatedly, even after restart or
  truncation, overapproximating stale position reports. The model simplifies
  metadata to recorded term events and assumes a persisted newer vote at the
  stale recipient; it does not establish arbitrary crash or next-ballot safety.
- **Quorum:** enumerates one and three members with positions 0–2, and five
  members with positions 0–1. It includes inactive members and mismatched terms.
  A zero result does not establish an active quorum. This checks accounting,
  not whether callers supplied truthful reports.
- **Nomination:** enumerates three- and five-member views, unknown peers, and
  bounded log terms and positions. It checks log suitability, not whether
  cached reports remain current or peers will actually grant votes.
- **StaleRecovery:** assumes a persisted newer vote, an eligible nominating
  survivor, successful request publication before ballot timeout, and eventual
  reliable delivery. It omits the appointed-leader gate. Operation-level weak
  fairness establishes election entry at the incumbent, not eventual service.
- **Replay:** separates recorded, notified, applied, accepted, and agent-term
  state, with one outstanding message per report/commit link. Healthy cases
  assume a stable leader and quorum, eventually serviced operations, and no
  recovery deadline expiry. Separate leader-loss cases allow the deadline to
  expire. These are dependency checks, not arbitrary transport or failover
  liveness.

`Consecutive` keeps persistent ballot promises, volatile acceptance, recording
metadata, and physical entries separate. In particular, a catch-up follower
must advance metadata before its accepted-term position reports support a
commitment. Otherwise a later ballot can prefer a shorter recording with newer
metadata and discard an acknowledged write. The `StampCatchupMetadata` control
preserves this failure path. Live join and catch-up have distinct ordering;
replay timing is checked separately by `Replay`.

These are bounded diagnostic models, with no checked refinement from Java or
composition proof between models. Passing them does not prove arbitrary
histories, membership changes, snapshots, multi-term metadata repair, partial
archive writes, or machine/power-loss durability. Model crashes retain files;
Java regressions use orderly retained-file stop/restart. Safety does not assume
fairness, and deadlock checking is disabled for the finite/stuttering models;
progress claims come only from the explicit temporal properties. The checks
use no state/action constraints, VIEW, or symmetry reduction. Review model/code
correspondence when changing the implementation; hashes record what was checked
but do not establish that correspondence.

## Running the Java regressions

Use the repository's Gradle build environment. For example, with JDK 21:

```sh
export JAVA_HOME=/path/to/jdk-21
export BUILD_JAVA_HOME="$JAVA_HOME"
export BUILD_JAVA_VERSION=21

./gradlew :aeron-system-tests:slowTest \
  --tests '*ElectionTermProgressTest' --tests '*ElectionReplayProgressTest' \
  --tests '*StalePositionQuorumTest' --tests '*AcknowledgedWriteDurabilityTest'
```

These tests cover quorum/term accounting, unfinished-leader ballots, replay
reporting and timeout recovery, nomination, and acknowledged-write preservation.
They use real consensus agents, services, transports, and archives. Probes
observe production decisions; fault injection drops selected incoming messages
or pauses selected work. The fixtures do not fabricate log bytes, terms, votes,
or positions. Some assertions identify an incorrect decision rather than
inevitable data loss or permanent unavailability.

[run-system-controls.py](run-system-controls.py) runs a fixed implementation
and a single-fix revert for each selected control:

```sh
# All nine controls, with one fixed/reverted pair each.
python3 docs/formal/election/run-system-controls.py

# Repeat one control three times.
python3 docs/formal/election/run-system-controls.py \
  --case partial-replay-report --repeat 3
```

| Control | Correction removed |
|---|---|
| `early-self-term` | Stamp the leader's own recorded position with the accepted term before joining its log. |
| `replication-quorum-term` | Use the election term for quorum accounting during leader log replication. |
| `replay-quorum-term` | Use the election term for quorum accounting during leader replay. |
| `canvass-quorum-term` | Use the election term when answering an old canvass. |
| `catchup-report-term` | Report catch-up recording progress in the accepted term. |
| `unfinished-leader` | Restart an unfinished leader on a newer ballot. |
| `partial-replay-report` | Report the recorded tail while awaiting commitment after partial replay. |
| `replay-commit-timeout` | Leave replay wait when the leader stops making progress. |
| `nomination-log-term` | Assess nomination using the recorded log term. |

The runner inherits the JDK environment, creates a private detached worktree,
and overlays snapshots of the relevant current Java sources before each run.
Each revert must match its source anchor exactly once and fail its designated
assertion. Build errors, timeouts, skipped cases, cached test tasks, and unrelated
failures do not count as reproductions. The default deadline is 240 seconds per
Gradle invocation; `--timeout` changes it. Runs are sequential because the tests
share ports; avoid running another system-test process alongside them.

Results go to ignored `results/system-controls-<run>/`, or a fresh directory
chosen with `--output`. Evidence includes source snapshots, patches, commands,
logs, JUnit XML, diagnostics, and `summary.json`. Require `complete`, every
result's `valid`, and `caller_sources_unchanged` to be true. The runner removes
its private worktree afterwards and leaves the caller's Java sources unchanged.
