# Independent agent review of the election abstractions

Reviewed on 2026-09-28 against Java commit
`0861a99a81230bb7430b6274965f3a79b55fe181` (`cluster-election-fix`). This is an
independent review by a separate agent, not an external human review or a
machine-checked refinement proof. I read the five models and runner before
reading their README, and checked the Java methods and persistence paths
directly. No applicable `AGENTS.md` was found.

**Result:** the original five models' 34 checks reproduced. The subsequent
consecutive-handoff review found a **high-severity catch-up safety gap in the
reviewed Java revision**, then reproduced by a real client-ack/recovery system
test. Recording ahead of replay could provide accepted-term quorum support
without durable matching term metadata, allowing the acknowledged write to
disappear after failover. A small Java correction now stamps that metadata
before catch-up reporting; both targeted system tests pass with it. See the
additional review below for evidence and the correction's preconditions.
**H1 is resolved by the reviewed change, and no blocking review finding remains
within the declared scope.** The final frozen 47-case matrix, exact-source
system controls, corrected regressions, and broader unit/system evidence have
been inspected. Neither the original nor expanded bounded suite establishes
preservation across arbitrary elections or failures.

## Final validation and disposition

I inspected the final frozen run
[`results/20260928T112828-grzk9xan/summary.json`](results/20260928T112828-grzk9xan/summary.json)
and all 47 corresponding raw TLC logs. **47/47 outcomes match:** 22 completed
positive checks, 15 defect controls, and 10 reachability/fairness witnesses.
Every positive check terminates normally with an empty queue; every negative
case reports its intended invariant or temporal failure, with no timeouts.
Summed process time is 295.062 seconds. Both `inputs_unchanged` and
`implementation_unchanged` are true. I independently compared all seven model
files and the runner with their frozen hashes, and all seven Java/test source
hashes with the implementation manifest; all match the current reviewed files.
This is an independent inspection of the root agent's final run, in addition
to my own earlier execution of the original 34 cases.

| Final positive model cut | Distinct states | Queue remaining |
|---|---:|---:|
| `consecutive-crash-0` | 1,342,833 | 0 |
| `consecutive-crash-1` | 1,031,363 | 0 |
| `consecutive-crash-2` | 1,302,682 | 0 |
| `ballots` | 889,869 | 0 |

The final `Consecutive.tla` SHA-256 is
`46ea37d6d0519deb69d29969b86279e2307e73eef6c423965bda4cc3d1c2187e`;
`Ballots.tla` is
`4219ee3a2fce178d767a383306ba90a66f5fa42ed3b533b771ee19440a860cd2`.
The final two-test regression source is
`3ad37841c076973cbed2e58d27b7199b990bd4422473ea020991a48243cf4bb4`.
The frozen manifest contains the remaining hashes, configurations, commands,
and tool versions.

I also inspected the archived Java runtime evidence in
[`results/system-20260928`](results/system-20260928):

- `cluster-unit-final` contains 658 declared cases: **655 executed passes and
  three skips**, with no errors or failures. Its log ends `BUILD SUCCESSFUL`.
- `system-regressions` contains **30 executed passes and one skip**, with no
  errors or failures. The skipped `RacingCatchupClusterTest` adds no executed
  coverage.
- `filter-control-final.xml` reports an actual premature target ACK, followed
  by successful recovery without that target on either survivor. The archived
  `ClusterMember` source removes only the quorum's term equality; its archived
  `Election` still contains the catch-up correction.
- `catchup-control-final.xml` reports accepted term 1, metadata term 0,
  target position 576, an actual client ACK, and loss on both recovered
  survivors. Its archived `Election` differs only by removing the new catch-up
  metadata update and its explanatory comment. Both controls use the final
  regression source, whose archived hash matches the current file.

The final corrected-source rerun also completed: JUnit timestamp
`2026-09-28T16:33:52.777Z` reports both new methods passing, with catch-up
metadata term 1 and recovered target true, and with stale-position premature
ACK false and recovered target true. `/tmp/aeron-regressions-final.log` ends
`BUILD SUCCESSFUL` after the production and test Checkstyle tasks. I inspected
these results; I did not execute concurrent or independent Gradle runs.

The baseline M/L findings below remain qualifications on broader claims,
rather than unfinished correction work. The new models address the identified
term-state and consecutive-election omissions within explicit finite bounds;
the Java change resolves the reproduced H1 failure. There is no claim of
external human review, arbitrary-history safety, combined-model refinement,
unconditional recovery liveness, or machine/power-loss durability.

## Reproduced evidence

I independently ran the unmodified matrix:

```sh
python3 docs/formal/election/run.py \
  --jar /Users/ericbowden/git/weareadaptive/seqcd/local/formal/tools/tla2tools-1.7.4.jar
```

All **34 checks matched their expected outcomes** in 76.037 seconds of summed
case time. Evidence is in
[`results/20260928T101753-wbftejp4/summary.json`](results/20260928T101753-wbftejp4/summary.json),
including frozen sources, configurations, logs, and hashes. Positive runs
finished with an empty queue; negative controls produced the intended invariant
or temporal failures. `inputs_unchanged` is true. Despite the jar filename, its
own banner is **TLC 2.19, 08 August 2024, revision 5a47802**. Java was Zulu
17.0.11; jar SHA-256 was
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.

The three fixed handoff cuts reached 26,645, 277,626, and 1,056,583 distinct
states. This measures the searched graphs, not protocol coverage. Results
directories are local evidence and are git-ignored.

## Findings and limits

These baseline findings describe the effect on conclusions one could draw
from the original suite. “Medium” here means a material missing obligation or
an abstraction unsuitable for a broader claim; it does not mean a reproduced
implementation failure. The subsequently reproduced high-severity Java finding
H1 is documented separately below.

### M1 — Medium: accepted terms and persistent ballot promises are different state

**Model:** `Handoff.tla:46-50,67-76` preserves `promised` across `Restart`, and
`HearNew` assigns it 1 when accepting the new leader. There is no separate
persisted ballot term. `StaleRecovery.tla:4-5` begins with a persisted newer
vote as an assumption.

**Java:** `Election.onNewLeadershipTerm` assigns `leadershipTermId` and the
in-memory `candidateTermId` at lines 470-477, without writing `NodeStateFile`.
Persistent writes occur before voting at lines 366-377, during nomination at
746-749, and in the single-node path at 693-694. `NodeStateFile:258-284` writes
the candidate term, then optionally forces the mapped file. On process startup,
`ConsensusModuleAgent:361-378` constructs the election from the recovery plan's
last term; `Election.init:687-688` raises the candidate term to the maximum of
that term and the node-state term. It does not restore every accepted in-memory
term.

**Consequence:** accepting an announcement cannot generally be treated as a
persistent promise after a process crash. A node that did not vote can accept
a newer leader and restart before its recording-log metadata advances. The
model's persistent `promised` state would exclude that loss of volatile state.

This does **not** refute the existing handoff result: the follower whose old
announcement is checked is node 2, and it starts with a real persisted vote in
term 1. The newly assigned promise of node 0 is not used by `HearOld`. It does
prevent generalizing `Restart` to arbitrary crash recovery. A consecutive
handoff model should explicitly distinguish persistent ballot term, accepted
election term, recording-log term, and replayed agent term.

Persistence itself also has a declared boundary: `NodeStateFile:388-393`
calls `MappedByteBuffer.force()` only when `fileSyncLevel > 0`, while
`ConsensusModule.Configuration.FILE_SYNC_LEVEL_DEFAULT` is 0 (line 929).
Retained-file process crashes are a different claim from machine/power failure
durability. The current models contain no torn-write, loss-of-unsynced-data,
archive force, or filesystem ordering transitions.

### M2 — Medium: a term event in the recording is not the recording-log metadata term

**Model:** `Handoff.tla:17` defines `Term(s)` by the presence of `"term-1"`.
That function determines both the canvass report term and whether an old tail
can be truncated (`53,69,87`). `RecordNew` changes it as soon as the term entry
is recorded.

**Java:** `Election.updateRecordingLog:1541-1545` sets `logLeadershipTermId`
when recording metadata is made coherent. The leader calls it in
`leaderInit:883-887`, **before** `leaderReady:899-905` appends the new term
event. The live follower calls it in `followerLogAwait:1155-1163`, and replication
can advance it through `updateRecordingLogForReplication:1547-1555`. Conversely,
catch-up recording can be ahead of replay; the agent term changes on replay in
`ConsensusModuleAgent:1593-1630`, and the election's replay callback updates its
log term at `Election:608-614`. Startup obtains the log term from the recovery
plan, not by this model's search of the entry sequence.

**Consequence:** neither direction of the equality “recorded term entry iff
known log term” holds throughout the Java protocol. This is not a faithful
quotient for arbitrary crash/restart cuts. In the single immutable-prefix
handoff, the simplification usefully exposes the original stale-leader race;
it is not evidence for term-metadata recovery or for every state that can
publish a current-term position. Model metadata advancement separately before
using those cases to validate a second election.

### M3 — Medium: one handoff does not prove preservation under the next election

**Model:** `Handoff.Init:28-38` assumes a valid winner with a prefix containing
all initially acknowledged entries. `Base` is immutable; node 1 cannot crash
or restart (`Restart` is restricted to nodes 0 and 2), and there is no term 2.
`HearNew` is the only truncation action and truncates only a term-0 tail to that
fixed base. Once a new-term entry has been acknowledged, no subsequent winner
can challenge it. `NoAcknowledgedTruncation` therefore tests a real old/new
race, but not an inductive preservation argument across elections.

**Java:** actual winning ballots use the persisted candidate term and log
comparison (`Election:335-405,764-800`; `ClusterMember:986-1040`), then calculate
truncation/replication boundaries from recording metadata
(`Election:450-539,1278-1313`). The current cuts assume the preconditions that
these mechanisms must preserve over time.

The separation from `Quorum`, `Nomination`, and `Replay` is useful for search
cost, but there is no checked composition relation. In particular, the local
ranking theorem says how to count reports, not that a report's term and position
refer to the leader's entry identities. A repeated-handoff/crash model and a
real egress-ack/recovery regression are appropriate additional evidence. They
should retain entry identity, historical acknowledgements, real voter
eligibility, and delayed old reports across both handoffs.

### M4 — Medium: the progress results require stronger conditions than eventual delivery alone

**Replay:** `Expire` and `RestartElection` are enabled only in `Mode="lost"`
(`Replay.tla:150-159`). Healthy `replay` and `catchup` executions never take a
deadline failure, although Java times out at `Election:988-991,1067-1069,
1093-1100,1165-1177` and `ConsensusModuleAgent:2008-2017`. Weak fairness does not
bound delivery delay relative to any of those deadlines. Consequently,
`EventuallyRecovered` is a result for a stable leader and quorum with
eventually serviced operations **and no recovery deadline expiry**, rather
than recovery from every eventually reliable schedule.

Replay also has a single nonduplicating report slot and commit slot, no
canvass report that overwrites the leader's current-term cache, and no term or
leader identity on `commitWire`. `ReceiveCommit` can update `notified` in any
phase (`93-96`), whereas Java ignores an election commit when `leaderMember`
has been cleared in CANVASS (`Election:575-577,1464-1468`). Reacceptance can
carry a commit position in both implementations, but that does not make all
intermediate states a refinement. These reductions are suitable for the
isolated dependency-cycle checks, not for adversarial transport or combined
handoff/replay progress.

**StaleRecovery:** `Timeout` requires `sent` (`26`), so weak fairness of
`SendRequest` guarantees the first ballot sends its request before it can time
out. Java can time out before a successful send (`Election:775-799`), for
example after publication backpressure or a long scheduling pause. The model
has no retries after the first request and omits the appointed-leader gate
(`Election:722-724`). Treat successful request publication, an eligible voter,
and reliable eventual request delivery as assumptions. The checked endpoint
is only `opened`, set when the incumbent enters CANVASS (`28-38`); no election
victory or client service follows in this model.

These limits are largely described in the README. Keep them alongside any
liveness claim; successful fair witnesses show satisfiability of the bounded
specifications, not sufficiency of fairness for the Java system.

### L1 — Low: nomination checks log suitability, not actual voting behavior

`Nomination.tla` correctly distinguishes `ownLogTerm` from `acceptedTerm` and
compares `(term, position)` lexicographically. Its `WouldReject` predicate
deliberately considers only the log comparator and unknown reports. Java can
also reject because the candidate term is not newer than its persisted/in-memory
candidate term (`Election:360-369`), ignore a request in another election
phase (`372-379`), or step down as a leader (`354-357`). Cached reports can
already be stale when a request arrives. Graceful-leader exclusion and the
stale-leader nomination escape are separate paths.

Thus the three properties are meaningful local consistency tests; they do
not assert that a predicted quorum actually grants votes. The README makes
this distinction correctly. The name `WouldReject` should continue to be
interpreted as “would reject by this log comparison,” especially in any reuse.

### L2 — Low: runner evidence pins the abstraction but not the Java correspondence

`run.py:127-134,184-186` snapshots and hashes the TLA files, generated configs,
runner, jar, and Java runtime. It does not include the Java source revision,
dirty status, or hashes of the Java methods being abstracted. `inputs_unchanged`
only checks the frozen input copies. A future Java change could therefore leave
all models green without changing their archived evidence.

Record the source commit and relevant Java hashes when publishing evidence,
or maintain explicit review of model/code correspondence. No Java source is
generated from these specifications, and passing TLC is not an automatic check
that callers pass the accepted election term.

**Follow-up:** the expanded runner now records the implementation commit,
tracked dirty status, and SHA-256 hashes of the five relevant production files
and two regression sources. It also reports whether that evidence changed
during a run. This resolves the evidence-pinning omission; it does not create
an executable Java/model correspondence check.

## Checks that withstand review within their stated scope

- `Quorum`'s recursive insertion matches the descending, zero-initialized
  array in `ClusterMember.quorumPosition:868-899`. Its separate cardinality
  contract exercises ties, inactive members, mismatching terms, and the local
  recording bound (`ConsensusModuleAgent:2902-2912`). Restricting `Supported`
  to positive positions is appropriate: a zero-filled array does not establish
  a live quorum at position zero. Negative/unset positions are not allowed in
  this model, but their insertion into Java's zero-filled array cannot raise
  a positive result.
- Handoff's history maximum is an overapproximation of delayed position
  reports for its safety checks: every previously sent position remains
  deliverable, and extra lower positions do not remove an unsafe behavior.
  It is not a latest-message cache masquerading as a network. Keeping all
  reports active is conservative for its commitment safety checks, though not
  a liveness abstraction.
- Acknowledgements in `Handoff` preserve entry identity and history. The
  `handoff-old-quorum` counterexample actually acknowledges `new-write` while
  the other long recording contains `old-a, old-b`, not the new entry. It is
  not just a numerical commit-position discrepancy. The old-guard control
  checks acknowledged-entry truncation separately.
- The replay model preserves a notification over a partial replay and CANVASS
  (`100-129`), reflecting `Election:1013-1029`, while INIT clears it
  (`Election:687`). It separates report generation, report receipt, quorum
  calculation, commit receipt, recording, and applying the fresh entry. The
  accepted-term catch-up stamp matches `ConsensusModuleAgent:1997-2004`;
  the leader self stamp and explicit election-term quorum argument match
  `Election:823-829`.
- Safety checks do not rely on fairness or on state/action constraints.
  Temporal checks use operation-level weak fairness, rather than fairness of
  `Recovered` itself. Negated-success properties provide actual fair success
  witnesses, and defect controls fail independently. These are useful defenses
  against vacuous tests, subject to the progress assumptions above.
- The runner rejects timeouts, parse failures, unexpected invariant failures,
  and incomplete positive searches. Invariant controls require exit code 12
  and the named violated invariant. Temporal controls require exit code 13;
  this is adequately tied to the intended property because each generated
  config contains exactly one temporal property. Disabling deadlock checks is
  appropriate for these finite/stuttering specifications, but means a safety
  pass is not a progress result.

## Additional work reviewed

The new consecutive-handoff/crash model and stale-position client-ack/recovery
system regression were not present in the 34-check snapshot above. Their
reviews are recorded separately below. A design discussion or source review
is not recorded as completed runtime verification.

### StalePositionQuorumTest source review

I reviewed
[`StalePositionQuorumTest.java`](../../../aeron-system-tests/src/test/java/io/aeron/cluster/StalePositionQuorumTest.java),
initial source SHA-256
`0a1eece434d4d241b34cd9d88838af8f1ee8fe19157dd9d92c0aa34beec424a7`,
and its `TestCluster`, `TestNode.TestService`, `ClusterInstrumentor`, and
`LogPublisher` dependencies. **No blocking source finding** in this version.
The root agent is running Gradle; I did not run concurrent Gradle tasks or
independently claim its runtime outcome.

The test addresses the missing implementation-level connection between a
position mismatch, a real service reply, and subsequent recovery:

1. A baseline is acknowledged and applied on all members. Both followers stop,
   an 8 KiB old-term message is actually appended and recorded at the incumbent,
   and the test verifies it has no client acknowledgement before stopping that
   leader. The remaining shorter recordings elect a leader and commit a
   distinct new-term marker.
2. The new voter stops. The old recording restarts with receive-side consensus
   handling suppressed. The old member must be in CANVASS and the new leader
   must actually receive its old-term position. The target's actual log append
   position must be above the stopped voter's recording and covered by the old
   tail. The test then observes execution of the real quorum method and polls
   actual client egress for two seconds while requiring the new leader to stay
   CLOSED with the same election count.
3. A fixed run must then obtain a real acknowledgement and application at a
   returned current-term replica. If an unsafe acknowledgement was already
   observed, this replication step is deliberately skipped so it cannot repair
   the evidence. In both branches the new leader stops before the other two
   heal. A fresh RECOVERY message must be acknowledged and applied on both
   survivors before target-identity assertions run. This avoids interpreting
   slow recovery as loss of an entry.

Instrumentation separates observations from faults. `DropAtOldMember`
suppresses six inbound consensus handlers. `ObserveMessage` reads the successful
return of `LogPublisher.appendMessage`; it does not manufacture a log position.
`ObserveStalePosition` runs after the real cache update and requires the source
member and old term. `ObserveQuorumPoll` only counts calls. None of the observers
change logs, votes, method return values, or cache contents. The service tracks
the unique payload bits before invoking `TestService`'s real echo path, so the
client bit is an egress observation, not an inference from a commit counter.
`TestCluster.startStaticNode` creates a new service instance from its supplier
and preserves files when passed `false`; the old member's recovered target bit
cannot have survived in its pre-restart service object.

**Low-severity scope limit:** these are retained-file stop/restart failures.
`TestCluster.stopNode:719-722` invokes `TestNode.close`, whose implementation
closes the consensus module, service containers, archive, and media driver
(`TestNode:283-289`). That is not an abrupt process kill or a power-loss test.
The test contains no snapshot, and `WriteTrackingService` does not serialize
its extra tracking bits in snapshots; keep snapshots outside this regression's
scope unless that state is added to its snapshot format.

An optional further diagnostic is to assert that neither survivor applies
`OLD_TAIL` after recovery. The existing target-identity assertions are the
essential safety checks; no claim of a source blocker depends on that optional
assertion.

I also reviewed the revised source, SHA-256
`9cb4983615bc0d95ce19ed38c6db1783a62d25cd273b8bb5b2fd04ee2fb2774d`.
`keepTransportConnected` adds a real UDP subscription and publication
destination, with no archive recorder or consensus position report. This is
a valid way to supply transport connectivity while all recording peers are
stopped: `ConsensusModuleAgent:1656` disables spies simulating a connection for
multi-member clusters. It is an explicit transport setup intervention, in
addition to node stops and receive-side drops. It does not invent a quorum
position. `lastPublisher` is observed at a successful real append; the baseline
and new-term marker precede the respective transport setup calls. The revised
test reconnects the client after removing the old leader.

I inspected the root agent's actual JUnit XML and the isolated mutation diff;
I did not independently execute Gradle:

| Run | Observed result |
|---|---|
| Fixed implementation, JUnit timestamp `2026-09-28T15:36:13.249Z` | Passed; old term 0, old tail 8800, stopped voter 864, target 960, premature ACK false, recovered target true |
| Filter-only mutant, JUnit timestamp `2026-09-28T15:40:00.561Z` | Failed all three intended assertions; the same positions, premature ACK true, target absent on both healed survivors |

The mutant is in `/tmp/aeron-stale-position-control-20260928`; its Java diff
removes only `member.leadershipTermId == leadershipTermId` from
`ClusterMember.quorumPosition`. Its test-source hash matches the revised source
above. The failure is an actual premature egress reply followed by successful
recovery without the target, not a timeout or an instrumentation failure.

### Consecutive-handoff/crash model

The additional model improves on the original handoff cut by deriving winners
from actual positive/negative ballot outcomes, retaining request log tuples,
and separating persistent ballot, volatile promise/accepted term, recording
metadata, and physical term events. It remains a fixed-candidate, bounded
schedule (members 1 then 2), with one retained historical report, atomic prefix
copying, and no complete transport or replay-notification state machine.

The final safety cut projects away the separate replayed agent term present in
earlier revisions. Leader service can begin after leader join and publication
of its term event; it does not require follower application. `Replay` still
advances metadata from recorded term events, and catch-up completion can advance
metadata and phase together once the event is present. The important windows
of metadata before recording and recording before metadata remain expressible.
This reduction does not remove a prerequisite of `Write` or `Ack` that Java
requires. It is reasonable for these entry-preservation safety properties,
with no claim that the model checks actual replay timing or two-election
liveness. The separate `Replay.tla` checks the narrower progress obligations
described above; no composition theorem joins their results.

The initial review found several concrete correspondence problems, which the
root agent corrected in the evolving source:

- A negative vote for an inferior log persists a higher ballot term even
  during follower catch-up/replay. The initial model allowed that only in
  canvass/ballot phases. The revised `Vote` separates phase-independent
  rejection/persistence from phase-dependent positive voting, following
  `Election:360-379`.
- `accepted >= persisted ballot` is not a Java invariant: a rejected higher
  ballot can advance the latter while a follower continues in its old accepted
  term. It was replaced with the narrower `NoStaleAction` check at announcement
  handling.
- A crash-before-either-election counterexample did not demonstrate a crash
  between handoffs. The revised crash classification records the actual cut;
  separate witnesses target volatile acceptance loss, metadata before the term
  event, and an acknowledged write before the second victory.
- Frozen initial announcement metadata excluded same-term rejoin. The revised
  acceptance case also permits current-term metadata; it does not truncate that
  case to an earlier term's base. Java's closed leader tailors its reply to a
  canvassing member's reported metadata (`ConsensusModuleAgent:932-950`).
- Replay of a new term event must advance both agent and follower metadata
  terms, as `Election.onReplayNewLeadershipTermEvent:608-614` does.

The initial `TypeOK` bound of four entries was also too short for the legitimate
five-entry shape `old-a, old-b, e1, x, e2`; the root agent corrected it. A failed
type bound was not evidence that the intended safety properties passed.

The final two-handoff witness requires historical acknowledgement of `x`, a
crash between the victories, and `e2` in the second leader's recording.
`TermEvent(2)` requires quorum support through the second term's **base before
appending** `e2`; the witness does not require a quorum to have recorded `e2`
itself, a second client acknowledgement, or completed recovery at every node.
This is adequate nonvacuity for the stated second-victory preservation check,
with that endpoint kept explicit.

#### H1 — High, resolved: catch-up recording before metadata can lose an acknowledged write

The first version allowed new recording only after its `Metadata` action had
advanced the follower's log metadata to the accepted term. That excludes the
actual `FOLLOWER_CATCHUP` path: the archive can record a term event and client
data while replay remains limited by an older commit notification. The new
accepted-term position report can then make the leader commit those bytes
before the follower learns their log term through replay.

Simply permitting every pre-metadata follower to record also admitted a
spurious early live-join path. I explicitly identified that issue: Java chooses
live join when the initial announcement has no ahead-of-follower log position,
and `followerLogAwait` stamps metadata before recording. The root agent split
live join (`J`) from true catch-up (`A`). A valid late catch-up requires a
leader log position greater than the follower's local append position when
the announcement is accepted.

I independently checked a stricter experimental copy with all three of these
conditions:

1. A pre-metadata follower can record only when its acceptance observed a
   longer leader log (true catch-up).
2. Replaying a term event advances the follower's metadata as well as agent
   term.
3. A leader cannot append its new term event until a supporting follower
   already has the new metadata term (forcing an initial real live join).

TLC still violated `AcknowledgedOnQuorum` after **17 states**, exploring
287,927 distinct states in approximately five seconds. Frozen experimental
source, configuration, command manifest, and complete trace are in
[`results/independent-catchup-review-20260928`](results/independent-catchup-review-20260928/manifest.json).
This is a diagnostic experiment, not a checked refinement. Its salient suffix
is:

1. An early live follower has metadata 1 but has not recorded the new event.
   The new leader records `e1, x`.
2. A different, late follower accepts term 1, records `e1, x` during catch-up,
   and still has metadata/agent term 0. Its accepted-term position supports
   acknowledging `x`.
3. Before replay advances its metadata, it returns to CANVASS.
4. A same-leader term-1 announcement addressed to its old metadata term
   truncates its recording to the term-1 base. Only the leader now retains `x`.

The corresponding Java paths in the uncorrected revision are:

- `catchupPoll:1988-2004` polls only through `min(recorded, notified)` and then
  reports the recorded position in `election.leadershipTermId()`.
- `followerCatchupAwait:1079-1086` joins without calling `updateRecordingLog`,
  unlike live `followerLogAwait:1159-1162`. `startLogRecording:2365-2371` only
  starts/extends the archive; it does not discover term events in that tail.
- `prepareForNewLeadership` refreshes the append position from the stopped
  archive, while `resetMembers` uses the existing `logLeadershipTermId`.
  Startup likewise derives the log term from the last recording-log entry:
  `RecordingLog.planRecovery:1754-1772` pairs that metadata term with the
  archive's stop position instead of scanning raw recorded term events.
- `onNewLeadershipTerm:455-467` truncates when the announced next-term base is
  below local append position, before assigning the new accepted term. A
  same-term reannouncement is not rejected by the candidate-term guard.

The model finding alone was treated as a hypothesis. The root agent then added
`shouldRetainAcknowledgedCatchupWriteAcrossMetadataLagAndFailover` to the real
system test. I reviewed its source and inspected both runtime logs. The fault
schedule holds `appendNewLeadershipTermEvent` with simulated publication
backpressure until a live follower has the new metadata term and is stopped;
the leader then appends the event and target. A late member takes true catch-up.
Dropping its inbound commit messages lets its real accepted-term recording
report support a client ACK while its replay remains behind. The sole leader
then stops, the earlier voter returns, and a new RECOVERY write establishes
that both survivors have recovered before checking target identity.

The uncorrected run in `/tmp/aeron-catchup-metadata.log` produced:

```text
oldTerm=0 acceptedTerm=1 voter=384 target=576 acknowledged=true recoveredTarget=false
```

Both survivors' acknowledged-target assertions failed. This was actual egress
followed by recovery without the entry, not a model-only position mismatch or
a timeout. No position, vote, term field, or replay limit was fabricated by the
test. The initial regression source and Java remain distinct from the new
correction; the log is retained as the failing implementation witness.

The later election explains the loss: the late member advertises the tuple
`(log term 0, position 576)`, while the earlier live-joined voter restarts with
`(log term 1, position 384)` despite never recording the term event. The log
comparator ranks the latter tuple higher. Its leader announcement supplies the
term-1 base at 384, allowing the surviving recorded target to be truncated.
With the correction, the late member instead advertises `(1, 576)` and rejects
the shorter candidate. Persisting only the candidate ballot term would not
repair this incorrect log ordering.

The correction adds `updateRecordingLog(nowNs)` immediately after a successful
`tryJoinLogAsFollower` in `followerCatchupAwait`, before entering
`FOLLOWER_CATCHUP`. This matches the existing live-join path. The corrected
system-test XML (`2026-09-28T15:52:02.304Z`) reports both test methods passing;
the catch-up evidence now has `catchupMetadataTerm=1` and
`recoveredTarget=true` for the same voter/target positions. The first corrected
Gradle invocation also reported a Checkstyle wrapping failure, so its runtime
pass must not be conflated with an entirely successful build. Formatting and
broader validation were subsequently completed by the root agent, including
the successful final exact-source rerun documented above. I inspected the
archived XML in
[`results/system-20260928/system-regressions`](results/system-20260928/system-regressions):
30 executed system cases pass, including both new methods, 24 `ClusterTest`
cases, both `AcknowledgedWriteDurabilityTest` cases, and the failed-election
and failed-catch-up recovery cases. `RacingCatchupClusterTest` is skipped, so
it contributes no executed coverage. An intermediate unit/style rerun in
`/tmp/aeron-election-style-unit.log` ends `BUILD SUCCESSFUL` with 49 passing
`RecordingLogTest` cases, including the three new join positions. The later
full cluster-unit archive confirms 655 executed passes and three skips. I
inspected this evidence rather than rerunning Gradle independently.

I reviewed the correction's ordering and preconditions:

- A follower below the accepted term base is routed through
  `FOLLOWER_LOG_REPLICATION`, including intervening historical terms, before
  it can take replay and catch-up. A conflicting older tail is truncated first.
- `followerReplay` must reach local append position before entering catch-up,
  and `followerCatchupAwait` verifies the image join position equals that replay
  position. For first entry into the accepted term with older metadata, the
  compatible prefix therefore reaches that term's base. For an already known
  current term, `RecordingLog.ensureCoherent` preserves the existing base rather
  than replacing it with the later rejoin position.
- Archive recording can begin inside `tryJoinLogAsFollower`, but no accepted-term
  catch-up position is sent until after it returns and metadata is updated.
  A crash before that update cannot have supplied new quorum support from this
  follower. As elsewhere, machine/power-loss guarantees depend on configured
  sync semantics and are outside this retained-file failure test.

No additional blocker was identified for this placement. The important broader
validation cases are replication over skipped terms and rejoining within an
already-known term without changing its original base. The bounded model must
retain the old-metadata catch-up behavior as an explicit negative control;
silently forbidding it would recreate the abstraction error that hid H1.

The added `RecordingLogTest.shouldPreserveExistingTermBaseWhenCatchupRejoinsLaterPosition`
checks join positions 1024, 4096, and 8192 against an existing term base of 1024,
then reloads the file and verifies the original base, uncommitted end, and
single term entry. This directly checks the correction's same-term rejoin
precondition rather than merely mirroring its new call.

### Competing-ballot model

I reviewed the additional `Ballots.tla` source. It isolates two-term competing
candidates with equal log eligibility and one retained-file process crash.
Retaining historical YES replies and omitting delivered NO replies increases
winning opportunities, which is conservative for its one-vote/one-leader
safety checks. The promise/phase guards prevent reuse of a reply in a different
candidate term. Its nomination term calculation matches the maximum proposal
against persistent state, and its crash flag observes actual volatile
acceptance above both persistent ballot and metadata terms. With uniform
initial state, fixing the crash to member 0 is symmetric in this isolated
model. No blocking source finding; no claim of general log eligibility or
ballot liveness.

I inspected the root agent's completed run in
[`results/20260928T104144-ps39i8hk/summary.json`](results/20260928T104144-ps39i8hk/summary.json).
All four cases match their expected outcomes, with unchanged frozen inputs.
The positive search completes with 889,869 distinct states and an empty queue.
The no-persistence control violates `OneLeaderPerTerm`; the separate crash and
volatile-acceptance witnesses also reach their intended cuts. This is inspected
runtime evidence, not a second independent execution of the new model. The
final frozen 47-case run repeats all four outcomes with the same `Ballots`
source hash and positive state count.
