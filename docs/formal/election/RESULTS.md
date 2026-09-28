# Measured results — 28 September 2026

All **47/47 expected outcomes** matched: **22 completed passes**, **15 defect/persistence controls**, and **10 reachability/fairness witnesses**. Summed TLC process time was **295.062 seconds** (4 minutes 55 seconds). Every positive case completed with an empty queue. A counterexample is counted as an expected control/witness, never as a safety pass.

The expanded checks found another real acknowledged-write-loss defect on the original branch. The working-tree correction and its reproductions are explained in [FOLLOW-UP.md](FOLLOW-UP.md); the source review is in [INDEPENDENT-REVIEW.md](INDEPENDENT-REVIEW.md).

These bounded models are not equivalent to the original full protocol model and do not prove arbitrary election histories or power-loss durability.

## Reproduce

```sh
python3 docs/formal/election/run.py --jar /path/to/tla2tools.jar
```

One worker, 1 GiB heap, seed 1, fingerprint polynomial 0, 120-second limit per case. The largest traversal was **1,342,833 distinct states**, completed in **77.016 seconds**. Timeout or a nonempty queue on a claimed pass fails the runner.

Tool banner: **TLC2 Version 2.19, 08 August 2024, revision 5a47802**. Java: Azul Zulu 17.0.11 on macOS 26.7. The local jar filename contains `1.7.4`; its banner and hash identify the actual tool.

Jar SHA-256: `936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.

[Frozen evidence and commands](results/20260928T112828-grzk9xan/summary.json) includes models, runner, generated configs, logs, Java runtime, source hashes, and tracked source status. Both `inputs_unchanged` and `implementation_unchanged` are **true**. Java commit is `0861a99a81230bb7430b6274965f3a79b55fe181` plus the documented working-tree correction and tests. Earlier exploratory failures/timeouts are retained locally and excluded from these totals.

## Final matrix

| Check | Expected outcome observed | Distinct states | Seconds |
|---|---|---:|---:|
| [ballots](results/20260928T112828-grzk9xan/ballots.out) | Pass; empty queue | 889,869 | 14.899 |
| [ballots-no-persistence](results/20260928T112828-grzk9xan/ballots-no-persistence.out) | Counterexample | 3,562 | 1.030 |
| [ballots-crash-witness](results/20260928T112828-grzk9xan/ballots-crash-witness.out) | Witness | 4,353 | 1.025 |
| [ballots-volatile-witness](results/20260928T112828-grzk9xan/ballots-volatile-witness.out) | Witness | 651 | 0.929 |
| [consecutive-crash-0](results/20260928T112828-grzk9xan/consecutive-crash-0.out) | Pass; empty queue | 1,342,833 | 77.016 |
| [consecutive-crash-1](results/20260928T112828-grzk9xan/consecutive-crash-1.out) | Pass; empty queue | 1,031,363 | 46.171 |
| [consecutive-crash-2](results/20260928T112828-grzk9xan/consecutive-crash-2.out) | Pass; empty queue | 1,302,682 | 58.059 |
| [consecutive-witness](results/20260928T112828-grzk9xan/consecutive-witness.out) | Witness | 196,320 | 7.249 |
| [consecutive-volatile-witness](results/20260928T112828-grzk9xan/consecutive-volatile-witness.out) | Witness | 523 | 1.090 |
| [consecutive-metadata-witness](results/20260928T112828-grzk9xan/consecutive-metadata-witness.out) | Witness | 970 | 1.000 |
| [consecutive-old-guard](results/20260928T112828-grzk9xan/consecutive-old-guard.out) | Counterexample | 56 | 0.924 |
| [consecutive-old-filter](results/20260928T112828-grzk9xan/consecutive-old-filter.out) | Counterexample | 1,558 | 1.044 |
| [consecutive-old-catchup-metadata](results/20260928T112828-grzk9xan/consecutive-old-catchup-metadata.out) | Counterexample | 58,029 | 3.931 |
| [handoff-base-0](results/20260928T112828-grzk9xan/handoff-base-0.out) | Pass; empty queue | 26,645 | 2.292 |
| [handoff-base-1](results/20260928T112828-grzk9xan/handoff-base-1.out) | Pass; empty queue | 277,626 | 9.164 |
| [handoff-base-2](results/20260928T112828-grzk9xan/handoff-base-2.out) | Pass; empty queue | 1,056,583 | 39.240 |
| [handoff-old-guard](results/20260928T112828-grzk9xan/handoff-old-guard.out) | Counterexample | 907 | 1.129 |
| [handoff-old-quorum](results/20260928T112828-grzk9xan/handoff-old-quorum.out) | Counterexample | 1,392 | 1.018 |
| [handoff-can-ack](results/20260928T112828-grzk9xan/handoff-can-ack.out) | Witness | 1,854 | 1.015 |
| [handoff-can-reject](results/20260928T112828-grzk9xan/handoff-can-reject.out) | Witness | 17 | 0.857 |
| [nomination-3](results/20260928T112828-grzk9xan/nomination-3.out) | Pass; empty queue | 1,500 | 0.958 |
| [nomination-5](results/20260928T112828-grzk9xan/nomination-5.out) | Pass; empty queue | 150,000 | 1.874 |
| [nomination-old-stamp](results/20260928T112828-grzk9xan/nomination-old-stamp.out) | Counterexample | Initial-state failure | 0.863 |
| [quorum-1](results/20260928T112828-grzk9xan/quorum-1.out) | Pass; empty queue | 36 | 0.855 |
| [quorum-3](results/20260928T112828-grzk9xan/quorum-3.out) | Pass; empty queue | 5,184 | 1.003 |
| [quorum-5](results/20260928T112828-grzk9xan/quorum-5.out) | Pass; empty queue | 65,536 | 1.483 |
| [quorum-old-filter](results/20260928T112828-grzk9xan/quorum-old-filter.out) | Counterexample | Initial-state failure | 0.921 |
| [stale-closed](results/20260928T112828-grzk9xan/stale-closed.out) | Pass; empty queue | 44 | 1.047 |
| [stale-unfinished](results/20260928T112828-grzk9xan/stale-unfinished.out) | Pass; empty queue | 44 | 0.907 |
| [stale-no-companion](results/20260928T112828-grzk9xan/stale-no-companion.out) | Counterexample | 16 | 0.856 |
| [stale-no-stepdown](results/20260928T112828-grzk9xan/stale-no-stepdown.out) | Counterexample | 44 | 0.862 |
| [stale-fair-witness](results/20260928T112828-grzk9xan/stale-fair-witness.out) | Witness | 44 | 0.872 |
| [replay-zero](results/20260928T112828-grzk9xan/replay-zero.out) | Pass; empty queue | 205 | 0.921 |
| [replay-retained](results/20260928T112828-grzk9xan/replay-retained.out) | Pass; empty queue | 205 | 0.970 |
| [replay-partial](results/20260928T112828-grzk9xan/replay-partial.out) | Pass; empty queue | 217 | 0.970 |
| [replay-available](results/20260928T112828-grzk9xan/replay-available.out) | Pass; empty queue | 173 | 0.913 |
| [replay-single](results/20260928T112828-grzk9xan/replay-single.out) | Pass; empty queue | 115 | 0.912 |
| [replay-old-report](results/20260928T112828-grzk9xan/replay-old-report.out) | Counterexample | 8 | 0.869 |
| [replay-old-self](results/20260928T112828-grzk9xan/replay-old-self.out) | Counterexample | 8 | 0.845 |
| [replay-old-filter-term](results/20260928T112828-grzk9xan/replay-old-filter-term.out) | Counterexample | 5 | 0.811 |
| [replay-fair-witness](results/20260928T112828-grzk9xan/replay-fair-witness.out) | Witness | 205 | 0.919 |
| [catchup](results/20260928T112828-grzk9xan/catchup.out) | Pass; empty queue | 344 | 0.928 |
| [catchup-old-term](results/20260928T112828-grzk9xan/catchup-old-term.out) | Counterexample | 184 | 0.912 |
| [catchup-fair-witness](results/20260928T112828-grzk9xan/catchup-fair-witness.out) | Witness | 344 | 0.969 |
| [lost-0](results/20260928T112828-grzk9xan/lost-0.out) | Pass; empty queue | 6 | 0.817 |
| [lost-1](results/20260928T112828-grzk9xan/lost-1.out) | Pass; empty queue | 6 | 0.854 |
| [lost-no-timeout](results/20260928T112828-grzk9xan/lost-no-timeout.out) | Counterexample | 4 | 0.869 |

## Java validation

- Full cluster unit suite: **658 total, 655 executed passes, 3 preexisting skips**, zero failures/errors. This includes three new recording-metadata rejoin/base-position cases.
- Expanded system selection: **31 total, 30 executed passes, 1 preexisting skip**, zero failures/errors. Includes both new regressions, both acknowledged-write regressions, 24 `ClusterTest` cases, failed-first-election recovery, and recovery after failed catch-up.
- Java 21 (Zulu 21.52.203.0); cluster main/test and system-test Checkstyle passed after line-wrap fixes. Final exact-source regression rerun is recorded in the system evidence below.
- Quorum-filter control: remove only the term comparison from the corrected working tree. Actual target ACK occurs before a current-term replica exists, followed by target loss from both recovered survivors.
- Catch-up metadata control: original `0861a99a81` implementation, same final test. Actual target ACK at position 576 with catch-up metadata term 0, followed by target loss. The corrected metadata term is 1 and the target survives.

[System validation artifacts](results/system-20260928/summary.json) include exact test/source hashes, fixed and control logs/XML, and source snapshots. These are retained-file stop/restart tests; transport-only subscribers provide connectivity without recording or consensus participation. Negative controls were run in an isolated checkout and did not change the primary checkout.

## Counterexample interpretation

- **Catch-up metadata omitted:** late recording receives `e1,x` while metadata remains old, supplies current-term quorum support, and the leader acknowledges `x`. Returning to CANVASS and processing a leadership announcement can truncate that acknowledged tail. The final model trace has 15 states. The independent review also reproduced a stronger live-join-first cut; the Java regression confirms loss through the next election.
- **Ballot persistence omitted:** one crashed member votes again in the same term, allowing two historical winners. This validates an essential assumption, not a separate newly claimed Java bug.
- **Candidate-term guard omitted:** an announcement below the effective promise is acted on. The original Handoff control independently demonstrates acknowledged-entry truncation from the stale-leader race.
- **Quorum term filter omitted:** an unrelated long old recording supports acknowledgement of a new entry it does not contain. The system control proves this with actual client egress and surviving-recording recovery.
- **Replay/progress controls:** disabling the retained-position report, accepted catch-up term, self stamp, stale-leader escape, or waiting timeout exposes the intended isolated dependency failure under each stated suffix assumption.

## Frozen executable input hashes

| File | SHA-256 |
|---|---|
| `Ballots.tla` | `4219ee3a2fce178d767a383306ba90a66f5fa42ed3b533b771ee19440a860cd2` |
| `Consecutive.tla` | `46ea37d6d0519deb69d29969b86279e2307e73eef6c423965bda4cc3d1c2187e` |
| `Handoff.tla` | `5361f6bda073f93e0dfc29309fd7f56ee5a9ae1c944613e8debbac20d0c022a2` |
| `Nomination.tla` | `0077afeaa85dc2f63b7f3d587425fdb9d275e60a9e7c07416a8413179eae891a` |
| `Quorum.tla` | `9095299995156cbe9f19ac94c3bdc3ad2ffafb3135b52cda4e28eba889147640` |
| `Replay.tla` | `3e6f9d1d2aa26175123129d4d9ee38c218034e61661c6ea503c6d44961d39e20` |
| `StaleRecovery.tla` | `9237fb404bed2b2bec4644749f5a97f6c081d64ec5ad994529b5ce4f43bf83cf` |
| `run.py` | `068b4c9e425f5e62312566e20fdbbecfee4fa13ac72170d96cfe5571b7abd2f2` |

Relevant Java source hashes and exact tracked-change status are in the linked summary. No model is generated from Java; correspondence still requires review.
