#!/usr/bin/env python3
"""Run the bounded election checks, including exact expected negative controls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent


def java_evidence():
    repo = ROOT.parents[2]
    paths = ["aeron-cluster/src/main/java/io/aeron/cluster/" + name + ".java" for name in
             ("Election", "ConsensusModuleAgent", "ClusterMember", "RecordingLog", "NodeStateFile")]
    paths += ["aeron-system-tests/src/test/java/io/aeron/cluster/StalePositionQuorumTest.java",
              "aeron-cluster/src/test/java/io/aeron/cluster/RecordingLogTest.java"]

    def git(*args):
        result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None

    return dict(commit=git("rev-parse", "HEAD"), tracked_status=git("status", "--porcelain", "-uno"),
                sources={p: hashlib.sha256((repo / p).read_bytes()).hexdigest()
                         for p in paths if (repo / p).is_file()})


def cases():
    result = {}

    def add(name, module, constants, invariants=(), prop=None, expected=None, spec=False):
        result[name] = dict(module=module, constants=constants, invariants=list(invariants),
                            property=prop, expected=expected or "pass", spec=spec)

    consecutive = dict(CrashNode=0, GuardCandidateTerm=True, FilterReports=True, InitialOldTail=0,
                       StampCatchupMetadata=True)
    add("ballots", "Ballots", dict(PersistBallots=True),
        ["TypeOK", "OneVotePerTerm", "OneLeaderPerTerm"])
    add("ballots-no-persistence", "Ballots", dict(PersistBallots=False),
        ["TypeOK", "OneLeaderPerTerm"], expected="invariant:OneLeaderPerTerm")
    add("ballots-crash-witness", "Ballots", dict(PersistBallots=True),
        ["TypeOK", "NoReelectionAfterCrash"], expected="invariant:NoReelectionAfterCrash")
    add("ballots-volatile-witness", "Ballots", dict(PersistBallots=True),
        ["TypeOK", "NoVolatileAcceptanceLoss"], expected="invariant:NoVolatileAcceptanceLoss")
    cinv = ["TypeOK", "AcknowledgedOnQuorum", "NextLeaderContainsAck", "NoAcknowledgedTruncation",
            "NoStaleAction"]
    for node in range(3):
        add(f"consecutive-crash-{node}", "Consecutive", dict(consecutive, CrashNode=node), cinv)
    add("consecutive-witness", "Consecutive", dict(consecutive, CrashNode=1), ["TypeOK", "NoTwoHandoffs"],
        expected="invariant:NoTwoHandoffs")
    add("consecutive-volatile-witness", "Consecutive", consecutive, ["TypeOK", "NoVolatileAcceptanceLoss"],
        expected="invariant:NoVolatileAcceptanceLoss")
    add("consecutive-metadata-witness", "Consecutive", consecutive, ["TypeOK", "NoMetadataBeforeEventCrash"],
        expected="invariant:NoMetadataBeforeEventCrash")
    add("consecutive-old-guard", "Consecutive", dict(consecutive, GuardCandidateTerm=False),
        ["TypeOK", "NoStaleAction"], expected="invariant:NoStaleAction")
    add("consecutive-old-filter", "Consecutive", dict(consecutive, FilterReports=False, InitialOldTail=2),
        ["TypeOK", "AcknowledgedOnQuorum"], expected="invariant:AcknowledgedOnQuorum")
    add("consecutive-old-catchup-metadata", "Consecutive", dict(consecutive, StampCatchupMetadata=False),
        ["TypeOK", "AcknowledgedOnQuorum"], expected="invariant:AcknowledgedOnQuorum")

    handoff = dict(GuardCandidateTerm=True, FilterReports=True, Base=0, OldLimit=2)
    inv = ["TypeOK", "AcknowledgedOnQuorum", "NoAcknowledgedTruncation"]
    for base in (0, 1, 2):
        add(f"handoff-base-{base}", "Handoff", dict(handoff, Base=base), inv)
    add("handoff-old-guard", "Handoff", dict(handoff, GuardCandidateTerm=False),
        ["TypeOK", "NoAcknowledgedTruncation"], expected="invariant:NoAcknowledgedTruncation")
    add("handoff-old-quorum", "Handoff", dict(handoff, FilterReports=False), inv,
        expected="invariant:AcknowledgedOnQuorum")
    add("handoff-can-ack", "Handoff", handoff, ["TypeOK", "NoNewAcknowledgement"],
        expected="invariant:NoNewAcknowledgement")
    add("handoff-can-reject", "Handoff", handoff, ["TypeOK", "NoStaleRejection"],
        expected="invariant:NoStaleRejection")

    for members in (3, 5):
        add(f"nomination-{members}", "Nomination", dict(Members=members, LogTermStamp=True),
            ["HonestAssessment", "QuorumAssessment", "UnanimousAssessment"])
    add("nomination-old-stamp", "Nomination", dict(Members=3, LogTermStamp=False),
        ["UnanimousAssessment"], expected="invariant:UnanimousAssessment")

    for members, maximum in ((1, 2), (3, 2), (5, 1)):
        add(f"quorum-{members}", "Quorum", dict(Members=members, MaxPosition=maximum, FilterReports=True),
            ["Supported", "Maximal", "Bounded"])
    add("quorum-old-filter", "Quorum", dict(Members=3, MaxPosition=2, FilterReports=False),
        ["Supported"], expected="invariant:Supported")

    recovery = dict(Companion=True, UnfinishedStepsDown=True, InitiallyClosed=True)
    for closed in (True, False):
        add("stale-" + ("closed" if closed else "unfinished"), "StaleRecovery",
            dict(recovery, InitiallyClosed=closed), ["TypeOK"], "EventuallyOpened", spec=True)
    add("stale-no-companion", "StaleRecovery", dict(recovery, Companion=False), ["TypeOK"],
        "EventuallyOpened", "temporal:EventuallyOpened", True)
    add("stale-no-stepdown", "StaleRecovery", dict(recovery, InitiallyClosed=False, UnfinishedStepsDown=False),
        ["TypeOK"], "EventuallyOpened", "temporal:EventuallyOpened", True)
    add("stale-fair-witness", "StaleRecovery", recovery, ["TypeOK"], "NoFairOpening",
        "temporal:NoFairOpening", True)

    replay = dict(Mode="replay", InitialApplied=1, InitialNotified=1, Members=3,
                  ReportWhileWaiting=True, CatchupAcceptedTerm=True, StampSelf=True,
                  ElectionTermFilter=True, WaitTimeout=True)
    rinv = ["TypeOK", "PositionOrder", "CommitHasCurrentTermSupport"]
    for applied, notified, label in [(0, 0, "zero"), (1, 1, "retained"), (0, 1, "partial"), (1, 2, "available")]:
        add(f"replay-{label}", "Replay", dict(replay, InitialApplied=applied, InitialNotified=notified),
            rinv, "EventuallyRecovered", spec=True)
    add("replay-single", "Replay", dict(replay, Members=1), rinv, "EventuallyRecovered", spec=True)
    add("replay-old-report", "Replay", dict(replay, ReportWhileWaiting=False), rinv,
        "EventuallyRecovered", "temporal:EventuallyRecovered", True)
    add("replay-old-self", "Replay", dict(replay, StampSelf=False), rinv,
        "EventuallyRecovered", "temporal:EventuallyRecovered", True)
    add("replay-old-filter-term", "Replay", dict(replay, ElectionTermFilter=False), rinv,
        expected="invariant:CommitHasCurrentTermSupport")
    add("replay-fair-witness", "Replay", replay, rinv, "NoFairRecovery", "temporal:NoFairRecovery", True)
    add("catchup", "Replay", dict(replay, Mode="catchup"), rinv, "EventuallyRecovered", spec=True)
    add("catchup-old-term", "Replay", dict(replay, Mode="catchup", CatchupAcceptedTerm=False), rinv,
        "EventuallyRecovered", "temporal:EventuallyRecovered", True)
    add("catchup-fair-witness", "Replay", dict(replay, Mode="catchup"), rinv,
        "NoFairRecovery", "temporal:NoFairRecovery", True)
    for initial in (0, 1):
        add(f"lost-{initial}", "Replay", dict(replay, Mode="lost", InitialApplied=initial, InitialNotified=initial),
            rinv, "EventuallyCanvass", spec=True)
    add("lost-no-timeout", "Replay", dict(replay, Mode="lost", WaitTimeout=False), rinv,
        "EventuallyCanvass", "temporal:EventuallyCanvass", True)
    return result


def config(case):
    def tla(value):
        return str(value).upper() if isinstance(value, bool) else json.dumps(value)
    lines = ["CONSTANTS"] + [f"    {k} = {tla(v)}" for k, v in case["constants"].items()]
    lines += ["SPECIFICATION Spec"] if case["spec"] else ["INIT Init", "NEXT Next"]
    lines += ["CHECK_DEADLOCK FALSE"]
    if case["invariants"]:
        lines += ["INVARIANTS"] + [f"    {x}" for x in case["invariants"]]
    if case["property"]:
        lines += ["PROPERTY " + case["property"]]
    return "\n".join(lines) + "\n"


def main():
    checks = cases()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("checks", nargs="*", help="Named checks; default is all. Use --list.")
    parser.add_argument("--jar", default=os.environ.get("TLA2TOOLS_JAR"))
    parser.add_argument("--java", default=os.environ.get("TLC_JAVA", "java"))
    parser.add_argument("--timeout", type=int, default=120, help="Seconds per check; timeout is failure, never a pass")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        for name, case in checks.items():
            print(f"{name}: {case['expected']}")
        return 0
    names = args.checks or list(checks)
    if any(x not in checks for x in names):
        parser.error("Unknown check name")
    if not args.jar or not Path(args.jar).is_file():
        parser.error("Pass --jar /path/to/tla2tools.jar or set TLA2TOOLS_JAR")
    jar = Path(args.jar).resolve()
    results = ROOT / "results"
    results.mkdir(exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix=time.strftime("%Y%m%dT%H%M%S-"), dir=results))
    inputs = run / "inputs"
    inputs.mkdir()
    for path in [*ROOT.glob("*.tla"), Path(__file__)]:
        shutil.copy2(path, inputs / path.name)
    for name in names:
        (inputs / f"{name}.cfg").write_text(config(checks[name]))
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in inputs.iterdir()}
    jar_hash = hashlib.sha256(jar.read_bytes()).hexdigest()
    java_version = subprocess.run([args.java, "-version"], capture_output=True, text=True, check=True).stderr.strip()
    summary = dict(java=java_version, jar=str(jar), jar_sha256=jar_hash, inputs=hashes,
                   implementation=java_evidence(), checks=[])
    print(f"Evidence: {run}", flush=True)
    for name in names:
        case = checks[name]
        scratch = tempfile.mkdtemp(prefix="aeron-election-tlc-")
        command = [args.java, "-Xmx1g", "-XX:+UseParallelGC", "-cp", str(jar), "tlc2.TLC",
                   "-workers", str(args.workers), "-fp", "0", "-seed", "1", "-lncheck", "final",
                   "-metadir", scratch, "-config", f"{name}.cfg", f"{case['module']}.tla"]
        started = time.monotonic()
        timed_out = False
        with (run / f"{name}.out").open("w") as output:
            process = subprocess.Popen(command, cwd=inputs, stdout=output, stderr=subprocess.STDOUT)
            try:
                code = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                code = process.returncode
        text = (run / f"{name}.out").read_text()
        expected = case["expected"]
        if expected == "pass":
            matched = code == 0 and "Model checking completed. No error has been found." in text
        elif expected.startswith("invariant:"):
            property_name = expected.split(":", 1)[1]
            matched = code == 12 and (f"Invariant {property_name} is violated" in text
                                     or f"Invariant {property_name} is violated by the initial state" in text)
        else:
            # Each config checks exactly one named temporal property. Reject
            # parser errors, deadlocks, other invariants, and timeout exits.
            matched = code == 13 and "Temporal properties were violated" in text
        counts = re.findall(r"([\d,]+) states generated, ([\d,]+) distinct states found, ([\d,]+) states left on queue", text)
        record = dict(name=name, **case, command=command, exit_code=code, timeout=timed_out,
                      matched=matched and not timed_out, seconds=round(time.monotonic()-started, 3))
        if counts:
            record.update(zip(("generated", "distinct", "queue"), [int(x.replace(",", "")) for x in counts[-1]]))
        if expected == "pass":
            record["matched"] = record["matched"] and record.get("queue") == 0
        record["summary"] = [line for line in text.splitlines() if re.search(r"Error:|states generated|Finished in|Model checking completed", line)]
        summary["checks"].append(record)
        (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(f"{name}: {'EXPECTED' if record['matched'] else 'UNEXPECTED'} {record['seconds']}s "
              f"states={record.get('distinct', '?')} expected={expected}", flush=True)
        if not record["matched"]:
            print("\n".join(record["summary"]) or text[-3000:], flush=True)
        shutil.rmtree(scratch)
    unchanged = all(hashlib.sha256((inputs/name).read_bytes()).hexdigest() == digest for name, digest in hashes.items())
    summary["inputs_unchanged"] = unchanged
    summary["implementation_unchanged"] = java_evidence() == summary["implementation"]
    (run / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if unchanged and all(x["matched"] for x in summary["checks"]) else 1


if __name__ == "__main__":
    sys.exit(main())
