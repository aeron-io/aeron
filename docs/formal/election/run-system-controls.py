#!/usr/bin/env python3
"""Run real-cluster election regressions and single-fix source controls in an isolated worktree.

Requires the same JDK/build environment as ./gradlew. Never modifies the caller's Java sources.
Every control must fail its designated assertion; build errors and incidental timeouts are failures
of this runner, not successful reproductions. Evidence includes exact source, patch, logs, and XML.
"""

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[3]
MAIN = Path("aeron-cluster/src/main/java/io/aeron/cluster")
TEST = Path("aeron-system-tests/src/test/java/io/aeron/cluster")
ELECTION = MAIN / "Election.java"
AGENT = MAIN / "ConsensusModuleAgent.java"
SOURCES = [ELECTION, AGENT, MAIN / "ClusterMember.java", TEST / "ElectionTermProgressTest.java",
    TEST / "StalePositionQuorumTest.java", TEST / "ElectionReplayProgressTest.java"]
CASES = {
    "early-self-term": ("shouldCountRecordedQuorumBeforeJoiningLeaderLog", "EARLY_SELF_TERM", 2),
    "replication-quorum-term": ("shouldCountRecordedQuorumBeforeJoiningLeaderLog", "ELECTION_QUORUM_TERM", 2),
    "replay-quorum-term": ("shouldCountRecordedQuorumBeforeJoiningLeaderLog", "ELECTION_QUORUM_TERM", 2),
    "canvass-quorum-term": ("shouldUseElectionQuorumWhenAnsweringOldCanvass", "ELECTION_QUORUM_TERM", 1),
    "catchup-report-term": ("shouldReportAcceptedTermDuringCatchupBeforeReplayingTermEvent", "CATCHUP_REPORT_TERM", 1),
    "unfinished-leader": ("shouldRestartUnfinishedLeaderOnRealNewerBallot", "UNFINISHED_LEADER_BALLOT", 4),
    "partial-replay-report": ("shouldReportRecordedTailAfterRealPartialReplay", "PARTIAL_REPLAY_REPORT", 1),
    "replay-commit-timeout": ("shouldLeaveReplayWaitWhenLeaderDisappears", "REPLAY_COMMIT_TIMEOUT", 1),
    "nomination-log-term": ("shouldAssessNominationUsingRecordedTermAfterUnreplayedAcceptance", "NOMINATION_LOG_TERM", 1),
}


def replace_once(text, before, after):
    if text.count(before) != 1:
        raise ValueError(f"expected exactly one mutation target, found {text.count(before)}: {before!r}")
    return text.replace(before, after, 1)


def mutate(case, sources):
    result = dict(sources)
    if case == "early-self-term":
        result[ELECTION] = replace_once(result[ELECTION],
            """        thisMember
            .leadershipTermId(leadershipTermId)
            .logPosition(appendPosition)
            .timeOfLastAppendPositionNs(nowNs);

        final long quorumPosition =""",
            """        thisMember.logPosition(appendPosition).timeOfLastAppendPositionNs(nowNs);

        final long quorumPosition =""")
    elif case in ("replication-quorum-term", "replay-quorum-term", "canvass-quorum-term"):
        method = {"replication-quorum-term": "    private int leaderLogReplication(",
            "replay-quorum-term": "    private int leaderReplay(",
            "canvass-quorum-term": "    void onCanvassPosition("}[case]
        start = result[ELECTION].index(method)
        end = result[ELECTION].index("\n    " + ("void " if case == "canvass-quorum-term" else "private "), start + 1)
        before = ("quorumPositionBoundedByLeaderLog(\n                            this.leadershipTermId, appendPosition, nowNs)"
            if case == "canvass-quorum-term" else
            "quorumPositionBoundedByLeaderLog(leadershipTermId, appendPosition, nowNs)")
        body = replace_once(result[ELECTION][start:end],
            before,
            "quorumPositionUsingAgentTerm(appendPosition, nowNs)")
        result[ELECTION] = result[ELECTION][:start] + body + result[ELECTION][end:]
        # Restore the old implicit-agent-term API only for the selected call site. The other fixes stay intact.
        signature = "    long quorumPositionBoundedByLeaderLog(\n"
        result[AGENT] = replace_once(result[AGENT], signature,
            """    long quorumPositionUsingAgentTerm(final long leaderAppendPosition, final long nowNs)
    {
        return quorumPositionBoundedByLeaderLog(leadershipTermId, leaderAppendPosition, nowNs);
    }

""" + signature)
    elif case == "catchup-report-term":
        result[AGENT] = replace_once(result[AGENT],
            "                election.leadershipTermId(),\n                currentAppendPosition,",
            "                leadershipTermId,\n                currentAppendPosition,")
    elif case == "unfinished-leader":
        result[ELECTION] = replace_once(result[ELECTION], """        if (candidateTermId > leadershipTermId && Cluster.Role.LEADER == consensusModuleAgent.role())
        {
            throw new ClusterEvent("unexpected vote request:" +
                " this.leadershipTermId=" + leadershipTermId + " candidateTermId=" + candidateTermId);
        }

""", "")
    elif case == "partial-replay-report":
        # Preserve the separately tested timeout while restoring the old nonzero-notification rejection.
        result[ELECTION] = replace_once(result[ELECTION],
            """                    return publishFollowerAppendPosition(nowNs);
                }
                else
                {
                    logReplay =""",
            """                    if (0 != notifiedCommitPosition)
                    {
                        state(CANVASS, nowNs, "log replay rejected: retained nonzero commit position");
                        return 0;
                    }
                    return publishFollowerAppendPosition(nowNs);
                }
                else
                {
                    logReplay =""")
    elif case == "replay-commit-timeout":
        result[ELECTION] = replace_once(result[ELECTION], """                    if (nowNs >= (timeOfLastStateChangeNs + ctx.leaderHeartbeatTimeoutNs()))
                    {
                        throw new TimeoutException(
                            "timeout awaiting commit position during replay", AeronException.Category.WARN);
                    }
""", "")
    elif case == "nomination-log-term":
        result[ELECTION] = replace_once(result[ELECTION],
            "thisMember.leadershipTermId(logLeadershipTermId).logPosition(appendPosition);",
            "thisMember.leadershipTermId(leadershipTermId).logPosition(appendPosition);")
    else:
        raise ValueError(case)
    return result


def command(args, cwd=ROOT, **kwargs):
    return subprocess.run(args, cwd=cwd, check=True, **kwargs)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def run(worktree, destination, sources, method, expected_marker, expected_count, timeout):
    destination.mkdir(parents=True)
    for path, content in sources.items():
        target = worktree / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)
        snapshot = destination / "sources" / path
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_text(content)
    with (destination / "source.patch").open("w") as output:
        command(["git", "diff", "--", str(MAIN)], cwd=worktree, stdout=output)
    results = worktree / "aeron-system-tests/build/test-results/slowTest"
    if results.exists():
        shutil.rmtree(results)
    diagnostics = worktree / "aeron-system-tests/build/test-output"
    if diagnostics.exists():
        shutil.rmtree(diagnostics)
    test_class = ("StalePositionQuorumTest" if method in ("shouldReportAcceptedTermDuringCatchupBeforeReplayingTermEvent",
        "shouldAssessNominationUsingRecordedTermAfterUnreplayedAcceptance") else
        "ElectionReplayProgressTest" if method in ("shouldReportRecordedTailAfterRealPartialReplay",
            "shouldLeaveReplayWaitWhenLeaderDisappears") else "ElectionTermProgressTest")
    args = ["./gradlew", "--no-daemon", "--no-build-cache", ":aeron-system-tests:slowTest",
        "--tests", f"*{test_class}.{method}"]
    started = time.monotonic()
    with (destination / "gradle.log").open("w") as output:
        process = subprocess.Popen(args, cwd=worktree, stdout=output, stderr=subprocess.STDOUT,
            start_new_session=True)
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise RuntimeError(f"control exceeded {timeout}s: {destination}")
    tests = []
    if results.exists():
        shutil.copytree(results, destination / "test-results")
        for path in sorted(results.glob("TEST-*.xml")):
            for test in ET.parse(path).getroot().iter("testcase"):
                failures = [item.attrib.get("message", "") for item in test.findall("failure")]
                errors = [item.attrib.get("message", "") for item in test.findall("error")]
                tests.append(dict(name=test.attrib["name"], classname=test.attrib["classname"],
                    failures=failures, errors=errors, skipped=test.find("skipped") is not None,
                    suppressed_failure=any("Suppressed:" in (item.text or "") for item in test.findall("failure"))))
    # Keep compact event/thread diagnostics, not gigabytes of copied archive segments and driver buffers.
    if diagnostics.exists():
        for pattern in ("**/events.log", "**/thread_dump.txt"):
            for path in diagnostics.glob(pattern):
                target = destination / "diagnostics" / path.relative_to(diagnostics)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
    valid = len(tests) == expected_count and all(not t["errors"] and not t["skipped"] and
        not t["suppressed_failure"] and t["classname"] == f"io.aeron.cluster.{test_class}" for t in tests)
    log = (destination / "gradle.log").read_text()
    valid &= ":aeron-system-tests:slowTest UP-TO-DATE" not in log and ":aeron-system-tests:slowTest FROM-CACHE" not in log
    if expected_marker:
        valid &= returncode != 0 and all(t["failures"] and
            all(expected_marker in message for message in t["failures"]) for t in tests)
    else:
        valid &= returncode == 0 and all(not t["failures"] for t in tests)
    record = dict(command=args, returncode=returncode, seconds=round(time.monotonic() - started, 3),
        expected_marker=expected_marker, expected_count=expected_count, valid=valid, tests=tests,
        source_sha256={str(path): digest(value) for path, value in sources.items()})
    (destination / "result.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"{'PASS' if valid else 'FAIL'} {destination.name} ({record['seconds']}s)", flush=True)
    return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", choices=CASES, help="default: every control")
    parser.add_argument("--repeat", type=int, default=1, help="repeat both fixed and reverted runs")
    parser.add_argument("--timeout", type=int, default=240, help="maximum seconds per Gradle invocation")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be positive")
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    sources = {path: (ROOT / path).read_text() for path in SOURCES}
    cases = args.case or list(CASES)
    # Validate all mutation anchors before any long-running work.
    controls = {case: mutate(case, sources) for case in cases}
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = (args.output or Path(__file__).parent / "results" / f"system-controls-{stamp}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    shutil.copy2(__file__, output / "runner.py")
    commit = command(["git", "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    parent = Path(tempfile.mkdtemp(prefix="aeron-election-controls-"))
    worktree = parent / "worktree"
    summary = dict(commit=commit, cases=cases, repeat=args.repeat, results=[], complete=False,
        runner_sha256=digest(Path(__file__).read_text()),
        environment={key: os.environ.get(key) for key in ("JAVA_HOME", "BUILD_JAVA_HOME", "BUILD_JAVA_VERSION")})
    try:
        command(["git", "worktree", "add", "--detach", str(worktree), commit], stdout=subprocess.DEVNULL)
        for iteration in range(1, args.repeat + 1):
            for case in cases:
                method, marker, count = CASES[case]
                for variant, content, expected in (("fixed", sources, None),
                        ("reverted", controls[case], marker)):
                    name = f"{iteration}-{case}-{variant}"
                    record = run(worktree, output / name, content, method, expected, count, args.timeout)
                    summary["results"].append(dict(name=name, **record))
                    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
                    if not record["valid"]:
                        raise RuntimeError(f"unexpected test outcome; inspect {output / name / 'gradle.log'}")
        summary["complete"] = True
    except BaseException as error:
        summary["error"] = repr(error)
        raise
    finally:
        summary["caller_sources_unchanged"] = all((ROOT / path).read_text() == value
            for path, value in sources.items())
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        # This private worktree contains only snapshots and generated build/test data from this runner.
        if worktree.exists():
            command(["git", "worktree", "remove", "--force", str(worktree)])
        shutil.rmtree(parent)
        print(f"Evidence: {output}", flush=True)


if __name__ == "__main__":
    main()
