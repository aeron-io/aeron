------------------------------ MODULE Replay ------------------------------
EXTENDS Integers
CONSTANTS Mode, InitialApplied, InitialNotified, Members,
          ReportWhileWaiting, CatchupAcceptedTerm, StampSelf, ElectionTermFilter, WaitTimeout
ASSUME /\ Mode \in {"replay","catchup","lost"} /\ Members \in {1,3}
       /\ InitialApplied \in 0..1 /\ InitialNotified \in InitialApplied..2

Min(a,b) == IF a < b THEN a ELSE b
Max(a,b) == IF a > b THEN a ELSE b
Empty == [t |-> -1, p |-> -1]
Reports == [t : -1..1, p : -1..4]
\* Term 1 is accepted, term 0 has been replayed. Position 2 is the old tail,
\* 3 is the new term event, 4 a distinct fresh client write. One or two live
\* members; in the three-member case the third member stays unavailable.
VARIABLES leaderPosition, leaderStage, leaderAgent, selfTerm, committed,
          recorded, applied, notified, followerAgent, followerPhase,
          reportWire, seen, commitWire, confirmed, expired, replayStop
vars == <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,
          recorded,applied,notified,followerAgent,followerPhase,
          reportWire,seen,commitWire,confirmed,expired,replayStop>>

Init ==
    /\ leaderPosition = IF Mode = "catchup" THEN 3 ELSE 2
    /\ leaderStage = IF Mode = "catchup" THEN "closed" ELSE "replication"
    /\ leaderAgent = IF Mode = "catchup" THEN 1 ELSE 0
    /\ selfTerm = leaderAgent
    /\ committed = IF Mode = "catchup" THEN 1 ELSE InitialApplied
    /\ recorded = IF Mode = "catchup" THEN 1 ELSE 2
    /\ applied = IF Mode = "catchup" THEN 1 ELSE InitialApplied
    /\ notified = IF Mode = "catchup" THEN 1 ELSE InitialNotified
    /\ followerAgent = 0
    /\ followerPhase = IF Mode = "catchup" THEN "catchup" ELSE "replay"
    /\ reportWire = Empty
    /\ seen = IF Mode = "catchup" THEN [t |-> 1,p |-> 1] ELSE [t |-> 0,p |-> 2]
    /\ commitWire = -1
    /\ confirmed = IF Mode = "catchup" THEN 1 ELSE 0
    /\ expired = FALSE /\ replayStop = -1

FilterTerm == IF ElectionTermFilter THEN 1 ELSE leaderAgent
Quorum == IF selfTerm # FilterTerm THEN 0
          ELSE IF Members = 1 THEN leaderPosition
          ELSE IF seen.t = FilterTerm THEN Min(leaderPosition,Max(0,seen.p)) ELSE 0

RefreshSelf ==
    /\ Mode # "lost"
    /\ selfTerm' = IF StampSelf THEN 1 ELSE selfTerm
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,committed,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

SendReport ==
    /\ Members = 3 /\ reportWire = Empty
    /\ \/ followerPhase \in {"catchup","follow"}
       \/ (followerPhase = "replay" /\ applied < recorded
           /\ IF ReportWhileWaiting THEN applied >= notified ELSE notified = 0)
    /\ reportWire' = [t |-> IF followerPhase = "catchup" /\ ~CatchupAcceptedTerm
                           THEN followerAgent ELSE 1, p |-> recorded]
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,seen,commitWire,confirmed,expired,replayStop>>

ReceiveReport ==
    /\ Mode # "lost" /\ reportWire # Empty
    /\ seen' = reportWire /\ reportWire' = Empty
    /\ confirmed' = IF reportWire.t = 1 THEN Max(confirmed,reportWire.p) ELSE confirmed
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,commitWire,expired,replayStop>>

LeaderReplayJoin ==
    /\ Mode # "lost" /\ leaderStage = "replication" /\ Quorum >= 2
    /\ leaderStage' = "ready" /\ leaderAgent' = 1
    /\ UNCHANGED <<leaderPosition,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

CloseElection ==
    /\ leaderStage = "ready" /\ committed >= 2 /\ selfTerm = 1
    /\ Members = 1 \/ (seen.t = 1 /\ seen.p >= 2)
    /\ leaderStage' = "closed" /\ leaderPosition' = 3
    /\ UNCHANGED <<leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

Write ==
    /\ leaderStage = "closed" /\ leaderPosition = 3
    /\ leaderPosition' = 4
    /\ UNCHANGED <<leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

PublishCommit ==
    /\ Mode # "lost" /\ commitWire = -1
    /\ commitWire' = Quorum
    /\ committed' = Max(committed,Quorum)
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,confirmed,expired,replayStop>>

ReceiveCommit ==
    /\ commitWire # -1 /\ notified' = Max(notified,commitWire) /\ commitWire' = -1
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,
                   followerAgent,followerPhase,reportWire,seen,confirmed,expired,replayStop>>

\* Incomplete replay really returns to CANVASS, preserving notified. Accepting
\* another announcement neither clears notified nor implicitly sends a report.
StartReplay ==
    /\ followerPhase = "replay" /\ applied < Min(recorded,notified)
    /\ replayStop' = Min(recorded,notified) /\ followerPhase' = "replaying"
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired>>

CompleteReplay ==
    /\ followerPhase = "replaying"
    /\ applied' = replayStop /\ replayStop' = -1
    /\ followerPhase' = IF applied' < recorded THEN "canvass" ELSE "catchup"
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,notified,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired>>

NothingToReplay ==
    /\ followerPhase = "replay" /\ applied = recorded /\ followerPhase' = "catchup"
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired,replayStop>>

RejectReplay ==
    /\ ~ReportWhileWaiting /\ followerPhase = "replay"
    /\ applied < recorded /\ notified > 0 /\ applied >= notified
    /\ followerPhase' = "canvass"
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired,replayStop>>

AcceptAgain ==
    /\ Mode # "lost" /\ followerPhase = "canvass" /\ followerPhase' = "replay"
    /\ notified' = Max(notified,Quorum)
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired,replayStop>>

Record ==
    /\ Mode # "lost" /\ followerPhase \in {"catchup","follow"}
    /\ recorded < leaderPosition /\ recorded' = recorded+1
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

Apply ==
    /\ followerPhase \in {"catchup","follow"} /\ applied < Min(recorded,notified)
    /\ applied' = applied+1
    /\ followerAgent' = IF applied' >= 3 THEN 1 ELSE followerAgent
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,notified,
                   followerPhase,reportWire,seen,commitWire,confirmed,expired,replayStop>>

FinishCatchup ==
    /\ followerPhase = "catchup" /\ followerAgent = 1 /\ applied = leaderPosition
    /\ followerPhase' = "follow"
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired,replayStop>>

Expire ==
    /\ Mode = "lost" /\ expired' = TRUE
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,notified,
                   followerAgent,followerPhase,reportWire,seen,commitWire,confirmed,replayStop>>
RestartElection ==
    /\ Mode = "lost" /\ WaitTimeout /\ expired /\ followerPhase = "replay"
    /\ applied < recorded /\ applied >= notified
    /\ followerPhase' = "canvass" /\ notified' = 0
    /\ UNCHANGED <<leaderPosition,leaderStage,leaderAgent,selfTerm,committed,recorded,applied,
                   followerAgent,reportWire,seen,commitWire,confirmed,expired,replayStop>>

Next == RefreshSelf \/ SendReport \/ ReceiveReport \/ LeaderReplayJoin \/ CloseElection \/ Write
        \/ PublishCommit \/ ReceiveCommit \/ StartReplay \/ CompleteReplay \/ NothingToReplay \/ RejectReplay
        \/ AcceptAgain \/ Record \/ Apply \/ FinishCatchup \/ Expire \/ RestartElection
Fair == /\ WF_vars(RefreshSelf) /\ WF_vars(SendReport) /\ WF_vars(ReceiveReport)
        /\ WF_vars(LeaderReplayJoin) /\ WF_vars(CloseElection) /\ WF_vars(Write)
        /\ WF_vars(PublishCommit) /\ WF_vars(ReceiveCommit) /\ WF_vars(StartReplay) /\ WF_vars(CompleteReplay)
        /\ WF_vars(NothingToReplay) /\ WF_vars(RejectReplay) /\ WF_vars(AcceptAgain)
        /\ WF_vars(Record) /\ WF_vars(Apply) /\ WF_vars(FinishCatchup)
        /\ WF_vars(Expire) /\ WF_vars(RestartElection)
Spec == Init /\ [][Next]_vars /\ Fair
Recovered == committed = 4 /\ (Members = 1 \/ (applied = 4 /\ followerPhase = "follow"))
EventuallyRecovered == <>Recovered
NoFairRecovery == ~<>Recovered
EventuallyCanvass == <>(followerPhase = "canvass")
TypeOK ==
    /\ leaderPosition \in 2..4 /\ leaderStage \in {"replication","ready","closed"}
    /\ leaderAgent \in 0..1 /\ selfTerm \in 0..1 /\ followerAgent \in 0..1
    /\ <<committed,recorded,applied,notified,confirmed>> \in [1..5 -> 0..4]
    /\ followerPhase \in {"replay","replaying","catchup","follow","canvass"}
    /\ replayStop \in -1..2
    /\ reportWire \in Reports /\ seen \in Reports /\ commitWire \in -1..4 /\ expired \in BOOLEAN
\* INIT clears notified while retaining an already applied prefix.
PositionOrder == applied <= recorded /\ (followerPhase = "canvass" \/ applied <= notified)
                 /\ committed <= leaderPosition
CommitHasCurrentTermSupport == Members = 1 \/ committed <= Max(InitialApplied,confirmed)
=============================================================================
