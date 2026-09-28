----------------------------- MODULE Handoff -----------------------------
EXTENDS Integers, Sequences, FiniteSets

\* A cut just after 1 wins a ballot with 2; 0 still leads the previous term.
\* The common, already committed prefix is omitted. Entries have identities,
\* not just positions. Base old entries were present at the winning candidate.
CONSTANTS GuardCandidateTerm, FilterReports, Base, OldLimit
ASSUME /\ Base \in 0..OldLimit /\ OldLimit \in 1..2
       /\ GuardCandidateTerm \in BOOLEAN /\ FilterReports \in BOOLEAN

Nodes == 0..2
OldEntries == <<"old-a", "old-b">>
OldPrefix(k) == SubSeq(OldEntries, 1, k)
Values(s) == {s[i] : i \in 1..Len(s)}
Min(a,b) == IF a < b THEN a ELSE b
Max(a,b) == IF a > b THEN a ELSE b
Term(s) == IF "term-1" \in Values(s) THEN 1 ELSE 0
ClientEntries == {"old-a", "old-b", "new-write"}
Phases == {"oldLeader", "elect", "newLeader", "ballot", "canvass", "oldFollow", "newFollow"}

VARIABLES log, phase, promised, published, oldSeen, newSeen, acked, lost, rejected
vars == <<log, phase, promised, published, oldSeen, newSeen, acked, lost, rejected>>

\* A history maximum for each source and report term represents any delayed
\* report at or below that position. Delivery never consumes it: duplication
\* and arbitrarily late delivery are overapproximated, including after restart.
\* This is NOT a latest-message-only channel or a pending-message constraint.
Init ==
    /\ \E o \in Base..OldLimit, v \in 0..Base :
        /\ log = <<OldPrefix(o), OldPrefix(Base), OldPrefix(v)>>
        /\ acked = Values(OldPrefix(v))
        /\ oldSeen = v
        /\ newSeen = [n \in {0,2} |-> [t |-> 0, p |-> IF n = 2 THEN v ELSE 0]]
        /\ published = [n \in Nodes |-> [t \in 0..1 |->
            IF t = 0 /\ n = 2 THEN v ELSE -1]]
    /\ phase = <<"oldLeader", "elect", "ballot">>
    /\ promised = <<0,1,1>>
    /\ lost = FALSE /\ rejected = FALSE

\* Arrays log/phase/promised use n+1 because TLA sequences start at 1.
OldAppend ==
    /\ phase[1] = "oldLeader" /\ Len(log[1]) < OldLimit
    /\ log' = [log EXCEPT ![1] = OldPrefix(Len(@)+1)]
    /\ UNCHANGED <<phase, promised, published, oldSeen, newSeen, acked, lost, rejected>>

Restart(n) ==
    /\ n \in {0,2}
    /\ phase[n+1] \in {"oldLeader", "ballot", "oldFollow", "newFollow"}
    /\ phase' = [phase EXCEPT ![n+1] = "canvass"]
    /\ UNCHANGED <<log, promised, published, oldSeen, newSeen, acked, lost, rejected>>

HearOld ==
    /\ phase[3] = "canvass" /\ Term(log[3]) = 0
    /\ IF GuardCandidateTerm /\ promised[3] > 0
       THEN /\ rejected' = TRUE /\ UNCHANGED phase
       ELSE /\ phase' = [phase EXCEPT ![3] = "oldFollow"] /\ UNCHANGED rejected
    /\ UNCHANGED <<log, promised, published, oldSeen, newSeen, acked, lost>>

RecordOld ==
    /\ phase[1] = "oldLeader" /\ phase[3] = "oldFollow"
    /\ Len(log[3]) < Len(log[1])
    /\ log' = [log EXCEPT ![3] = OldPrefix(Len(@)+1)]
    /\ UNCHANGED <<phase, promised, published, oldSeen, newSeen, acked, lost, rejected>>

\* NewLeadershipTerm first truncates a conflicting old tail and re-enters
\* CANVASS. Acceptance and archive replication are separate subsequent steps.
HearNew(n) ==
    /\ n \in {0,2} /\ phase[n+1] \in {"canvass", "ballot"}
    /\ IF Term(log[n+1]) = 0 /\ Len(log[n+1]) > Base
       THEN /\ log' = [log EXCEPT ![n+1] = OldPrefix(Base)]
            /\ lost' = (lost \/ ((Values(log[n+1]) \cap acked) \ Values(OldPrefix(Base)) # {}))
            /\ phase' = [phase EXCEPT ![n+1] = "canvass"]
            /\ UNCHANGED promised
       ELSE /\ phase' = [phase EXCEPT ![n+1] = "newFollow"]
            /\ promised' = [promised EXCEPT ![n+1] = 1]
            /\ UNCHANGED <<log, lost>>
    /\ UNCHANGED <<published, oldSeen, newSeen, acked, rejected>>

RecordNew(n) ==
    /\ n \in {0,2} /\ phase[n+1] = "newFollow"
    /\ Len(log[n+1]) < Len(log[2])
    /\ log' = [log EXCEPT ![n+1] = SubSeq(log[2],1,Len(@)+1)]
    /\ UNCHANGED <<phase, promised, published, oldSeen, newSeen, acked, lost, rejected>>

Publish(n) ==
    /\ n \in {0,2} /\ phase[n+1] \in {"canvass", "oldFollow", "newFollow"}
    /\ LET t == IF phase[n+1] = "newFollow" THEN 1 ELSE Term(log[n+1]) IN
        published' = [published EXCEPT ![n][t] = Max(@,Len(log[n+1]))]
    /\ UNCHANGED <<log, phase, promised, oldSeen, newSeen, acked, lost, rejected>>

DeliverOld ==
    /\ phase[1] = "oldLeader"
    /\ \E p \in 0..published[2][0] : oldSeen' = p
    /\ UNCHANGED <<log, phase, promised, published, newSeen, acked, lost, rejected>>

DeliverNew(n) ==
    /\ n \in {0,2}
    /\ \E t \in 0..1 : \E p \in 0..published[n][t] :
        newSeen' = [newSeen EXCEPT ![n] = [t |-> t, p |-> p]]
    /\ UNCHANGED <<log, phase, promised, published, oldSeen, acked, lost, rejected>>

\* Two-of-three ranking, bounded by this leader's own recording. The leader's
\* current-term self stamp is covered independently by Replay.tla.
PeerPos(n) == IF FilterReports /\ newSeen[n].t # 1 THEN 0 ELSE newSeen[n].p
NewQuorum == Min(Len(log[2]), Max(PeerPos(0),PeerPos(2)))
OldQuorum == Min(Len(log[1]), Max(Base,oldSeen))

CloseNewElection ==
    /\ phase[2] = "elect"
    /\ \E n \in {0,2} : newSeen[n].t = 1 /\ newSeen[n].p >= Base
    /\ log' = [log EXCEPT ![2] = Append(@,"term-1")]
    /\ phase' = [phase EXCEPT ![2] = "newLeader"]
    /\ UNCHANGED <<promised, published, oldSeen, newSeen, acked, lost, rejected>>

NewWrite ==
    /\ phase[2] = "newLeader" /\ Len(log[2]) = Base+1
    /\ log' = [log EXCEPT ![2] = Append(@,"new-write")]
    /\ UNCHANGED <<phase, promised, published, oldSeen, newSeen, acked, lost, rejected>>

AckOld ==
    /\ phase[1] = "oldLeader"
    /\ acked' = acked \cup (Values(SubSeq(log[1],1,OldQuorum)) \cap ClientEntries)
    /\ UNCHANGED <<log, phase, promised, published, oldSeen, newSeen, lost, rejected>>
AckNew ==
    /\ phase[2] = "newLeader"
    /\ acked' = acked \cup (Values(SubSeq(log[2],1,NewQuorum)) \cap ClientEntries)
    /\ UNCHANGED <<log, phase, promised, published, oldSeen, newSeen, lost, rejected>>

Next == OldAppend \/ HearOld \/ RecordOld \/ DeliverOld \/ CloseNewElection \/ NewWrite \/ AckOld \/ AckNew
        \/ (\E n \in {0,2} : Restart(n) \/ HearNew(n) \/ RecordNew(n) \/ Publish(n) \/ DeliverNew(n))

TypeOK ==
    /\ phase \in [1..3 -> Phases] /\ promised \in [1..3 -> 0..1]
    /\ \A i \in 1..3 : log[i] \in {OldPrefix(k) : k \in 0..OldLimit}
        \cup {Append(OldPrefix(Base),"term-1"), Append(Append(OldPrefix(Base),"term-1"),"new-write")}
    /\ acked \subseteq ClientEntries /\ lost \in BOOLEAN /\ rejected \in BOOLEAN
    /\ published \in [Nodes -> [0..1 -> -1..(Base+2) \cup 0..OldLimit]]
    /\ oldSeen \in 0..OldLimit
    /\ newSeen \in [{0,2} -> [t : 0..1, p : 0..(Base+2) \cup 0..OldLimit]]

AcknowledgedOnQuorum == \A x \in acked : Cardinality({i \in 1..3 : x \in Values(log[i])}) >= 2
NoAcknowledgedTruncation == ~lost
\* Deliberately negated reachability checks, not safety requirements.
NoNewAcknowledgement == "new-write" \notin acked
NoStaleRejection == ~rejected
=============================================================================
