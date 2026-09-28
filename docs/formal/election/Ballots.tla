----------------------------- MODULE Ballots -----------------------------
EXTENDS Integers, FiniteSets

\* Competing candidates, two terms, one retained-file process crash at member
\* 0 (members are otherwise symmetric). Logs are equally eligible: this model
\* isolates durable voting; Consecutive checks changing log tuples and entries.
CONSTANT PersistBallots
Nodes == 0..2
Terms == 1..2
Max(a,b) == IF a > b THEN a ELSE b
VARIABLES phase, promise, disk, accepted, metadata, requests, votes, winners, crash
vars == <<phase,promise,disk,accepted,metadata,requests,votes,winners,crash>>

Init ==
    /\ phase = [n \in Nodes |-> "C"]
    /\ promise = [n \in Nodes |-> 0] /\ disk = promise
    /\ accepted = promise /\ metadata = promise
    /\ requests = {} /\ votes = {} /\ winners = {} /\ crash = "none"

Nominate(n) ==
    /\ phase[n] = "C" /\ promise[n] < 2
    /\ LET t == Max(disk[n],promise[n]+1) IN
        /\ phase' = [phase EXCEPT ![n] = "B"]
        /\ promise' = [promise EXCEPT ![n] = t]
        /\ disk' = [disk EXCEPT ![n] = IF PersistBallots THEN t ELSE @]
        /\ requests' = requests \cup {<<n,t>>}
        /\ votes' = votes \cup {<<n,n,t>>}
    /\ UNCHANGED <<accepted,metadata,winners,crash>>

\* Request snapshots remain deliverable across timeout and crash. Positive
\* replies are retained, so they may contribute to a later decision only when
\* its candidate/term still matches. Omitting delivered negative replies is a
\* safety overapproximation: a NO can only prevent a winning quorum in Java.
Request(v,c,t) ==
    /\ <<c,t>> \in requests /\ v # c
    /\ IF phase[v] = "L" /\ t > accepted[v]
       THEN /\ phase' = [phase EXCEPT ![v] = "C"]
            /\ UNCHANGED <<promise,disk,votes>>
       ELSE /\ phase[v] \in {"C","B","V"} /\ t > promise[v]
            /\ phase' = [phase EXCEPT ![v] = "V"]
            /\ promise' = [promise EXCEPT ![v] = t]
            /\ disk' = [disk EXCEPT ![v] = IF PersistBallots THEN t ELSE @]
            /\ votes' = votes \cup {<<v,c,t>>}
    /\ UNCHANGED <<accepted,metadata,requests,winners,crash>>

Win(n) ==
    /\ phase[n] = "B"
    /\ Cardinality({v \in Nodes : <<v,n,promise[n]>> \in votes}) >= 2
    /\ phase' = [phase EXCEPT ![n] = "L"]
    /\ accepted' = [accepted EXCEPT ![n] = promise[n]]
    /\ winners' = winners \cup {<<n,promise[n]>>}
    /\ UNCHANGED <<promise,disk,metadata,requests,votes,crash>>

Timeout(n) ==
    /\ phase[n] # "C"
    /\ phase' = [phase EXCEPT ![n] = "C"]
    /\ UNCHANGED <<promise,disk,accepted,metadata,requests,votes,winners,crash>>

Hear(n,c,t) ==
    /\ <<c,t>> \in winners /\ n # c /\ t >= promise[n]
    /\ phase[n] = "C" \/ (phase[n] \in {"B","V"} /\ promise[n] = t)
    /\ phase' = [phase EXCEPT ![n] = "F"]
    /\ promise' = [promise EXCEPT ![n] = Max(@,t)]
    /\ accepted' = [accepted EXCEPT ![n] = t]
    /\ UNCHANGED <<disk,metadata,requests,votes,winners,crash>>

Metadata(n) ==
    /\ phase[n] \in {"F","L"} /\ metadata[n] < accepted[n]
    /\ metadata' = [metadata EXCEPT ![n] = accepted[n]]
    /\ UNCHANGED <<phase,promise,disk,accepted,requests,votes,winners,crash>>

Crash ==
    /\ crash = "none"
    /\ crash' = IF accepted[0] > Max(disk[0],metadata[0]) THEN "accepted"
                 ELSE IF disk[0] > 0 THEN "vote" ELSE "other"
    /\ phase' = [phase EXCEPT ![0] = "C"]
    /\ promise' = [promise EXCEPT ![0] = Max(disk[0],metadata[0])]
    /\ accepted' = [accepted EXCEPT ![0] = metadata[0]]
    /\ UNCHANGED <<disk,metadata,requests,votes,winners>>

Next == Crash \/ (\E n \in Nodes : Nominate(n) \/ Win(n) \/ Timeout(n) \/ Metadata(n)
        \/ (\E c \in Nodes, t \in Terms : Request(n,c,t) \/ Hear(n,c,t)))
TypeOK ==
    /\ phase \in [Nodes -> {"C","B","V","F","L"}]
    /\ promise \in [Nodes -> 0..2] /\ disk \in [Nodes -> 0..2]
    /\ accepted \in [Nodes -> 0..2] /\ metadata \in [Nodes -> 0..2]
    /\ requests \subseteq Nodes \X Terms /\ votes \subseteq Nodes \X Nodes \X Terms
    /\ winners \subseteq Nodes \X Terms /\ crash \in {"none","vote","accepted","other"}
OneVotePerTerm == \A n \in Nodes, t \in Terms :
    Cardinality({c \in Nodes : <<n,c,t>> \in votes}) <= 1
OneLeaderPerTerm == \A t \in Terms : Cardinality({n \in Nodes : <<n,t>> \in winners}) <= 1
NoVolatileAcceptanceLoss == crash # "accepted"
NoReelectionAfterCrash == ~(crash = "vote" /\ \E a,b \in Nodes :
    a # b /\ <<a,1>> \in winners /\ <<b,2>> \in winners)
=============================================================================
