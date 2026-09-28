------------------------------ MODULE Quorum ------------------------------
EXTENDS Integers, Sequences, FiniteSets
CONSTANTS Members, MaxPosition, FilterReports
VARIABLES reports, leaderPosition
vars == <<reports,leaderPosition>>
Nodes == 1..Members
Threshold == (Members \div 2)+1
Init == /\ reports \in [Nodes -> [t : 0..1, p : 0..MaxPosition, active : BOOLEAN]]
        /\ leaderPosition \in 0..MaxPosition
Next == UNCHANGED vars
Min(a,b) == IF a < b THEN a ELSE b
Max(a,b) == IF a > b THEN a ELSE b

\* ClusterMember.quorumPosition: insert into a descending, zero-filled array.
RECURSIVE Insert(_, _), Rank(_, _)
Insert(p, ranked) == IF Len(ranked) = 0 THEN <<>> ELSE
    <<Max(p,Head(ranked))>> \o Insert(Min(p,Head(ranked)),Tail(ranked))
Rank(i, ranked) == IF i > Members THEN ranked ELSE
    Rank(i+1, IF reports[i].active /\ (~FilterReports \/ reports[i].t = 1)
              THEN Insert(reports[i].p,ranked) ELSE ranked)
Result == Min(leaderPosition,Rank(1,[i \in 1..Threshold |-> 0])[Threshold])
Support(p) == Cardinality({n \in Nodes : reports[n].active /\ reports[n].t = 1 /\ reports[n].p >= p})
\* Independent set-cardinality contract, including ties, unavailable members,
\* arbitrary stale positions, and the leader's local-recording bound.
Supported == \A p \in 1..MaxPosition : Result >= p => Support(p) >= Threshold
Maximal == \A p \in 1..leaderPosition : Support(p) >= Threshold => Result >= p
Bounded == Result <= leaderPosition
=============================================================================
