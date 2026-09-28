--------------------------- MODULE Nomination ---------------------------
EXTENDS Integers, FiniteSets
CONSTANTS Members, LogTermStamp
ASSUME Members \in {3,5}
VARIABLES ownLogTerm, acceptedTerm, ownPosition, peer
vars == <<ownLogTerm, acceptedTerm, ownPosition, peer>>
Peers == 1..(Members-1)
Reports == {[t |-> -1, p |-> -1]} \cup [t : 0..2, p : 0..2]
Init == /\ ownLogTerm \in 0..1 /\ acceptedTerm \in ownLogTerm..2
        /\ ownPosition \in 0..2 /\ peer \in [Peers -> Reports]
Next == UNCHANGED vars
SelfStamp == IF LogTermStamp THEN ownLogTerm ELSE acceptedTerm
NotAhead(t,p,u,q) == t < u \/ (t = u /\ p <= q)
Predicted == {n \in Peers : peer[n].p # -1 /\ NotAhead(peer[n].t,peer[n].p,SelfStamp,ownPosition)}
\* Independently stated request-vote comparator, using the actual log metadata.
WouldReject == {n \in Peers : peer[n].p = -1 \/ peer[n].t > ownLogTerm
                \/ (peer[n].t = ownLogTerm /\ peer[n].p > ownPosition)}
HonestAssessment == Predicted \cap WouldReject = {}
QuorumAssessment == Cardinality(Predicted)+1 >= (Members \div 2)+1 =>
                    Cardinality(Peers \ WouldReject)+1 >= (Members \div 2)+1
UnanimousAssessment == Predicted = Peers => WouldReject = {}
=============================================================================
