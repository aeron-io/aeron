--------------------------- MODULE Consecutive ---------------------------
EXTENDS Integers, Sequences, FiniteSets

\* Two consecutive ballots, rotating candidates 1 then 2. This is a bounded
\* fault-schedule model, NOT a refinement of every Election state. See README.
\* No winner or safe winning prefix is assumed. Requests retain the log tuple
\* at nomination; deliveries, timeouts, metadata, recording and replay race.
CONSTANTS CrashNode, GuardCandidateTerm, FilterReports, InitialOldTail, StampCatchupMetadata
Nodes == 0..2
Terms == 1..2
Max(a,b) == IF a > b THEN a ELSE b
Values(s) == {s[k] : k \in 1..Len(s)}
Prefix(a,b) == Len(a) <= Len(b) /\ a = SubSeq(b,1,Len(a))
AtLeast(a,b) == a.meta > b.meta \/ (a.meta = b.meta /\ Len(a.log) >= Len(b.log))
Event(t) == IF t = 1 THEN "e1" ELSE "e2"
ReplayTerm(s) == IF "e2" \in Values(s) THEN 2 ELSE IF "e1" \in Values(s) THEN 1 ELSE 0
Phases == {"C", "B", "V", "A", "K", "J", "F", "L"}

VARIABLES r, disk, nominated, request, yes, no, won, base, prior,
          crashed, ack, badTruncate, held, rejected
vars == <<r, disk, nominated, request, yes, no, won, base, prior,
          crashed, ack, badTruncate, held, rejected>>

Init ==
    /\ r = [n \in Nodes |-> [phase |-> IF n = 0 THEN "L" ELSE "C",
           log |-> IF n = 0 THEN SubSeq(<<"old-a", "old-b">>,1,InitialOldTail) ELSE <<>>,
           meta |-> 0, promise |-> 0, accepted |-> 0]]
    /\ disk = [n \in Nodes |-> 0]
    /\ nominated = {} /\ won = {0}
    /\ request = [t \in Terms |-> [meta |-> 0, log |-> <<>>]]
    /\ yes = [t \in Terms |-> {}] /\ no = [t \in Terms |-> {}]
    /\ base = [t \in 0..2 |-> <<>>] /\ prior = [t \in 0..2 |-> 0]
    /\ crashed = "none" /\ ack = FALSE /\ badTruncate = FALSE
    /\ held = [from |-> -1, term |-> 0, pos |-> 0]
    /\ rejected = FALSE

\* Candidate identities are fixed to bound schedules, but eligibility and
\* durable voting are checked. The second ballot may start before the first
\* leader has completed replay, published a term event, or acknowledged x.
Nominate(t) ==
    /\ t \notin nominated /\ t-1 \in won /\ r[t].phase = "C"
    /\ r[t].promise < t
    /\ r' = [r EXCEPT ![t].phase = "B", ![t].promise = t]
    /\ disk' = [disk EXCEPT ![t] = t]
    /\ nominated' = nominated \cup {t}
    /\ request' = [request EXCEPT ![t] = [meta |-> r[t].meta, log |-> r[t].log]]
    /\ yes' = [yes EXCEPT ![t] = {t}]
    /\ UNCHANGED <<no, won, base, prior, crashed, ack, badTruncate, held, rejected>>

\* A delayed request can step a leader down, then be retried. A negative vote
\* caused by an inferior log also persists the higher candidate term.
Vote(n,t) ==
    /\ t \in nominated /\ n # t /\ n \notin yes[t] \cup no[t]
    /\ IF r[n].phase = "L" /\ t > r[n].accepted
       THEN /\ r' = [r EXCEPT ![n].phase = "C"]
            /\ UNCHANGED <<disk, yes, no>>
       ELSE /\ LET fresh == t > r[n].promise
                   inferior == ~AtLeast(request[t], r[n])
                   grant == fresh /\ ~inferior /\ r[n].phase \in {"C", "B", "V"}
                   persist == fresh /\ (inferior \/ grant) IN
               /\ ~fresh \/ inferior \/ grant
               /\ r' = [r EXCEPT ![n].promise = IF persist THEN t ELSE @,
                         ![n].phase = IF grant THEN "V" ELSE @]
               /\ disk' = [disk EXCEPT ![n] = IF persist THEN t ELSE @]
               /\ yes' = [yes EXCEPT ![t] = IF grant /\ t \notin won THEN @ \cup {n} ELSE @]
               /\ no' = [no EXCEPT ![t] = IF grant \/ t \in won THEN @ ELSE @ \cup {n}]
    /\ UNCHANGED <<nominated, request, won, base, prior, crashed, ack, badTruncate, held, rejected>>

Win(t) ==
    /\ r[t].phase = "B" /\ r[t].promise = t
    /\ t \notin won /\ Cardinality(yes[t]) >= 2 /\ no[t] = {}
    /\ r' = [r EXCEPT ![t].phase = "L", ![t].accepted = t]
    /\ won' = won \cup {t}
    /\ base' = [base EXCEPT ![t] = r[t].log]
    /\ prior' = [prior EXCEPT ![t] = r[t].meta]
    /\ yes' = [yes EXCEPT ![t] = {}] /\ no' = [no EXCEPT ![t] = {}]
    /\ UNCHANGED <<disk, nominated, request, crashed, ack, badTruncate, held, rejected>>

Timeout(n) ==
    /\ r[n].phase # "C"
    /\ r' = [r EXCEPT ![n].phase = "C"]
    /\ UNCHANGED <<disk, nominated, request, yes, no, won, base, prior,
                   crashed, ack, badTruncate, held, rejected>>

\* Retained-file process crash/restart. Accepted terms are NOT durable votes.
\* Recovery starts from recording metadata and max(persisted ballot, metadata).
Crash ==
    /\ crashed = "none" /\ CrashNode \in Nodes
    /\ r' = [r EXCEPT ![CrashNode].phase = "C",
             ![CrashNode].accepted = r[CrashNode].meta,
             ![CrashNode].promise = Max(disk[CrashNode],r[CrashNode].meta)]
    /\ crashed' = IF r[CrashNode].accepted > Max(disk[CrashNode],r[CrashNode].meta)
                  THEN "accepted"
                  ELSE IF ack /\ 1 \in won /\ 2 \notin won THEN "between"
                  ELSE IF r[CrashNode].meta > ReplayTerm(r[CrashNode].log) THEN "metadata"
                  ELSE "other"
    /\ UNCHANGED <<disk, nominated, request, yes, no, won, base, prior,
                   ack, badTruncate, held, rejected>>

\* Announcements from any historical winner can be delayed indefinitely.
\* Truncation throws back to CANVASS before accepted/promise can change.
Hear(n,t) ==
    /\ t \in won /\ n # t
    /\ r[n].phase = "C" \/ (r[n].phase \in {"B","V"} /\ r[n].promise = t)
    /\ IF GuardCandidateTerm /\ t < r[n].promise
       THEN /\ UNCHANGED <<rejected,r,badTruncate>>
       ELSE /\ r[n].meta = prior[t] \/ (r[n].meta = t /\ r[t].meta >= t)
            /\ IF t > 0 /\ r[n].meta = prior[t] /\ Len(r[n].log) > Len(base[t])
               THEN /\ r' = [r EXCEPT ![n].log = SubSeq(@,1,Len(base[t])), ![n].phase = "C"]
                    /\ badTruncate' = (badTruncate \/ (ack /\ "x" \in Values(r[n].log)
                                                       /\ "x" \notin Values(base[t])))
               ELSE /\ r' = [r EXCEPT ![n].phase = IF Len(r[t].log) > Len(r[n].log) THEN "A" ELSE "J",
                                       ![n].accepted = t,
                                       ![n].promise = Max(@,t)]
                    /\ UNCHANGED badTruncate
            /\ rejected' = (rejected \/ t < r[n].promise)
    /\ UNCHANGED <<disk, nominated, request, yes, no, won, base, prior, crashed, ack, held>>

\* Archive catch-up to the chosen base is one atomic prefix-copy. Byte-wise
\* replication, replay deadlines, and multi-term metadata repair are outside
\* this cut. No conflicting prefix can silently be replaced here.
CopyBase(n) ==
    /\ r[n].phase \in {"A","J"}
    /\ LET t == r[n].accepted IN
        /\ Prefix(r[n].log,base[t]) /\ r[n].log # base[t]
        /\ r' = [r EXCEPT ![n].log = base[t]]
    /\ UNCHANGED <<disk, nominated, request, yes, no, won, base, prior,
                   crashed, ack, badTruncate, held, rejected>>

Metadata(n) ==
    /\ r[n].phase \in {"K", "J", "L"} /\ r[n].accepted \in won
    /\ r[n].phase = "K" => ReplayTerm(r[n].log) >= r[n].accepted
    /\ Prefix(base[r[n].accepted],r[n].log)
    /\ r[n].meta < r[n].accepted \/ r[n].phase \in {"K","J"}
    /\ r' = [r EXCEPT ![n].meta = r[n].accepted,
             ![n].phase = IF @ \in {"K","J"} THEN "F" ELSE @]
    /\ UNCHANGED <<disk, nominated, request, yes, no, won, base, prior,
                   crashed, ack, badTruncate, held, rejected>>

JoinCatchup(n) ==
    /\ r[n].phase = "A" /\ Prefix(base[r[n].accepted],r[n].log)
    /\ r' = [r EXCEPT ![n].phase = "K",
             ![n].meta = IF StampCatchupMetadata THEN r[n].accepted ELSE @]
    /\ UNCHANGED <<disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,held,rejected>>

ReportTerm(n) == IF r[n].phase \in {"A","K","J","F"} THEN r[n].accepted ELSE r[n].meta
SaveReport(n) ==
    /\ held.from = -1 /\ r[n].phase \in {"C","A","K","J","F"}
    /\ held' = [from |-> n, term |-> ReportTerm(n), pos |-> Len(r[n].log)]
    /\ UNCHANGED <<r,disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,rejected>>
Supports(n,t,p) ==
    (r[n].phase \in {"C","A","K","J","F"} /\ Len(r[n].log) >= p
     /\ (~FilterReports \/ ReportTerm(n) = t))
    \/ (held.from = n /\ held.pos >= p /\ (~FilterReports \/ held.term = t))

TermEvent(t) ==
    /\ t \in won /\ r[t].phase = "L" /\ r[t].meta = t
    /\ r[t].log = base[t]
    /\ \E n \in Nodes \ {t} : Supports(n,t,Len(base[t]))
    /\ r' = [r EXCEPT ![t].log = Append(@,Event(t))]
    /\ UNCHANGED <<disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,held,rejected>>
Write ==
    /\ 1 \in won /\ r[1].phase = "L"
    /\ r[1].log = Append(base[1],"e1")
    /\ r' = [r EXCEPT ![1].log = Append(@,"x")]
    /\ UNCHANGED <<disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,held,rejected>>
Record(n) ==
    /\ r[n].phase \in {"K","F"}
    /\ LET t == r[n].accepted IN
        /\ r[t].phase = "L" /\ r[t].accepted = t
        /\ Prefix(r[n].log,r[t].log) /\ r[n].log # r[t].log
        /\ r' = [r EXCEPT ![n].log = r[t].log]
    /\ UNCHANGED <<disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,held,rejected>>
Replay(n) ==
    /\ r[n].phase \in {"A","K","J","F","L"} /\ r[n].meta < ReplayTerm(r[n].log)
    /\ r' = [r EXCEPT ![n].meta = ReplayTerm(r[n].log)]
    /\ UNCHANGED <<disk,nominated,request,yes,no,won,base,prior,crashed,ack,badTruncate,held,rejected>>
Ack ==
    /\ ~ack /\ r[1].phase = "L" /\ "x" \in Values(r[1].log)
    /\ \E n \in Nodes \ {1} : Supports(n,1,Len(r[1].log))
    /\ ack' = TRUE
    /\ UNCHANGED <<r,disk,nominated,request,yes,no,won,base,prior,crashed,badTruncate,held,rejected>>

Next == Crash \/ Write \/ Ack
    \/ (\E t \in Terms : Nominate(t) \/ Win(t) \/ TermEvent(t))
    \/ (\E n \in Nodes : Timeout(n) \/ CopyBase(n) \/ Metadata(n) \/ JoinCatchup(n)
        \/ SaveReport(n) \/ Record(n) \/ Replay(n)
        \/ (\E t \in Terms : Vote(n,t)) \/ (\E t \in 0..2 : Hear(n,t)))

TypeOK ==
    /\ \A n \in Nodes : /\ r[n].phase \in Phases /\ r[n].meta \in 0..2
        /\ r[n].promise \in 0..2 /\ r[n].accepted \in 0..2
        /\ Len(r[n].log) <= 5 /\ Values(r[n].log) \subseteq {"old-a","old-b","e1","e2","x"}
    /\ disk \in [Nodes -> 0..2] /\ nominated \subseteq Terms /\ won \subseteq 0..2
    /\ crashed \in {"none","accepted","between","metadata","other"}
    /\ ack \in BOOLEAN /\ badTruncate \in BOOLEAN
AcknowledgedOnQuorum == ack => Cardinality({n \in Nodes : "x" \in Values(r[n].log)}) >= 2
NextLeaderContainsAck == (ack /\ 2 \in won) => "x" \in Values(r[2].log)
NoAcknowledgedTruncation == ~badTruncate
\* 'rejected' records a stale announcement incorrectly acted on, for mutation testing.
NoStaleAction == ~rejected
\* Deliberately false invariants supply concrete nonvacuity traces.
NoTwoHandoffs == ~(ack /\ 2 \in won /\ crashed = "between" /\ "e2" \in Values(r[2].log))
NoVolatileAcceptanceLoss == crashed # "accepted"
NoMetadataBeforeEventCrash == crashed # "metadata"
=============================================================================
