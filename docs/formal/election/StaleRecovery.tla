-------------------------- MODULE StaleRecovery --------------------------
EXTENDS Integers
CONSTANTS Companion, UnfinishedStepsDown, InitiallyClosed
\* Winner of term 1 is dead. Only the term-0 incumbent and its former voter
\* survive. The voter has persisted candidateTermId=1, but log/accepted term=0.
\* Healthy delivery suffix; no known quorum at the voter; no clock integers.
VARIABLES incumbent, voter, stale, deadline, canvass, announcement, request, sent, opened
vars == <<incumbent,voter,stale,deadline,canvass,announcement,request,sent,opened>>
Init == /\ incumbent = IF InitiallyClosed THEN "closed" ELSE "unfinished"
        /\ voter = "canvass" /\ stale = FALSE /\ deadline = FALSE
        /\ canvass = FALSE /\ announcement = FALSE /\ request = FALSE
        /\ sent = FALSE /\ opened = FALSE
Tick == /\ deadline' = TRUE /\ UNCHANGED <<incumbent,voter,stale,canvass,announcement,request,sent,opened>>
SendCanvass == /\ voter = "canvass" /\ canvass' = TRUE
              /\ UNCHANGED <<incumbent,voter,stale,deadline,announcement,request,sent,opened>>
Answer == /\ canvass /\ incumbent # "canvass" /\ canvass' = FALSE /\ announcement' = TRUE
          /\ UNCHANGED <<incumbent,voter,stale,deadline,request,sent,opened>>
RejectOld == /\ announcement /\ announcement' = FALSE /\ stale' = TRUE
             /\ UNCHANGED <<incumbent,voter,deadline,canvass,request,sent,opened>>
Nominate == /\ voter = "canvass" /\ ~sent /\ Companion /\ stale /\ deadline
            /\ voter' = "ballot"
            /\ UNCHANGED <<incumbent,stale,deadline,canvass,announcement,request,sent,opened>>
SendRequest == /\ voter = "ballot" /\ ~sent /\ sent' = TRUE /\ request' = TRUE
               /\ UNCHANGED <<incumbent,voter,stale,deadline,canvass,announcement,opened>>
\* A ballot timeout may precede delivery; the one-shot term-2 request remains.
Timeout == /\ voter = "ballot" /\ sent /\ voter' = "canvass" /\ stale' = FALSE
           /\ UNCHANGED <<incumbent,deadline,canvass,announcement,request,sent,opened>>
DeliverRequest ==
    /\ request /\ request' = FALSE
    /\ IF incumbent = "closed" \/ UnfinishedStepsDown
       THEN /\ incumbent' = "canvass" /\ opened' = TRUE
       ELSE UNCHANGED <<incumbent,opened>>
    /\ UNCHANGED <<voter,stale,deadline,canvass,announcement,sent>>
Next == Tick \/ SendCanvass \/ Answer \/ RejectOld \/ Nominate \/ SendRequest \/ Timeout \/ DeliverRequest
Spec == Init /\ [][Next]_vars
        /\ WF_vars(Tick) /\ WF_vars(SendCanvass) /\ WF_vars(Answer)
        /\ WF_vars(RejectOld) /\ WF_vars(Nominate) /\ WF_vars(SendRequest) /\ WF_vars(DeliverRequest)
EventuallyOpened == <>opened
NoFairOpening == ~<>opened
TypeOK == /\ incumbent \in {"closed","unfinished","canvass"} /\ voter \in {"canvass","ballot"}
          /\ <<stale,deadline,canvass,announcement,request,sent,opened>> \in [1..7 -> BOOLEAN]
=============================================================================
