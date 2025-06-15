from collections import Counter
from dataclasses import dataclass, replace
import enum
from typing import Final, Self, Sequence, TypeAlias

# --------------------------------- MODULE raft ---------------------------------
# This is the formal specification for the Raft consensus algorithm.
#
# Copyright 2014 Diego Ongaro.
# This work is licensed under the Creative Commons Attribution-4.0
# International License https://creativecommons.org/licenses/by/4.0/

# EXTENDS Naturals, FiniteSets, Sequences, TLC

# The set of server IDs
Server: Final = [10, 20, 30, 40, 50]

# The set of requests that can go into the log
Value: Final = [100, 200, 300]

# Server states.
class ServerState(enum.Enum):
    Follower = enum.auto()
    Candidate = enum.auto()
    Leader = enum.auto()

# A reserved value.
Nil: Final = None
NilType: TypeAlias = type[None]

# Message types:
class Message(enum.Enum):
    RequestVoteRequest = enum.auto()
    RequestVoteResponse = enum.auto()
    AppendEntriesRequest = enum.auto()
    AppendEntriesResponse = enum.auto()

MessageId = int
ServerId = int
TermId = int
LogId = int
LogValue = int


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    value: LogValue


@dataclass(frozen=True, slots=True)
class ElectionEntry:
    eterm: TermId
    eleader: ServerId
    elog: LogId
    evotes: dict[ServerId, list[ServerId]]
    evoterLog: dict[ServerId, dict[ServerId, list[LogEntry]]]


@dataclass(frozen=True, slots=True)
class Server:
    """The following variables are all per server (functions with domain Server)."""
    # The server's term number.
    currentTerm: int = 1
    # The server's state (Follower, Candidate, or Leader).
    state: ServerState = ServerState.Follower
    # The candidate the server voted for in its current term, or
    # Nil if it hasn't voted for any.
    votedFor: ServerId | NilType = Nil

    # A Sequence of log entries. The index into this sequence is the index of the
    # log entry. Unfortunately, the Sequence module defines Head(s) as the entry
    # with index 1, so be careful not to use that!
    log: list[LogEntry] = []
    # The index of the latest entry in the log the state machine may apply.
    commitIndex: int = 0
    # logVars == <<log, commitIndex>>


@dataclass(frozen=True, slots=True)
class Candidate:
    """The following variables are used only on candidates"""
    # The set of servers from which the candidate has received a RequestVote
    # response in its currentTerm.
    votesResponded: list[ServerId] =[]
    # The set of servers from which the candidate has received a vote in its
    # currentTerm.
    votesGranted: list[ServerId] = []
    # A history variable used in the proof. This would not be present in an
    # implementation.
    # Function from each server that voted for this candidate in its currentTerm
    # to that voter's log.
    voterLog: dict[ServerId, list[LogEntry]] = {}

@dataclass(frozen=True, slots=True)
class Leader:
    """The following variables are used only on leaders"""
    # The next entry to send to each follower.
    nextIndex: dict[ServerId, int] = {}
    # The latest entry that each follower has acknowledged is the same as the
    # leader's. This is used to calculate commitIndex on the leader.
    matchIndex: dict[ServerId, int] = {}

@dataclass(frozen=True, slots=True)
class System:
    # Global variables
        
    # A bag of records representing requests and responses sent from one server
    # to another. TLAPS doesn't support the Bags module, so this is a function
    # mapping Message to Nat.
    messages: Counter[MessageId] = Counter()

    # A history variable used in the proof. This would not be present in an
    # implementation.
    # Keeps track of successful elections, including the initial logs of the
    # leader and voters' logs. Set of functions containing various things about
    # successful elections (see BecomeLeader).
    elections: list[ElectionEntry] = []

    # A history variable used in the proof. This would not be present in an
    # implementation.
    # Keeps track of every log ever in the system (set of logs).
    allLogs: list[LogEntry] = []

    # serverVars == <<currentTerm, state, votedFor>>
    serverVars: dict[ServerId, Server]

    # candidateVars == <<votesResponded, votesGranted, voterLog>>
    candidateVars: dict[ServerId, Candidate] = {}

    # leaderVars == <<nextIndex, matchIndex, elections>>
    leaderVars: dict[ServerId, Leader] = {}
    # End of per server variables.

    # All variables; used for stuttering (asserting state hasn't changed).
    # vars == <<messages, allLogs, serverVars, candidateVars, leaderVars, logVars>>

    # ----
    # Helpers

    # The set of all quorums. This just calculates simple majorities, but the only
    # important property is that every quorum overlaps with every other.
    def Quorum(self, votes: list[ServerId]) -> bool:
        # {i \in SUBSET(Server) : Cardinality(i) * 2 > Cardinality(Server)}
        return len(votes) > len(self.serverVars) / 2

    def Send(self, m: MessageId) -> Self:
        """Add a message to the bag of messages."""
        return replace(self, messages = WithMessage(m, self.messages))

    def Discard(self, m: MessageId) -> Self:
        """
        Remove a message from the bag of messages. Used when a server is done
        processing a message.
        """
        return replace(self, messages = WithoutMessage(m, self.messages))

    # Combination of Send and Discard
    def Reply(self, response: MessageId, request: MessageId) -> Self:
        return replace(self, messages = WithoutMessage(request, WithMessage(response, self.messages)))


    @classmethod
    def init(cls, server_ids: Sequence[ServerId]) -> Self:
        # Define initial values for all variables
        return cls(serverVars={i: Server() for i in server_ids})


def LastTerm(xlog: list[LogEntry]) -> int:
    """The term of the last entry in a log, or 0 if the log is empty."""
    return 0 if len(xlog) == 0 else xlog[-1].term

def WithMessage(m:MessageId, msgs: Counter[MessageId]) -> Counter[MessageId]:
    """
    Helper for Send and Reply. Given a message m and bag of messages, return a
    new bag of messages with one more m in it.
    """
    result = msgs.copy()
    result[m] += 1
    return result

def WithoutMessage(m:MessageId, msgs: Counter[MessageId]) -> Counter[MessageId]:
    """
    Helper for Discard and Reply. Given a message m and bag of messages, return
    a new bag of messages with one less m in it.
    """
    result = msgs.copy()
    if 0 < result[m]:
        result[m] -= 1
    return result

def Min(s):
    """Return the minimum value from a set, or undefined if the set is empty."""
    return min(s, default=Nil)

def Max(s):
    """Return the maximum value from a set, or undefined if the set is empty."""
    return max(s, default=Nil)

# ----
# Define state transitions

# Server i restarts from stable storage.
# It loses everything but its currentTerm, votedFor, and log.
Restart(i) ==
    /\ state'          = [state EXCEPT ![i] = Follower]
    /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
    /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
    /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
    /\ nextIndex'      = [nextIndex EXCEPT ![i] = [j \in Server |-> 1]]
    /\ matchIndex'     = [matchIndex EXCEPT ![i] = [j \in Server |-> 0]]
    /\ commitIndex'    = [commitIndex EXCEPT ![i] = 0]
    /\ UNCHANGED <<messages, currentTerm, votedFor, log, elections>>

# Server i times out and starts a new election.
Timeout(i) == /\ state[i] \in {Follower, Candidate}
              /\ state' = [state EXCEPT ![i] = Candidate]
              /\ currentTerm' = [currentTerm EXCEPT ![i] = currentTerm[i] + 1]
              # Most implementations would probably just set the local vote
              # atomically, but messaging localhost for it is weaker.
              /\ votedFor' = [votedFor EXCEPT ![i] = Nil]
              /\ votesResponded' = [votesResponded EXCEPT ![i] = {}]
              /\ votesGranted'   = [votesGranted EXCEPT ![i] = {}]
              /\ voterLog'       = [voterLog EXCEPT ![i] = [j \in {} |-> <<>>]]
              /\ UNCHANGED <<messages, leaderVars, logVars>>

# Candidate i sends j a RequestVote request.
RequestVote(i, j) ==
    /\ state[i] = Candidate
    /\ j \notin votesResponded[i]
    /\ Send([mtype         |-> RequestVoteRequest,
             mterm         |-> currentTerm[i],
             mlastLogTerm  |-> LastTerm(log[i]),
             mlastLogIndex |-> Len(log[i]),
             msource       |-> i,
             mdest         |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

# Leader i sends j an AppendEntries request containing up to 1 entry.
# While implementations may want to send more than 1 at a time, this spec uses
# just 1 because it minimizes atomic regions without loss of generality.
AppendEntries(i, j) ==
    /\ i /= j
    /\ state[i] = Leader
    /\ LET prevLogIndex == nextIndex[i][j] - 1
           prevLogTerm == IF prevLogIndex > 0 THEN
                              log[i][prevLogIndex].term
                          ELSE
                              0
           # Send up to 1 entry, constrained by the end of the log.
           lastEntry == Min({Len(log[i]), nextIndex[i][j]})
           entries == SubSeq(log[i], nextIndex[i][j], lastEntry)
       IN Send([mtype          |-> AppendEntriesRequest,
                mterm          |-> currentTerm[i],
                mprevLogIndex  |-> prevLogIndex,
                mprevLogTerm   |-> prevLogTerm,
                mentries       |-> entries,
                # mlog is used as a history variable for the proof.
                # It would not exist in a real implementation.
                mlog           |-> log[i],
                mcommitIndex   |-> Min({commitIndex[i], lastEntry}),
                msource        |-> i,
                mdest          |-> j])
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

# Candidate i transitions to leader.
BecomeLeader(i) ==
    /\ state[i] = Candidate
    /\ votesGranted[i] \in Quorum
    /\ state'      = [state EXCEPT ![i] = Leader]
    /\ nextIndex'  = [nextIndex EXCEPT ![i] =
                         [j \in Server |-> Len(log[i]) + 1]]
    /\ matchIndex' = [matchIndex EXCEPT ![i] =
                         [j \in Server |-> 0]]
    /\ elections'  = elections \cup
                         {[eterm     |-> currentTerm[i],
                           eleader   |-> i,
                           elog      |-> log[i],
                           evotes    |-> votesGranted[i],
                           evoterLog |-> voterLog[i]]}
    /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>

# Leader i receives a client request to add v to the log.
ClientRequest(i, v) ==
    /\ state[i] = Leader
    /\ LET entry == [term  |-> currentTerm[i],
                     value |-> v]
           newLog == Append(log[i], entry)
       IN  log' = [log EXCEPT ![i] = newLog]
    /\ UNCHANGED <<messages, serverVars, candidateVars,
                   leaderVars, commitIndex>>

# Leader i advances its commitIndex.
# This is done as a separate step from handling AppendEntries responses,
# in part to minimize atomic regions, and in part so that leaders of
# single-server clusters are able to mark entries committed.
AdvanceCommitIndex(i) ==
    /\ state[i] = Leader
    /\ LET # The set of servers that agree up through index.
           Agree(index) == {i} \cup {k \in Server :
                                         matchIndex[i][k] >= index}
           # The maximum indexes for which a quorum agrees
           agreeIndexes == {index \in 1..Len(log[i]) :
                                Agree(index) \in Quorum}
           # New value for commitIndex'[i]
           newCommitIndex ==
              IF /\ agreeIndexes /= {}
                 /\ log[i][Max(agreeIndexes)].term = currentTerm[i]
              THEN
                  Max(agreeIndexes)
              ELSE
                  commitIndex[i]
       IN commitIndex' = [commitIndex EXCEPT ![i] = newCommitIndex]
    /\ UNCHANGED <<messages, serverVars, candidateVars, leaderVars, log>>

# ----
# Message handlers
# i = recipient, j = sender, m = message

# Server i receives a RequestVote request from server j with
# m.mterm <= currentTerm[i].
HandleRequestVoteRequest(i, j, m) ==
    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
                    /\ m.mlastLogIndex >= Len(log[i])
        grant == /\ m.mterm = currentTerm[i]
                 /\ logOk
                 /\ votedFor[i] \in {Nil, j}
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
          \/ ~grant /\ UNCHANGED votedFor
       /\ Reply([mtype        |-> RequestVoteResponse,
                 mterm        |-> currentTerm[i],
                 mvoteGranted |-> grant,
                 # mlog is used just for the `elections' history variable for
                 # the proof. It would not exist in a real implementation.
                 mlog         |-> log[i],
                 msource      |-> i,
                 mdest        |-> j],
                 m)
       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars>>

# Server i receives a RequestVote response from server j with
# m.mterm = currentTerm[i].
HandleRequestVoteResponse(i, j, m) ==
    # This tallies votes even when the current state is not Candidate, but
    # they won't be looked at, so it doesn't matter.
    /\ m.mterm = currentTerm[i]
    /\ votesResponded' = [votesResponded EXCEPT ![i] =
                              votesResponded[i] \cup {j}]
    /\ \/ /\ m.mvoteGranted
          /\ votesGranted' = [votesGranted EXCEPT ![i] =
                                  votesGranted[i] \cup {j}]
          /\ voterLog' = [voterLog EXCEPT ![i] =
                              voterLog[i] @@ (j :> m.mlog)]
       \/ /\ ~m.mvoteGranted
          /\ UNCHANGED <<votesGranted, voterLog>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars>>

# Server i receives an AppendEntries request from server j with
# m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
# implementations could safely accept more by treating them the same as
# multiple independent requests of 1 entry.
HandleAppendEntriesRequest(i, j, m) ==
    LET logOk == \/ m.mprevLogIndex = 0
                 \/ /\ m.mprevLogIndex > 0
                    /\ m.mprevLogIndex <= Len(log[i])
                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
    IN /\ m.mterm <= currentTerm[i]
       /\ \/ /\ # reject request
                \/ m.mterm < currentTerm[i]
                \/ /\ m.mterm = currentTerm[i]
                   /\ state[i] = Follower
                   /\ \lnot logOk
             /\ Reply([mtype           |-> AppendEntriesResponse,
                       mterm           |-> currentTerm[i],
                       msuccess        |-> FALSE,
                       mmatchIndex     |-> 0,
                       msource         |-> i,
                       mdest           |-> j],
                       m)
             /\ UNCHANGED <<serverVars, logVars>>
          \/ # return to follower state
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Candidate
             /\ state' = [state EXCEPT ![i] = Follower]
             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages>>
          \/ # accept request
             /\ m.mterm = currentTerm[i]
             /\ state[i] = Follower
             /\ logOk
             /\ LET index == m.mprevLogIndex + 1
                IN \/ # already done with request
                       /\ \/ m.mentries = << >>
                          \/ /\ m.mentries /= << >>
                             /\ Len(log[i]) >= index
                             /\ log[i][index].term = m.mentries[1].term
                          # This could make our commitIndex decrease (for
                          # example if we process an old, duplicated request),
                          # but that doesn't really affect anything.
                       /\ commitIndex' = [commitIndex EXCEPT ![i] =
                                              m.mcommitIndex]
                       /\ Reply([mtype           |-> AppendEntriesResponse,
                                 mterm           |-> currentTerm[i],
                                 msuccess        |-> TRUE,
                                 mmatchIndex     |-> m.mprevLogIndex +
                                                     Len(m.mentries),
                                 msource         |-> i,
                                 mdest           |-> j],
                                 m)
                       /\ UNCHANGED <<serverVars, log>>
                   \/ # conflict: remove 1 entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) >= index
                       /\ log[i][index].term /= m.mentries[1].term
                       /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
                                          log[i][index2]]
                          IN log' = [log EXCEPT ![i] = new]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
                   \/ # no conflict: append entry
                       /\ m.mentries /= << >>
                       /\ Len(log[i]) = m.mprevLogIndex
                       /\ log' = [log EXCEPT ![i] =
                                      Append(log[i], m.mentries[1])]
                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
       /\ UNCHANGED <<candidateVars, leaderVars>>

# Server i receives an AppendEntries response from server j with
# m.mterm = currentTerm[i].
HandleAppendEntriesResponse(i, j, m) ==
    /\ m.mterm = currentTerm[i]
    /\ \/ /\ m.msuccess # successful
          /\ nextIndex'  = [nextIndex  EXCEPT ![i][j] = m.mmatchIndex + 1]
          /\ matchIndex' = [matchIndex EXCEPT ![i][j] = m.mmatchIndex]
       \/ /\ \lnot m.msuccess # not successful
          /\ nextIndex' = [nextIndex EXCEPT ![i][j] =
                               Max({nextIndex[i][j] - 1, 1})]
          /\ UNCHANGED <<matchIndex>>
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, logVars, elections>>

# Any RPC with a newer term causes the recipient to advance its term first.
UpdateTerm(i, j, m) ==
    /\ m.mterm > currentTerm[i]
    /\ currentTerm'    = [currentTerm EXCEPT ![i] = m.mterm]
    /\ state'          = [state       EXCEPT ![i] = Follower]
    /\ votedFor'       = [votedFor    EXCEPT ![i] = Nil]
       # messages is unchanged so m can be processed further.
    /\ UNCHANGED <<messages, candidateVars, leaderVars, logVars>>

# Responses with stale terms are ignored.
DropStaleResponse(i, j, m) ==
    /\ m.mterm < currentTerm[i]
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

# Receive a message.
Receive(m) ==
    LET i == m.mdest
        j == m.msource
    IN # Any RPC with a newer term causes the recipient to advance
       # its term first. Responses with stale terms are ignored.
       \/ UpdateTerm(i, j, m)
       \/ /\ m.mtype = RequestVoteRequest
          /\ HandleRequestVoteRequest(i, j, m)
       \/ /\ m.mtype = RequestVoteResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleRequestVoteResponse(i, j, m)
       \/ /\ m.mtype = AppendEntriesRequest
          /\ HandleAppendEntriesRequest(i, j, m)
       \/ /\ m.mtype = AppendEntriesResponse
          /\ \/ DropStaleResponse(i, j, m)
             \/ HandleAppendEntriesResponse(i, j, m)

# End of message handlers.
# ----
# Network state transitions

# The network duplicates a message
DuplicateMessage(m) ==
    /\ Send(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

# The network drops a message
DropMessage(m) ==
    /\ Discard(m)
    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

# ----
# Defines how the variables may transition.
Next == /\ \/ \E i \in Server : Restart(i)
           \/ \E i \in Server : Timeout(i)
           \/ \E i,j \in Server : RequestVote(i, j)
           \/ \E i \in Server : BecomeLeader(i)
           \/ \E i \in Server, v \in Value : ClientRequest(i, v)
           \/ \E i \in Server : AdvanceCommitIndex(i)
           \/ \E i,j \in Server : AppendEntries(i, j)
           \/ \E m \in DOMAIN messages : Receive(m)
           \/ \E m \in DOMAIN messages : DuplicateMessage(m)
           \/ \E m \in DOMAIN messages : DropMessage(m)
           # History variable that tracks every log ever:
        /\ allLogs' = allLogs \cup {log[i] : i \in Server}

# The specification must start with the initial state and transition according
# to Next.
Spec == Init /\ [][Next]_vars

# ===============================================================================