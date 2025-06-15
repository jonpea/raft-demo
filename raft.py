from collections import Counter
from dataclasses import dataclass, replace
import enum
from typing import Callable, Final, Generator, Self, Sequence, TypeAlias

# --------------------------------- MODULE raft ---------------------------------
# This is the formal specification for the Raft consensus algorithm.
#
# Copyright 2014 Diego Ongaro.
# This work is licensed under the Creative Commons Attribution-4.0
# International License https://creativecommons.org/licenses/by/4.0/

# EXTENDS Naturals, FiniteSets, Sequences, TLC

"""
# The set of server IDs
Server: Final = [10, 20, 30, 40, 50]

# The set of requests that can go into the log
Value: Final = [100, 200, 300]
"""

MessageId = int
ServerId = int
TermId = int
LogId = int
LogValue = int


# Server states.
class ServerState(enum.Enum):
    Follower = enum.auto()
    Candidate = enum.auto()
    Leader = enum.auto()


class NilType:
    pass


# A reserved value.
Nil: Final = NilType()


# # Message types:
# class MessageType(enum.Enum):
#     RequestVoteRequest = enum.auto()
#     RequestVoteResponse = enum.auto()
#     AppendEntriesRequest = enum.auto()
#     AppendEntriesResponse = enum.auto()

# @dataclass(frozen=True, slots=True)
# class MessageEntry:
#     mtype: MessageType
#     mterm: TermId
#     mlastLogTerm: TermId
#     mlastLogIndex: int
#     msource: ServerId
#     mdest: ServerId


@dataclass(frozen=True, slots=True)
class LogEntry:
    term: int
    value: LogValue


@dataclass(frozen=True, slots=True)
class Message:
    mterm: TermId
    msource: ServerId
    mdest: ServerId


@dataclass(frozen=True, slots=True)
class RequestVoteRequest(Message):
    mlastLogTerm: LogEntry
    mlastLogIndex: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse(Message):
    mvoteGranted: bool
    mlog: list[LogEntry]


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest(Message):
    mprevLogIndex: int
    mprevLogTerm: int
    mentries: list[LogEntry]
    mlog: list[LogEntry]
    mcommitIndex: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse(Message):
    msuccess: bool
    mmatchIndex: int


@dataclass(frozen=True, slots=True)
class ElectionEntry:
    eterm: TermId
    eleader: ServerId
    elog: LogId
    evotes: dict[ServerId, list[ServerId]]
    evoterLog: dict[ServerId, dict[ServerId, list[LogEntry]]]


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

    """
    # serverVars == <<currentTerm, state, votedFor>>
    serverVars: dict[ServerId, Server]
    # candidateVars == <<votesResponded, votesGranted, voterLog>>
    candidateVars: dict[ServerId, Candidate] = {}
    # leaderVars == <<nextIndex, matchIndex, elections>>
    leaderVars: dict[ServerId, Leader] = {}
    """
    # Servers
    currentTerm: dict[ServerId, int]
    state: dict[ServerId, ServerState]
    votedFor: dict[ServerId, ServerId | NilType]
    log: dict[ServerId, list[LogEntry]]
    commitIndex: dict[ServerId, int]
    # Candidates
    votesResponded: dict[ServerId, set[ServerId]]
    votesGranted: dict[ServerId, set[ServerId]]
    voterLog: dict[ServerId, dict[ServerId, list[LogEntry]]]
    # Leader
    nextIndex: dict[ServerId, dict[ServerId, int]]
    matchIndex: dict[ServerId, dict[ServerId, int]]
    # End of per server variables.

    # All variables; used for stuttering (asserting state hasn't changed).
    # vars == <<messages, allLogs, serverVars, candidateVars, leaderVars, logVars>>

    # ----
    # Helpers

    # The set of all quorums. This just calculates simple majorities, but the only
    # important property is that every quorum overlaps with every other.
    def Quorum(self, votes: list[ServerId]) -> bool:
        # {i \in SUBSET(Server) : Cardinality(i) * 2 > Cardinality(Server)}
        return len(votes) > len(self.server_ids) // 2

    def Send(self, m: MessageId) -> Self:
        """Add a message to the bag of messages."""
        return replace(self, messages=WithMessage(m, self.messages))

    def Discard(self, m: MessageId) -> Self:
        """
        Remove a message from the bag of messages. Used when a server is done
        processing a message.
        """
        return replace(self, messages=WithoutMessage(m, self.messages))

    # Combination of Send and Discard
    def Reply(self, response: MessageId, request: MessageId) -> Self:
        return replace(
            self, messages=WithoutMessage(request, WithMessage(response, self.messages))
        )

    @classmethod
    def init(cls, server_ids: Sequence[ServerId]) -> Self:
        """Define initial values for all variables."""
        return cls(
            # InitHistoryVars:
            voterLog={i: {} for i in server_ids},
            # InitServerVars:
            currentTerm={i: 1 for i in server_ids},
            state={i: ServerState.Follower for i in server_ids},
            votedFor={i: Nil for i in server_ids},
            # InitCandidateVars:
            votesResponded={i: set() for i in server_ids},
            votesGranted={i: set() for i in server_ids},
            # The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
            # leader does not send itself messages. It's still easier to include these
            # in the functions.
            # InitLeaderVars:
            nextIndex={i: {j: 1 for j in server_ids} for i in server_ids},
            matchIndex={i: {j: 0 for j in server_ids} for i in server_ids},
            # InitLogVars:
            log={i: [] for i in server_ids},
            commitIndex={i: 0 for i in server_ids},
        )

    @property
    def server_ids(self) -> Sequence[ServerId]:
        return self.state.keys()


def LastTerm(xlog: list[LogEntry]) -> int:
    """The term of the last entry in a log, or 0 if the log is empty."""
    return 0 if len(xlog) == 0 else xlog[-1].term


def WithMessage(m: MessageId, msgs: Counter[MessageId]) -> Counter[MessageId]:
    """
    Helper for Send and Reply. Given a message m and bag of messages, return a
    new bag of messages with one more m in it.
    """
    result = msgs.copy()
    result[m] += 1
    return result


def WithoutMessage(m: MessageId, msgs: Counter[MessageId]) -> Counter[MessageId]:
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


@dataclass(frozen=True, slots=True)
class Conjunct:
    terms: tuple[Callable, ...]


@dataclass(frozen=True, slots=True)
class Disjunct:
    terms: tuple[Callable, ...]


Expression: TypeAlias = Conjunct | Disjunct


def any_of(*terms: Callable) -> Disjunct:
    return Disjunct(terms)


def all_of(*terms: Callable) -> Conjunct:
    return Conjunct(terms)


@dataclass(frozen=True, slots=True)
class Transition:
    system: System
    args: tuple


def transition(system: System, *args, **kwargs) -> Transition:
    return Transition(system=replace(system, **kwargs), args=args)


Transitions: TypeAlias = Generator[Transition]
TransitionActions: TypeAlias = Callable[[System], Transitions]


# Server i restarts from stable storage.
# It loses everything but its currentTerm, votedFor, and log.
def Restart(i: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        yield transition(
            sys,
            state=sys.state | {i: ServerState.Follower},
            votesResponded=sys.votesResponded | {i: []},
            votesGranted=sys.votesGranted | {i: []},
            voterLog=sys.voterLog | {i: {}},
            nextIndex=sys.nextIndex | {i: {j: 1 for j in sys.server_ids}},
            matchIndex=sys.matchIndex | {i: {j: 0 for j in sys.server_ids}},
            commitIndex=sys.commitIndex | {i: 0},
        )

    return step


# Server i times out and starts a new election.
def Timeout(i: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if sys.state[i] in (ServerState.Follower, ServerState.Candidate):
            yield transition(
                sys,
                state=sys.state | {i: ServerState.Candidate},
                currentTerm=sys.currentTerm | {i: sys.currentTerm[i] + 1},
                # Most implementations would probably just set the local vote
                # atomically, but messaging localhost for it is weaker.
                votedFor=sys.votedFor | {i: Nil},
                votesResponded=sys.votesResponded | {i: []},
                votesGranted=sys.votesGranted | {i: []},
                voterLog=sys.voterLog | {i: {}},
            )

    return step


# Candidate i sends j a RequestVote request.
def RequestVote(i: ServerId, j: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if j not in sys.votesResponded[i]:
            yield transition(
                sys,
                state=sys.state | {i: ServerState.Candidate},
                messages=WithMessage(
                    RequestVoteRequest(
                        mterm=sys.currentTerm[i],
                        mlastLogTerm=LastTerm(sys.log[i]),
                        mlastLogIndex=len(sys.log[i]),
                        msource=i,
                        mdest=j,
                    ),
                    sys.messages,
                ),
            )

    return step


def SubSeq(s, i_base1: int, j_base1: int):
    """A Python-translation of the TLA+ library function."""
    # TLA+ indexing is "base-1 and inclusive at both ends"
    return s[i_base1 - 1 : j_base1]


# Leader i sends j an AppendEntries request containing up to 1 entry.
# While implementations may want to send more than 1 at a time, this spec uses
# just 1 because it minimizes atomic regions without loss of generality.
def AppendEntries(i: ServerId, j: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if i != j and sys.state[i] is ServerState.Leader:
            prevLogIndex = sys.nextIndex[i][j] - 1
            prevLogTerm = sys.log[i][prevLogIndex].term if prevLogIndex > 0 else 0
            # Send up to 1 entry, constrained by the end of the log.
            lastEntry = min(len(sys.log[i]), sys.nextIndex[i][j])
            entries = SubSeq(sys.log[i], sys.nextIndex[i][j], lastEntry)

            yield transition(
                sys,
                messages=WithMessage(
                    AppendEntriesRequest(
                        mterm=sys.currentTerm[i],
                        mprevLogIndex=prevLogIndex,
                        mprevLogTerm=prevLogTerm,
                        mentries=entries,
                        # mlog is used as a history variable for the proof.
                        # It would not exist in a real implementation.
                        mlog=sys.log[i],
                        mcommitIndex=min(sys.commitIndex[i], lastEntry),
                        msource=i,
                        mdest=j,
                    ),
                    sys.messages,
                ),
            )

    return step


# Candidate i transitions to leader.
def BecomeLeader(i: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if sys.state[i] is ServerState.Candidate and sys.Quorum(sys.votesGranted[i]):
            yield transition(
                sys,
                state=sys.state | {i: ServerState.Leader},
                nextIndex=sys.nextIndex
                | {i: {j: len(sys.log[i]) + 1 for j in sys.server_ids}},
                matchIndex=sys.matchIndex | {i: {j: 0 for j in sys.server_ids}},
                elections=sys.elections
                + [
                    ElectionEntry(
                        eterm=sys.currentTerm[i],
                        eleader=i,
                        elog=sys.log[i],
                        evotes=sys.votesGranted[i],
                        evoterLog=sys.voterLog[i],
                    ),
                ],
            )


# Leader i receives a client request to add v to the log.
def ClientRequest(i: ServerId, v: LogValue) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if sys.state[i] is ServerState.Leader:
            entry = LogEntry(term=sys.currentTerm[i], value=v)
            yield transition(sys, log=sys.log | {i: sys.log[i] + [entry]})

    return step


# Leader i advances its commitIndex.
# This is done as a separate step from handling AppendEntries responses,
# in part to minimize atomic regions, and in part so that leaders of
# single-server clusters are able to mark entries committed.
def AdvanceCommitIndex(i: ServerId) -> TransitionActions:
    def step(sys: System) -> Transitions:
        if sys.state[i] is ServerState.Leader:
            # The set of servers that agree up through index.
            def Agree(index: int) -> set[ServerId]:
                return {i} | {
                    k for k in sys.server_ids if sys.matchIndex[i][k] >= index
                }

            # The maximum indexes for which a quorum agrees
            agreeIndexes = {
                index for index in range(len(sys.log[i])) if sys.Quorum(Agree(index))
            }
            # New value for commitIndex'[i]
            newCommitIndex = (
                max(agreeIndexes)
                if len(agreeIndexes) != 0
                and sys.log[i][Max(agreeIndexes)].term == sys.currentTerm[i]
                else sys.commitIndex[i]
            )
            yield transition(sys, commitIndex=sys.commitIndex | {i: newCommitIndex})

    return step


# ----
# Message handlers
# i = recipient, j = sender, m = message


# Server i receives a RequestVote request from server j with
# m.mterm <= currentTerm[i].
def HandleRequestVoteRequest(
    i: ServerId, j: ServerId, m: RequestVoteRequest
) -> TransitionActions:
    def step(sys: System) -> Transitions:
        log_i: list[LogEntry] = sys.log[i]
        logOk: bool = (m.mlastLogTerm > LastTerm(log_i)) or (
            m.mlastLogTerm == LastTerm(log_i) and m.mlastLogIndex >= len(log_i)
        )
        grant: bool = (
            m.mterm == sys.currentTerm[i] and logOk and sys.votedFor[i] in (Nil, j)
        )
        if m.mterm <= sys.currentTerm[i]:
            request = m
            response = RequestVoteResponse(
                mterm=sys.currentTerm[i],
                mvoteGranted=grant,
                # mlog is used just for the `elections' history variable for
                # the proof. It would not exist in a real implementation.
                mlog=sys.log[i],
                msource=i,
                mdest=j,
            )
            yield transition(
                sys,
                messages=WithoutMessage(request, WithMessage(response, sys.messages)),
                votedFor=sys.votedFor | {i: j} if grant else sys.votedFor,
            )

    return step


# Server i receives a RequestVote response from server j with
# m.mterm = currentTerm[i].
def HandleRequestVoteResponse(
    i: ServerId, j: ServerId, m: RequestVoteResponse
) -> TransitionActions:
    def action(sys: System) -> Transitions:
        # This tallies votes even when the current state is not Candidate, but
        # they won't be looked at, so it doesn't matter.
        if m.mterm == sys.currentTerm[i]:
            votesResponded = sys.votesResponded | {i: sys.votesResponded[i] | {j}}
            votesGranted = (
                (sys.votesGranted | {i: sys.votesGranted[i] | {j}})
                if m.mvoteGranted
                else sys.votesGranted
            )
            voterLog = (
                (sys.voterLog | {i: sys.voterLog[i] | {j: m.mlog}})
                if m.mvoteGranted
                else sys.voterLog
            )
            yield transition(
                sys,
                votesResponded=votesResponded,
                votesGranted=votesGranted,
                voterLog=voterLog,
                messages=WithoutMessage(m, sys.messages),
            )

    return action


# Server i receives an AppendEntries request from server j with
# m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
# implementations could safely accept more by treating them the same as
# multiple independent requests of 1 entry.
def HandleAppendEntriesRequest(
    i: ServerId, j: ServerId, m: AppendEntriesRequest
) -> TransitionActions:
    def action(sys: System) -> Transitions:
        log_i: list[LogEntry] = sys.log[i]
        logOk: bool = (m.mprevLogIndex == 0) or (
            0 < m.mprevLogIndex <= len(log_i)
            and m.mprevLogTerm == log_i[m.mprevLogIndex].term
        )
        if m.mterm <= sys.currentTerm[i]:
            # fmt: off
            if (  
                m.mterm < sys.currentTerm[i] 
                or (
                    m.mterm == sys.currentTerm[i]
                    and sys.state[i] is ServerState.Follower
                    and not logOk
                )
            ):  # fmt: on
                # reject request
                yield sys.Reply(
                    response=AppendEntriesResponse(
                        mterm=sys.currentTerm[i],
                        msuccess=False,
                        mmatchIndex=0,
                        msource=i,
                        mdest=j,
                    ),
                    request=m,
                )

            elif (  # fmt: off
                m.mterm == sys.currentTerm[i]
                and sys.state[i] is ServerState.Candidate  # fmt: on
            ):
                # return to follower state
                yield transition(sys, state=sys.state | {i: ServerState.Follower})

            elif (
                m.mterm == sys.currentTerm[i]
                and sys.state[i] is ServerState.Follower
                and logOk
            ):
                # accept request
                index = m.mprevLogIndex + 1
                if len(m.mentries) == 0 or (
                    0 < len(m.mentries)
                    and len(log_i) >= index
                    and log_i[index].term == m.mentries[0].term
                ):
                    # already done with request

                    # This could make our commitIndex decrease (for
                    # example if we process an old, duplicated request),
                    # but that doesn't really affect anything.
                    commitIndex = sys.commitIndex | {i: m.mcommitIndex}
                    response = AppendEntriesResponse(
                        mterm=sys.currentTerm[i],
                        msuccess=True,
                        mmatchIndex=m.mprevLogIndex + len(m.mentries),
                        msource=i,
                        mdest=j,
                    )
                    yield transition(
                        sys,
                        commitIndex=commitIndex,
                        messages=WithoutMessage(m, WithMessage(response, sys.messages)),
                    )

                elif (
                    len(m.mentries) != 0
                    and len(log_i) >= index
                    and log_i[index].term != m.mentries[0].term
                ):
                    # conflict: remove 1 entry
                    new = {index2: log_i[index2] for index2 in range(len(log_i) - 1)}
                    yield transition(sys, log=sys.log | {i: new})

                elif len(m.mentries) != 0 and len(log_i) == m.mprevLogIndex:
                    # no conflict: append entry
                    yield transition(sys, log=sys.log | {i: log_i + [m.mentries[0]]})

    return action


# Server i receives an AppendEntries response from server j with
# m.mterm = currentTerm[i].
def HandleAppendEntriesResponse(
    i: ServerId, j: ServerId, m: AppendEntriesResponse
) -> TransitionActions:
    def action(sys: System) -> Transitions:
        if m.mterm == sys.currentTerm[i]:
            nIi: dict[ServerId, int] = sys.nextIndex[i]
            nextIndex = (
                sys.nextIndex | {i: nIi | {j: m.mmatchIndex + 1}}
                if m.msuccess
                else sys.nextIndex | {i: nIi | {j: max(nIi[j] - 1, 1)}}
            )
            matchIndex = (
                sys.matchIndex | {i: sys.matchIndex[i] | {j: m.mmatchIndex}}
                if m.msuccess
                else sys.matchIndex
            )
            yield transition(
                sys,
                nextIndex=nextIndex,
                matchIndex=matchIndex,
                messages=WithoutMessage(m, sys.messages),
            )

    return action


# Any RPC with a newer term causes the recipient to advance its term first.
def UpdateTerm(i: ServerId, j: ServerId, m: Message) -> TransitionActions:
    def action(sys: System) -> Transitions:
        if m.mterm > sys.currentTerm[i]:
            yield transition(
                sys,
                currentTerm=sys.currentTerm | {i: m.mterm},
                state=sys.state | {i: ServerState.Follower},
                votedFor=sys.votedFor | {i: Nil},
                # messages is unchanged so m can be processed further.
            )

    return action


# Responses with stale terms are ignored.
def DropStaleResponse(i: ServerId, j: ServerId, m: Message) -> TransitionActions:
    def action(sys: System) -> Transitions:
        if m.mterm < sys.currentTerm[i]:
            yield transition(
                sys,
                messages=WithoutMessage(m, sys.messages),
            )

    return action


# Receive a message.
def Receive(m: Message):
    def match_on(i: ServerId, j: ServerId, m: Message):
        match m:
            case RequestVoteRequest():
                return HandleRequestVoteRequest(i, j, m)
            case RequestVoteResponse():
                return any_of(
                    DropStaleResponse(i, j, m),
                    HandleRequestVoteResponse(i, j, m),
                )
            case AppendEntriesRequest():
                return HandleAppendEntriesRequest(i, j, m)
            case AppendEntriesResponse():
                return any_of(
                    DropStaleResponse(i, j, m),
                    HandleAppendEntriesResponse(i, j, m),
                )
            case _:
                raise ValueError(f"unhandled message: {m!r}")

    i: ServerId = m.mdest
    j: ServerId = m.msource

    # Any RPC with a newer term causes the recipient to advance
    # its term first. Responses with stale terms are ignored.
    return all_of(
        UpdateTerm(i, j, m),
        match_on(i, j, m),
    )


# End of message handlers.

# Network state transitions


# The network duplicates a message
def DuplicateMessage(m: MessageEntry) -> TransitionActions:
    def action(sys: System) -> Transitions:
        yield sys.Send(m)

    return action


# The network drops a message
def DropMessage(m: MessageEntry):
    def action(sys: System) -> Transitions:
        yield sys.Discard(m)

    return action


def transitions(state) -> list[Callable[[System], Transitions]]:
    def generate(state) -> Generator[Transition]:
        for i in state.server_ids:
            yield Restart(i)
            yield Timeout(i)
            yield BecomeLeader(i)
            yield AdvanceCommitIndex(i)
            for j in state.server_ids:
                yield RequestVote(i, j)
                yield AppendEntries(i, j)
            for v in state.values:
                yield ClientRequest(i, v)
        for m in state.messages:
            yield Receive(m)
            yield DuplicateMessage(m)
            yield DropMessage(m)

    return list(generate(state))
