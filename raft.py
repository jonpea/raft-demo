import enum
import itertools
import random
from collections import Counter
from dataclasses import dataclass, replace
from typing import (
    Any,
    Callable,
    Final,
    Generator,
    Self,
    Sequence,
    TypeAlias,
    TypeVar,
)

LogId = int
LogValue = int
MessageId = int
ServerId = int
TermId = int
T = TypeVar("T")


class ServerState(enum.Enum):
    """Server states"""

    FOLLOWER = enum.auto()
    CANDIDATE = enum.auto()
    LEADER = enum.auto()


class NilType:
    pass


# A reserved value.
Nil: Final = NilType()


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
    mlastLogTerm: int
    mlastLogIndex: int


@dataclass(frozen=True, slots=True)
class RequestVoteResponse(Message):
    mvoteGranted: bool
    mlog: tuple[LogEntry, ...]  # hashable (unlike list)


@dataclass(frozen=True, slots=True)
class AppendEntriesRequest(Message):
    mprevLogIndex: int
    mprevLogTerm: int
    mentries: tuple[LogEntry, ...]
    mlog: tuple[LogEntry, ...]  # hashable (unlike list)
    mcommitIndex: int


@dataclass(frozen=True, slots=True)
class AppendEntriesResponse(Message):
    msuccess: bool
    mmatchIndex: int


@dataclass(frozen=True, slots=True)
class ElectionEntry:
    eterm: TermId
    eleader: ServerId
    elog: list[LogEntry]
    evotes: set[ServerId]
    evoterLog: dict[ServerId, list[LogEntry]]


@dataclass(frozen=True, slots=True)
class System:

    # Global variables
    # A bag of records representing requests and responses sent from one server
    # to another. TLAPS doesn't support the Bags module, so this is a function
    # mapping Message to Nat.
    messages: Counter[Message]

    # A history variable used in the proof. This would not be present in an
    # implementation.
    # Keeps track of successful elections, including the initial logs of the
    # leader and voters' logs. Set of functions containing various things about
    # successful elections (see BecomeLeader).
    elections: list[ElectionEntry]

    # A history variable used in the proof. This would not be present in an
    # implementation.
    # Keeps track of every log ever in the system (set of logs).
    allLogs: list[LogEntry]

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
    def Quorum(self, votes: set[int] | list[int]) -> bool:
        # {i \in SUBSET(Server) : Cardinality(i) * 2 > Cardinality(Server)}
        return len(votes) > len(self.serverIds) // 2

    @classmethod
    def init(cls, serverIds: Sequence[ServerId]) -> Self:
        """Define initial values for all variables."""
        return cls(
            # InitHistoryVars:
            elections=[],
            allLogs=[],
            voterLog={i: {} for i in serverIds},
            # InitServerVars:
            currentTerm={i: 1 for i in serverIds},
            state={i: ServerState.FOLLOWER for i in serverIds},
            votedFor={i: Nil for i in serverIds},
            # InitCandidateVars:
            votesResponded={i: set() for i in serverIds},
            votesGranted={i: set() for i in serverIds},
            # The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
            # leader does not send itself messages. It's still easier to include these
            # in the functions.
            # InitLeaderVars:
            nextIndex={
                i: {j: 1 for j in serverIds} for i in serverIds
            },  # base-0 for Python
            matchIndex={i: {j: 0 for j in serverIds} for i in serverIds},
            # InitLogVars:
            log={i: [] for i in serverIds},
            commitIndex={i: 0 for i in serverIds},
            # Init:
            messages=Counter(),
        )

    @property
    def serverIds(self) -> Sequence[ServerId]:
        return list(self.state.keys())


def LastTerm(xlog: list[LogEntry]) -> int:
    """The term of the last entry in a log, or 0 if the log is empty."""
    return 0 if len(xlog) == 0 else xlog[-1].term


def WithMessage(m: Message, msgs: Counter[Message]) -> Counter[Message]:
    """
    Helper for Send and Reply. Given a message m and bag of messages, return a
    new bag of messages with one more m in it.
    """
    result = msgs.copy()
    result[m] += 1
    return result


def WithoutMessage(m: Message, msgs: Counter[Message]) -> Counter[Message]:
    """
    Helper for Discard and Reply. Given a message m and bag of messages, return
    a new bag of messages with one less m in it.
    """
    result = msgs.copy()
    if 0 < result[m]:
        result[m] -= 1
    return result


# >>>
def Send(m: Message, msgs: Counter[Message]) -> Counter[Message]:
    """Add a message to the bag of messages."""
    return WithMessage(m, msgs)


def Discard(m: Message, msgs: Counter[Message]) -> Counter[Message]:
    """
    Remove a message from the bag of messages. Used when a server is done
    processing a message.
    """
    return WithoutMessage(m, msgs)


def Reply(
    response: Message,
    request: Message,
    msgs: Counter[Message],
) -> Counter[Message]:
    """Combination of Send and Discard."""
    return WithoutMessage(request, WithMessage(response, msgs))


# ----
# Define state transitions


# @dataclass(frozen=True, slots=True)
# class Transition:
#     terms: list[Callable]
#
#     @classmethod
#     def init(cls, *args) -> Self:
#         return cls(list(args))
#
# class Any(Transition):
#     pass
#
# class All(Transition):
#     pass


Transition: TypeAlias = Callable[[System], System | None]


def Restart(i: ServerId) -> Transition:
    """
    Server i restarts from stable storage.
    It loses everything but its currentTerm, votedFor, and log.
    """

    def step(sys: System) -> System | None:
        return replace(
            sys,
            state=sys.state | {i: ServerState.FOLLOWER},
            votesResponded=sys.votesResponded | {i: set()},
            votesGranted=sys.votesGranted | {i: set()},
            voterLog=sys.voterLog | {i: dict()},
            # base-0 for Python
            nextIndex=sys.nextIndex | {i: {j: 1 * 0 for j in sys.serverIds}},
            matchIndex=sys.matchIndex | {i: {j: 0 for j in sys.serverIds}},
            commitIndex=sys.commitIndex | {i: 0},
        )

    return step


def Timeout(i: ServerId) -> Transition:
    """Server i times out and starts a new election."""

    def step(sys: System) -> System | None:
        if sys.state[i] in (ServerState.FOLLOWER, ServerState.CANDIDATE):
            return replace(
                sys,
                state=sys.state | {i: ServerState.CANDIDATE},
                currentTerm=sys.currentTerm | {i: sys.currentTerm[i] + 1},
                # Most implementations would probably just set the local vote
                # atomically, but messaging localhost for it is weaker.
                votedFor=sys.votedFor | {i: Nil},
                votesResponded=sys.votesResponded | {i: set()},
                votesGranted=sys.votesGranted | {i: set()},
                voterLog=sys.voterLog | {i: dict()},
            )
        return None

    return step


def RequestVote(i: ServerId, j: ServerId) -> Transition:
    """Candidate i sends j a RequestVote request."""

    def step(sys: System) -> System | None:
        if j not in sys.votesResponded[i]:
            return replace(
                sys,
                state=sys.state | {i: ServerState.CANDIDATE},
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
        return None

    return step


def SubSeq(s: list[T], i_base1: int, j_base1: int) -> list[T]:
    """A Python-translation of the TLA+ library function."""
    # TLA+ indexing is "base-1 and inclusive at both ends"
    return s[i_base1 - 1 : j_base1]


def AppendEntries(i: ServerId, j: ServerId) -> Transition:
    """
    Leader i sends j an AppendEntries request containing up to 1 entry.
    While implementations may want to send more than 1 at a time, this spec uses
    just 1 because it minimizes atomic regions without loss of generality.
    """

    def step(sys: System) -> System | None:
        if i != j and sys.state[i] is ServerState.LEADER:
            prevLogIndex = sys.nextIndex[i][j] - 1  # base-0 (nextIndex is base-1)
            prevLogTerm = sys.log[i][prevLogIndex].term if prevLogIndex > 0 else 0
            # Send up to 1 entry, constrained by the end of the log.
            lastEntry = min(len(sys.log[i]), sys.nextIndex[i][j])
            entries = SubSeq(sys.log[i], sys.nextIndex[i][j], lastEntry)

            return replace(
                sys,
                messages=WithMessage(
                    AppendEntriesRequest(
                        mterm=sys.currentTerm[i],
                        mprevLogIndex=prevLogIndex,
                        mprevLogTerm=prevLogTerm,
                        mentries=tuple(entries),
                        # mlog is used as a history variable for the proof.
                        # It would not exist in a real implementation.
                        mlog=tuple(sys.log[i]),
                        mcommitIndex=min(sys.commitIndex[i], lastEntry),
                        msource=i,
                        mdest=j,
                    ),
                    sys.messages,
                ),
            )
        return None

    return step


def BecomeLeader(i: ServerId) -> Transition:
    """Candidate i transitions to leader."""

    def step(sys: System) -> System | None:
        if sys.state[i] is ServerState.CANDIDATE and sys.Quorum(sys.votesGranted[i]):
            return replace(
                sys,
                state=sys.state | {i: ServerState.LEADER},
                nextIndex=sys.nextIndex
                | {i: {j: len(sys.log[i]) + 1 for j in sys.serverIds}},
                matchIndex=sys.matchIndex | {i: {j: 0 for j in sys.serverIds}},
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
        return None

    return step


def ClientRequest(i: ServerId, v: LogValue) -> Transition:
    """Leader i receives a client request to add v to the log."""

    def step(sys: System) -> System | None:
        if sys.state[i] is ServerState.LEADER:
            entry = LogEntry(term=sys.currentTerm[i], value=v)
            return replace(sys, log=sys.log | {i: sys.log[i] + [entry]})
        return None

    return step


def AdvanceCommitIndex(i: ServerId) -> Transition:
    """
    Leader i advances its commitIndex.
    This is done as a separate step from handling AppendEntries responses,
    in part to minimize atomic regions, and in part so that leaders of
    single-server clusters are able to mark entries committed.
    """

    def step(sys: System) -> System | None:
        if sys.state[i] is ServerState.LEADER:
            # The set of servers that agree up through index.
            def Agree(index: int) -> set[ServerId]:
                return {i} | {k for k in sys.serverIds if sys.matchIndex[i][k] >= index}

            # The maximum indexes for which a quorum agrees
            agreeIndexes = {
                index for index in range(len(sys.log[i])) if sys.Quorum(Agree(index))
            }
            # New value for commitIndex'[i]
            newCommitIndex = (
                max(agreeIndexes)
                if len(agreeIndexes) != 0
                and sys.log[i][max(agreeIndexes)].term == sys.currentTerm[i]
                else sys.commitIndex[i]
            )
            return replace(sys, commitIndex=sys.commitIndex | {i: newCommitIndex})
        return None

    return step


# ----
# Message handlers
# i = recipient, j = sender, m = message


def HandleRequestVoteRequest(
    i: ServerId, j: ServerId, m: RequestVoteRequest
) -> Transition:
    """
    Server i receives a RequestVote request from server j with
    m.mterm <= currentTerm[i].
    """

    def step(sys: System) -> System | None:
        if m.mterm <= sys.currentTerm[i]:
            log_i: list[LogEntry] = sys.log[i]
            logOk: bool = (m.mlastLogTerm > LastTerm(log_i)) or (
                m.mlastLogTerm == LastTerm(log_i) and m.mlastLogIndex >= len(log_i)
            )
            grant: bool = (
                m.mterm == sys.currentTerm[i] and logOk and sys.votedFor[i] in (Nil, j)
            )
            request = m
            response = RequestVoteResponse(
                mterm=sys.currentTerm[i],
                mvoteGranted=grant,
                # mlog is used just for the `elections' history variable for
                # the proof. It would not exist in a real implementation.
                mlog=tuple(sys.log[i]),
                msource=i,
                mdest=j,
            )
            return replace(
                sys,
                messages=WithoutMessage(request, WithMessage(response, sys.messages)),
                votedFor=sys.votedFor | {i: j} if grant else sys.votedFor,
            )
        return None

    return step


def HandleRequestVoteResponse(
    i: ServerId, j: ServerId, m: RequestVoteResponse
) -> Transition:
    """
    Server i receives a RequestVote response from server j with
    m.mterm = currentTerm[i].
    """

    def action(sys: System) -> System | None:
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
                (sys.voterLog | {i: sys.voterLog[i] | {j: list(m.mlog)}})
                if m.mvoteGranted
                else sys.voterLog
            )
            return replace(
                sys,
                votesResponded=votesResponded,
                votesGranted=votesGranted,
                voterLog=voterLog,
                messages=WithoutMessage(m, sys.messages),
            )
        return None

    return action


def HandleAppendEntriesRequest(
    i: ServerId, j: ServerId, m: AppendEntriesRequest
) -> Transition:
    """
    Server i receives an AppendEntries request from server j with
    m.mterm <= currentTerm[i]. This just handles m.entries of length 0 or 1, but
    implementations could safely accept more by treating them the same as
    multiple independent requests of 1 entry.
    """

    def action(sys: System) -> System | None:
        log_i: list[LogEntry] = sys.log[i]
        logOk: bool = (m.mprevLogIndex == 0) or (
            0 < m.mprevLogIndex <= len(log_i)  # for decrement...
            and m.mprevLogTerm == log_i[m.mprevLogIndex - 1].term  # base-0
        )
        if m.mterm <= sys.currentTerm[i]:
            # fmt: off
            if (  
                m.mterm < sys.currentTerm[i] 
                or (
                    m.mterm == sys.currentTerm[i]
                    and sys.state[i] is ServerState.FOLLOWER
                    and not logOk
                )
            ):  # fmt: on
                # reject request
                response = AppendEntriesResponse(
                    mterm=sys.currentTerm[i],
                    msuccess=False,
                    mmatchIndex=0,
                    msource=i,
                    mdest=j,
                )
                request = m
                return replace(
                    sys,
                    messages=WithoutMessage(
                        request,
                        WithMessage(response, sys.messages),
                    ),
                )

            elif (  # fmt: off
                m.mterm == sys.currentTerm[i]
                and sys.state[i] is ServerState.CANDIDATE  # fmt: on
            ):
                # return to follower state
                return replace(sys, state=sys.state | {i: ServerState.FOLLOWER})

            elif (
                m.mterm == sys.currentTerm[i]
                and sys.state[i] is ServerState.FOLLOWER
                and logOk
            ):
                # accept request
                index = m.mprevLogIndex + 1
                if len(m.mentries) == 0 or (
                    0 < len(m.mentries)
                    and len(log_i) > index
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
                    return replace(
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
                    return replace(sys, log=sys.log | {i: log_i[:-1]})

                elif len(m.mentries) != 0 and len(log_i) == m.mprevLogIndex:
                    # no conflict: append entry
                    return replace(sys, log=sys.log | {i: log_i + [m.mentries[0]]})
        return None

    return action


def HandleAppendEntriesResponse(
    i: ServerId, j: ServerId, m: AppendEntriesResponse
) -> Transition:
    """
    Server i receives an AppendEntries response from server j with
    m.mterm = currentTerm[i].
    """

    def action(sys: System) -> System | None:
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
            return replace(
                sys,
                nextIndex=nextIndex,
                matchIndex=matchIndex,
                messages=WithoutMessage(m, sys.messages),
            )
        return None

    return action


def UpdateTerm(i: ServerId, j: ServerId, m: Message) -> Transition:
    """Any RPC with a newer term causes the recipient to advance its term first."""

    def action(sys: System) -> System | None:
        if m.mterm > sys.currentTerm[i]:
            return replace(
                sys,
                currentTerm=sys.currentTerm | {i: m.mterm},
                state=sys.state | {i: ServerState.FOLLOWER},
                votedFor=sys.votedFor | {i: Nil},
                # messages is unchanged so m can be processed further.
            )
        return None

    return action


def DropStaleResponse(i: ServerId, j: ServerId, m: Message) -> Transition:
    """Responses with stale terms are ignored."""

    def action(sys: System) -> System | None:
        if m.mterm < sys.currentTerm[i]:
            return replace(
                sys,
                messages=WithoutMessage(m, sys.messages),
            )
        return None

    return action


def Receive(m: Message) -> Transition:
    """Receive a message and apply the appropriate state transition."""

    def action(sys: System) -> System | None:
        i, j = m.mdest, m.msource

        ToTransition: TypeAlias = Callable[[ServerId, ServerId, Any], Transition]

        def invoke(f: ToTransition) -> System | None:
            nonlocal i, j, m, sys
            return f(i, j, m)(sys)

        def invoke2(f: ToTransition, g: ToTransition) -> System | None:
            candidate = invoke(f)
            if candidate is not None:
                return candidate
            return invoke(g)

        # Check for newer term
        if (next_sys := invoke(UpdateTerm)) is not None:
            return next_sys

        # Dispatch by message type
        match m:
            case RequestVoteRequest():
                return invoke(HandleRequestVoteRequest)
            case RequestVoteResponse():
                return invoke2(DropStaleResponse, HandleRequestVoteResponse)
            case AppendEntriesRequest():
                return invoke(HandleAppendEntriesRequest)
            case AppendEntriesResponse():
                return invoke2(DropStaleResponse, HandleAppendEntriesResponse)
            case _:
                raise ValueError(f"Unhandled message: {m!r}")

    return action


# End of message handlers.

# Network state transitions


def DuplicateMessage(m: Message) -> Transition:
    """The network duplicates a message."""

    def action(sys: System) -> System | None:
        return replace(sys, messages=WithMessage(m, sys.messages))

    return action


def DropMessage(m: Message) -> Transition:
    """The network drops a message."""

    def action(sys: System) -> System | None:
        return replace(sys, messages=WithoutMessage(m, sys.messages))

    return action


def messageTransitions(messages: Counter[Message]) -> Generator[Transition, None, None]:
    """Generate transitions for handling messages (receive, duplicate, drop)."""
    for m in messages:
        yield Receive(m)
        yield DuplicateMessage(m)
        yield DropMessage(m)


def serverTransitions(
    serverIds: Sequence[ServerId],
    logValues: Sequence[LogValue],
) -> Generator[Transition, None, None]:
    """Generate transitions for server actions (e.g., timeout, voting, client requests)."""
    for i in serverIds:
        yield Restart(i)
        yield Timeout(i)
        yield BecomeLeader(i)
        yield AdvanceCommitIndex(i)
        for j in serverIds:
            yield RequestVote(i, j)
            yield AppendEntries(i, j)
        for v in logValues:
            yield ClientRequest(i, v)


@dataclass(frozen=True, slots=True)
class SystemData:
    serverIds: list[ServerId]
    logValues: list[LogValue]
    transitions: list[Transition]

    @classmethod
    def init(cls, serverIds: list[ServerId], logValues: list[LogValue]) -> Self:
        """Initialize SystemData with server transitions."""
        return cls(
            serverIds=serverIds,
            logValues=logValues,
            transitions=list(
                serverTransitions(serverIds, logValues)
            ),  # Fixed: pass arguments
        )

    def step(self, system: System) -> System:
        """Apply one enabled Raft transition or stutter, returning new state."""
        # Combine static server transitions with dynamic message transitions
        transitions = itertools.chain(
            self.transitions,
            messageTransitions(system.messages),
        )
        # Find transitions that can apply in current state
        enabled = [t for t in transitions if t(system) is not None]
        # Apply a random enabled transition or stutter
        if len(enabled) == 0:
            return system
        system_next: System | None = random.choice(enabled)(system)
        assert system_next is not None  # to satisfy mypy
        return system_next

    def steps(
        self, system: System, num_steps: int, seed: int | None = None
    ) -> list[System]:
        """Run Raft simulation for given steps, returning state history."""
        if seed is not None:
            random.seed(seed)
        systems = [system]
        for _ in range(num_steps):
            system = self.step(system)
            systems.append(system)
        return systems
