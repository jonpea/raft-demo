import collections
import operator
from typing import Callable, Final
from hypothesis import Phase, Verbosity, seed, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule
import hypothesis.strategies as st
import raft


SERVER_IDS: Final = [10, 20, 30]
LOG_VALUES: Final = [111, 222, 333]


@seed(0)
@settings(
    max_examples=1000,
    derandomize=False,
    verbosity=Verbosity.normal,
    phases=tuple(Phase),
    stateful_step_count=50,
    report_multiple_bugs=True,
)
class RaftStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.sys = self.sysprev = raft.System.init(serverIds=SERVER_IDS)
        self.data = raft.SystemData.init(serverIds=SERVER_IDS, logValues=LOG_VALUES)

    def step_through(self, transition, *args):
        transit: Callable[..., raft.System | None] = transition(*args)
        self.sysprev = self.sys
        sysnext: raft.System | None = transit(self.sys)
        self.sys = self.data.step(sysnext or self.sys)

    @rule(
        serverId=st.sampled_from(SERVER_IDS),
        dest_id=st.sampled_from(SERVER_IDS),
    )
    def request_vote(self, serverId, dest_id):
        self.step_through(raft.RequestVote, serverId, dest_id)

    @rule(serverId=st.sampled_from(SERVER_IDS))
    def timeout(self, serverId):
        self.step_through(raft.Timeout, serverId)

    @rule(
        serverId=st.sampled_from(SERVER_IDS),
        logValue=st.sampled_from(LOG_VALUES),
    )
    def client_request(self, serverId, logValue):
        self.step_through(raft.ClientRequest, serverId, logValue)

    @invariant()
    def unique_leader(self):
        """In each term, one server is designated leader"""
        leaders_by_term: dict[raft.TermId, raft.ServerId] = {}
        for i in self.sys.serverIds:
            if self.sys.state[i] == raft.ServerState.LEADER:
                term = self.sys.currentTerm[i]
                assert term not in leaders_by_term, "uniqueness"
                leaders_by_term[term] = i

    @invariant()
    def log_consistency(self):
        """Log terms increase in step with term counter."""
        for i in self.sys.serverIds:
            currentTerm = self.sys.currentTerm[i]
            logTerms = [entry.term for entry in self.sys.log[i]]
            assert max(logTerms, default=0) <= currentTerm, "causal"
            assert all(map(operator.le, logTerms[:-1], logTerms[1:])), "non-decreasing"

    @invariant()
    def leader_logs(self):
        """The leader preserves it own logs."""
        for i in self.sys.serverIds:
            if self.sys.state[i] is raft.ServerState.LEADER:
                if self.sysprev.state[i] is not raft.ServerState.LEADER:
                    assert all(  # maps over the shorter sequence
                        map(operator.eq, self.sysprev.log[i], self.sys.log[i])
                    )

    @invariant()
    def term_counter(self):
        """Term counter."""
        for i in self.sys.serverIds:
            assert (
                self.sysprev.currentTerm[i] <= self.sys.currentTerm[i]
            ), "non-decreasing"

    @invariant()
    def one_vote_per_server(self):
        # counts = collections.Counter(self.sys.elections)
        pass


# Works with pytest or unittest
RaftTestCase = RaftStateMachine.TestCase
