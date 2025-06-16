from typing import Callable, Final
from hypothesis import Phase, Verbosity, seed, settings
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule
import hypothesis.strategies as st
import raft


SERVER_IDS: Final = [10, 20, 30]
LOG_VALUES: Final = [111, 222, 333]


@seed(0)
@settings(
    max_examples=5000,
    derandomize=False,
    verbosity=Verbosity.normal,
    phases=tuple(Phase),
    stateful_step_count=50,
    report_multiple_bugs=True,
)
class RaftStateMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.system = raft.System.init(serverIds=SERVER_IDS)
        self.data = raft.SystemData.init(serverIds=SERVER_IDS, logValues=LOG_VALUES)

    def step_through(self, transition, *args):
        transit: Callable[..., raft.System | None] = transition(*args)
        system: raft.System | None = transit(self.system)
        self.system = self.data.step(system or self.system)

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
    def single_leader_per_term(self):
        leaders_by_term: dict[raft.TermId, raft.ServerId] = {}
        for i in self.system.serverIds:
            if self.system.state[i] == raft.ServerState.LEADER:
                term = self.system.currentTerm[i]
                assert term not in leaders_by_term, f"Multiple leaders in term {term}"
                leaders_by_term[term] = i

    @invariant()
    def log_consistency(self):
        for i in self.system.serverIds:
            log: list[raft.LogEntry] = self.system.log[i]
            for entry in log:
                assert entry.term <= self.system.currentTerm[i], "Invalid term in log"


# Works with pytest or unittest
RaftTestCase = RaftStateMachine.TestCase
