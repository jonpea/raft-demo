A toy implementation of the Raft algorithm
==========================================

> ⚠️ This work is incomplete

A port to Python of the [TLA+](https://en.wikipedia.org/wiki/TLA%2B) specification for the [Raft consensus algorithm](https://raft.github.io/).

* For Diego Ongaro's TLA+ specification, see [github.com/ongardie/raft.tla](https://github.com/ongardie/raft.tla)
* For more on Raft, see [raft.github.io](https://raft.github.io/)
* For more on TLA+, see [Hillel Wayne's](https://www.hillelwayne.com/) learning resources at [Learn TLA+](https://learntla.com/).

---

# Notes

## Terminology

| Term      | Description                                                            |
| --------- | ---------------------------------------------------------------------- |
| Client    | Non-participant that sends data to the leader                          |
| Leader    | Participant that accepts data and sends to followers                   |
| Followers | Participant that receives data from the leader                         |
| Log       | State machine on each participant                                      |
| Term      | Time interval stating with an election and followed by log propagation |

Message flow from client to leader to followers

<!-- 
* Two phases:
  1. Leader election
  2. Log propagation

Cluster configuration

election timeout >> message transmission time 
-->

