Notes on the raft algorithm
===========================

# Terminology

| Term      | Description                                                            |
| --------- | ---------------------------------------------------------------------- |
| Client    | Non-participant that sends data to the leader                          |
| Leader    | Participant that accepts data and sends to followers                   |
| Followers | Participant that receives data from the leader                         |
| Log       | State machine on each participant                                      |
| Term      | Time interval stating with an election and followed by log propagation |

* Message flow from client to leader to followers
* Two phases:
  1. Leader election
  2. Log propagation

Cluster configuration

election timeout >> message transmission time

