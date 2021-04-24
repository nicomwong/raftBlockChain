# Paxos Block Chain

Inspired by UCSB CS 171 Distributed System's project on the Paxos protocol, which I implemented at https://github.com/nicomwong/paxosBlockChain.

This project is similar, but it implements the Raft protocol for consensus in a distributed system. The Raft protocol is said to be easier to understand and more specifically defined for a multi-consensus application than Paxos. Similar to the aforementioned project, this implementation gaurantees the consistency of a replicated, append-only block chain (log) and key-value store (a simple state machine).
