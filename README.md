# Olympus

Olympus is a toy implementation of [Hermes replication protocol](https://hermes-protocol.com/)
in Rust.
Its main purpose is not to be a production-ready implementation,
but a learning platform.

## TODO

There's a lot remaining to work on:

- The write replay feature
- The Read-Modify-Write protocol
- Leases based on Paxos
- Various optimizations, starting with RDMA

Also, tests could go even further in order to verify the implementation
against the TLA+ specification.

## How to test Olympus with Elle

The setup is pretty cluncky but it goes this way:

1. Run a cluster of olympus node (for instance with `./cluster.sh`)
2. Run `cargo run --bin fuzz_par_rw` with stands for fuzzy parallel reads and writes
3. Copy the output to a `olympus-analysis/history.clj` file that defines a `history` array of the operations
4. Run `lein run` in `olympus-analysis` to get the result

So far with 5 concurrent process and 2k operations, no anomalies were found.

## Papers to read

- Antonios Katsarakis, Vasilis Gavrielatos, M.R. Siavash Katebzadeh, Arpit Joshi, Aleksandar Dragojevic, Boris Grot, and Vijay Nagarajan. 2020. Hermes: A Fast, Fault-Tolerant and Linearizable Replication Protocol. In Proceedings of the Twenty-Fifth International Conference on Architectural Support for Programming Languages and Operating Systems (ASPLOS '20). Association for Computing Machinery, New York, NY, USA, 201–217. [DOI](https://doi.org/10.1145/3373376.3378496)
- Aleksandar Dragojević, Dushyanth Narayanan, Orion Hodson, and Miguel Castro. 2014. FaRM: fast remote memory. In Proceedings of the 11th USENIX Conference on Networked Systems Design and Implementation (NSDI'14). USENIX Association, USA, 401–414. [link](https://www.usenix.org/system/files/conference/nsdi14/nsdi14-paper-dragojevic.pdf)
- Leslie Lamport, Dahlia Malkhi, and Lidong Zhou. 2009. Vertical paxos and primary-backup replication. In Proceedings of the 28th ACM symposium on Principles of distributed computing (PODC '09). Association for Computing Machinery, New York, NY, USA, 312–313. [DOI](https://doi.org/10.1145/1582716.1582783)
