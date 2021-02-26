

# How to test Olympus with Elle

The setup is pretty cluncky but it goes this way:

1. Run a cluster of olympus node (for instance with `./cluster.sh`)
2. Run `cargo run --bin fuzz_par_rw` with stands for fuzzy parallel reads and writes
3. Copy the output to a `olympus-analysis/history.clj` file that defines a `history` array of the operations
4. Run `lein run` in `olympus-analysis` to get the result

So far with 5 concurrent process and 2k operations, no anomalies were found.
