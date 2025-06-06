#!/bin/bash

# Script to run benchmarks on benchmark-2 directory

# Change to benchmark directory
cd "$(dirname "$0")"

# Run the benchmark with benchmark-2 directory
BENCHMARK_DIRS=("../benchmark-2") ./run_all_benchmarks.sh "$@"
