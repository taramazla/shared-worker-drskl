#!/bin/bash

# Script to run benchmarks on benchmark-3 directory

# Change to benchmark directory
cd "$(dirname "$0")"

# Run the benchmark with benchmark-3 directory
BENCHMARK_DIRS=("../benchmark-3") ./run_all_benchmarks.sh "$@"
