#!/bin/bash

# Script to run benchmarks on benchmark-1 directory

# Change to benchmark directory
cd "$(dirname "$0")"

# Run the benchmark with benchmark-1 directory
BENCHMARK_DIRS=("../benchmark-1") ./run_all_benchmarks.sh "$@"
