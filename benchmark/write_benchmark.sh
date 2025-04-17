#!/bin/bash
set -e

# Default parameters
USERS=${USERS:-50}  # Default to fewer users for write benchmarks
SPAWN_RATE=${SPAWN_RATE:-5}  # Slower spawn rate for writes
RUN_TIME=${RUN_TIME:-60}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Create a descriptive results directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="benchmark_results/write_benchmark_${TIMESTAMP}"
mkdir -p $RESULTS_DIR

echo "Starting WRITE-ONLY benchmark with $USERS users for $RUN_TIME seconds"
echo "Results will be saved to $RESULTS_DIR"

# Run the benchmark with write-only flag
USERS=$USERS SPAWN_RATE=$SPAWN_RATE RUN_TIME=$RUN_TIME ${SCRIPT_DIR}/run_locust_benchmark.sh --write-only

# Copy the results to the specific directory
cp benchmark_results/locust_write-only.log $RESULTS_DIR/
cp benchmark_results/locust_metrics_*.json $RESULTS_DIR/

echo "Benchmark complete! Results saved to $RESULTS_DIR"
echo "Summary of throughput and latency metrics:"
echo "----------------------------------------"