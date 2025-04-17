#!/bin/bash
set -e

# Default parameters
USERS=${USERS:-100}
SPAWN_RATE=${SPAWN_RATE:-10}
RUN_TIME=${RUN_TIME:-60}

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Create a descriptive results directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="benchmark_results/read_benchmark_${TIMESTAMP}"
mkdir -p $RESULTS_DIR

echo "Starting READ-ONLY benchmark with $USERS users for $RUN_TIME seconds"
echo "Results will be saved to $RESULTS_DIR"

# Run the benchmark with read-only flag
USERS=$USERS SPAWN_RATE=$SPAWN_RATE RUN_TIME=$RUN_TIME "$SCRIPT_DIR/run_locust_benchmark.sh" --read-only

# Copy the results to the specific directory
cp benchmark_results/locust_read-only.log $RESULTS_DIR/
cp benchmark_results/locust_metrics_*.json $RESULTS_DIR/

echo "Benchmark complete! Results saved to $RESULTS_DIR"
echo "Summary of throughput and latency metrics:"
echo "----------------------------------------"