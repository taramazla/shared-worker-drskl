#!/bin/bash

# Script to run benchmarks sequentially on benchmark-1, benchmark-2, and benchmark-3
# This allows comparing the performance between different benchmarks

# Default values
BENCHMARK_TYPE=${BENCHMARK_TYPE:-"mixed"}
USERS=${USERS:-100}
SPAWN_RATE=${SPAWN_RATE:-10}
RUN_TIME=${RUN_TIME:-60}
# Default is to run all benchmark directories
SELECTED_BENCHMARKS="benchmark-1,benchmark-2,benchmark-3"

# Function to print usage information
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run benchmarks sequentially on selected benchmark directories"
    echo
    echo "Options:"
    echo "  -t, --type TYPE     Benchmark type: read, write, or mixed (default: $BENCHMARK_TYPE)"
    echo "  -b, --benchmarks LIST  Comma-separated list of benchmarks to run (default: $SELECTED_BENCHMARKS)"
    echo "                      Example: -b benchmark-1,benchmark-3"
    echo "  -u, --users N       Number of simulated users (default: $USERS)"
    echo "  -s, --spawn-rate N  User spawn rate per second (default: $SPAWN_RATE)"
    echo "  -r, --runtime N     Runtime in seconds (default: $RUN_TIME)"
    echo "  --help              Display this help message"
}

# Parse command line arguments for this wrapper script
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -t|--type)
            BENCHMARK_TYPE="$2"
            shift 2
            ;;
        -b|--benchmarks)
            SELECTED_BENCHMARKS="$2"
            shift 2
            ;;
        -u|--users)
            USERS="$2"
            shift 2
            ;;
        -s|--spawn-rate)
            SPAWN_RATE="$2"
            shift 2
            ;;
        -r|--runtime)
            RUN_TIME="$2"
            shift 2
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Change to benchmark directory
cd "$(dirname "$0")"

echo "=== Running sequential benchmarks on selected benchmark directories ==="
echo "Benchmark type: $BENCHMARK_TYPE"
echo "Selected benchmarks: $SELECTED_BENCHMARKS"
echo "Users: $USERS"
echo "Spawn rate: $SPAWN_RATE"
echo "Runtime: $RUN_TIME seconds"
echo

# Parse the comma-separated list of benchmark directories
IFS=',' read -ra BENCHMARK_ARRAY <<< "$SELECTED_BENCHMARKS"

# Run each selected benchmark sequentially
for benchmark_dir in "${BENCHMARK_ARRAY[@]}"; do
    echo "=== Running $benchmark_dir ==="
    BENCHMARK_DIRS=("../$benchmark_dir") ./run_all_benchmarks.sh -t "$BENCHMARK_TYPE" -u "$USERS" -s "$SPAWN_RATE" -r "$RUN_TIME"
    echo
done

echo "All sequential benchmarks completed successfully!"
echo "You can compare the results in the benchmark_results directories of each benchmark folder."
