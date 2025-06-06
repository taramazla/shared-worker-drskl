#!/bin/bash

# Script to run specific benchmark scenarios across selected directories
# This is a convenient wrapper around the other benchmark scripts

# Colors for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
SCENARIO="mixed"     # Options: read, write, mixed, failover, compare-failover
BENCHMARK_DIRS="benchmark-1" # Default directory, can be comma-separated
USERS=100
SPAWN_RATE=10
RUN_TIME=60
WORKER_NODE="worker1" # Only used for failover scenarios

# Function to print usage information
print_usage() {
    echo -e "${BLUE}Usage:${NC} $0 [OPTIONS]"
    echo "Run specific benchmark scenarios across selected directories"
    echo
    echo -e "${BLUE}Options:${NC}"
    echo "  -s, --scenario TYPE    Benchmark scenario to run (default: $SCENARIO)"
    echo "                         Options: read, write, mixed, failover, compare-failover"
    echo "  -d, --dirs DIRS        Comma-separated list of benchmark directories (default: $BENCHMARK_DIRS)"
    echo "                         Example: -d benchmark-1,benchmark-3"
    echo "  -u, --users N          Number of simulated users (default: $USERS)"
    echo "  -r, --spawn-rate N     User spawn rate per second (default: $SPAWN_RATE)"
    echo "  -t, --runtime N        Runtime in seconds (default: $RUN_TIME)"
    echo "  -w, --worker NODE      Worker node for failover tests (default: $WORKER_NODE)"
    echo "                         Options: worker1, worker2, worker3, worker4"
    echo "  --help                 Display this help message"
    echo
    echo -e "${BLUE}Examples:${NC}"
    echo "  $0 --scenario read --dirs benchmark-1,benchmark-2"
    echo "  $0 --scenario failover --dirs benchmark-3 --worker worker2"
    echo "  $0 --scenario compare-failover --dirs benchmark-1,benchmark-2"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -s|--scenario)
            SCENARIO="$2"
            shift 2
            ;;
        -d|--dirs)
            BENCHMARK_DIRS="$2"
            shift 2
            ;;
        -u|--users)
            USERS="$2"
            shift 2
            ;;
        -r|--spawn-rate)
            SPAWN_RATE="$2"
            shift 2
            ;;
        -t|--runtime)
            RUN_TIME="$2"
            shift 2
            ;;
        -w|--worker)
            WORKER_NODE="$2"
            shift 2
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option:${NC} $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate scenario
if [[ "$SCENARIO" != "read" && "$SCENARIO" != "write" && "$SCENARIO" != "mixed" && "$SCENARIO" != "failover" && "$SCENARIO" != "compare-failover" ]]; then
    echo -e "${RED}Error:${NC} Invalid scenario. Must be one of: read, write, mixed, failover, compare-failover"
    exit 1
fi

# Function to run standard benchmark
run_standard_benchmark() {
    local benchmark_type=$1
    echo -e "\n${GREEN}Running $benchmark_type benchmark on directories: $BENCHMARK_DIRS${NC}"
    ./run_all_benchmarks.sh -t "$benchmark_type" -d "$BENCHMARK_DIRS" \
        -u "$USERS" -s "$SPAWN_RATE" -r "$RUN_TIME"
}

# Function to run worker failover benchmark
run_failover_benchmark() {
    # Split comma-separated benchmark directories
    IFS=',' read -ra benchmark_array <<< "$BENCHMARK_DIRS"

    # Run failover benchmark for each directory
    for dir in "${benchmark_array[@]}"; do
        echo -e "\n${GREEN}Running failover benchmark on directory: $dir${NC}"
        ./worker_failover_benchmark.sh --dir "$dir" --failure-type "$WORKER_NODE" \
            --type "mixed" --users "$USERS" --spawn-rate "$SPAWN_RATE" \
            --runtime "$RUN_TIME"
    done
}

# Run the appropriate scenario
case "$SCENARIO" in
    read)
        run_standard_benchmark "read"
        ;;
    write)
        run_standard_benchmark "write"
        ;;
    mixed)
        run_standard_benchmark "mixed"
        ;;
    failover)
        run_failover_benchmark
        ;;
    compare-failover)
        echo -e "\n${GREEN}Running failover comparison on directories: $BENCHMARK_DIRS${NC}"
        ./compare_worker_failovers.sh -d "$BENCHMARK_DIRS" \
            -u "$USERS" -s "$SPAWN_RATE" -r "$RUN_TIME"
        ;;
esac

echo -e "\n${GREEN}✓ Benchmark scenario completed successfully!${NC}"
