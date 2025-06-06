#!/bin/bash

# Script to run benchmarks on benchmark-1, benchmark-2, and benchmark-3 directories
# This script allows running read, write, or mixed benchmarks across specified benchmark directories

# Available options: read, write, mixed
BENCHMARK_TYPE=${BENCHMARK_TYPE:-"mixed"}
# Default benchmark directories (can be overridden with environment variable)
BENCHMARK_DIRS=${BENCHMARK_DIRS:-("../benchmark-1" "../benchmark-2" "../benchmark-3")}

# Default values
USERS=${USERS:-100}
SPAWN_RATE=${SPAWN_RATE:-10}
RUN_TIME=${RUN_TIME:-60}
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"citus"}
DB_PASSWORD=${DB_PASSWORD:-"citus"}
DB_NAME=${DB_NAME:-"citus"}
READ_RATIO=${READ_RATIO:-80}
WRITE_RATIO=${WRITE_RATIO:-20}

# Function to print usage information
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run benchmarks across multiple benchmark directories"
    echo
    echo "Options:"
    echo "  -t, --type TYPE     Benchmark type: read, write, or mixed (default: $BENCHMARK_TYPE)"
    echo "  -d, --dirs DIRS     Comma-separated list of benchmark directories to run (default: all)"
    echo "                      Example: -d benchmark-1,benchmark-3"
    echo "  -u, --users N       Number of simulated users (default: $USERS)"
    echo "  -s, --spawn-rate N  User spawn rate per second (default: $SPAWN_RATE)"
    echo "  -r, --runtime N     Runtime in seconds (default: $RUN_TIME)"
    echo "  -h, --host HOST     Database host (default: $DB_HOST)"
    echo "  -p, --port PORT     Database port (default: $DB_PORT)"
    echo "  --read-ratio N      Read ratio percentage for mixed benchmark (default: $READ_RATIO)"
    echo "  --write-ratio N     Write ratio percentage for mixed benchmark (default: $WRITE_RATIO)"
    echo "  --help              Display this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -t|--type)
            BENCHMARK_TYPE="$2"
            shift 2
            ;;
        -d|--dirs)
            # Convert comma-separated list to array of directory paths
            IFS=',' read -ra selected_dirs <<< "$2"
            BENCHMARK_DIRS=()
            for dir in "${selected_dirs[@]}"; do
                # Add "../" prefix if not already present
                if [[ "$dir" != "../"* ]]; then
                    BENCHMARK_DIRS+=("../$dir")
                else
                    BENCHMARK_DIRS+=("$dir")
                fi
            done
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
        -h|--host)
            DB_HOST="$2"
            shift 2
            ;;
        -p|--port)
            DB_PORT="$2"
            shift 2
            ;;
        --read-ratio)
            READ_RATIO="$2"
            shift 2
            ;;
        --write-ratio)
            WRITE_RATIO="$2"
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

# Validate benchmark type
if [[ "$BENCHMARK_TYPE" != "read" && "$BENCHMARK_TYPE" != "write" && "$BENCHMARK_TYPE" != "mixed" ]]; then
    echo "Error: Invalid benchmark type. Must be one of: read, write, mixed"
    print_usage
    exit 1
fi

# Validate read/write ratio for mixed benchmark
if [[ "$BENCHMARK_TYPE" == "mixed" ]]; then
    if [ "$READ_RATIO" -lt 0 ] || [ "$WRITE_RATIO" -lt 0 ] || [ $(($READ_RATIO + $WRITE_RATIO)) -ne 100 ]; then
        echo "Error: READ_RATIO and WRITE_RATIO must be non-negative and sum to 100."
        exit 1
    fi
fi

# Export environment variables for Locust
export DB_HOST
export DB_PORT
export DB_USER
export DB_PASSWORD
export DB_NAME
export USERS
export SPAWN_RATE
export RUN_TIME

# Configure read/write ratios based on benchmark type
if [[ "$BENCHMARK_TYPE" == "read" ]]; then
    export READ_RATIO=100
    export WRITE_RATIO=0
elif [[ "$BENCHMARK_TYPE" == "write" ]]; then
    export READ_RATIO=0
    export WRITE_RATIO=100
else
    export READ_RATIO=$READ_RATIO
    export WRITE_RATIO=$WRITE_RATIO
fi

# Source the setup script to ensure the environment is ready
source ./setup_benchmark_env.sh

echo "=== Starting $BENCHMARK_TYPE benchmark across all benchmark directories ==="
echo "Users: $USERS, Spawn rate: $SPAWN_RATE, Runtime: $RUN_TIME seconds"
if [[ "$BENCHMARK_TYPE" == "mixed" ]]; then
    echo "Read/Write ratio: $READ_RATIO/$WRITE_RATIO"
fi
echo "Database connection: $DB_HOST:$DB_PORT"
echo

# Run benchmarks for each directory
for dir in "${BENCHMARK_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        echo "Warning: Directory $dir not found. Skipping."
        continue
    fi

    echo "=== Running benchmark in $dir ==="

    # Create results directory if it doesn't exist
    mkdir -p "$dir/benchmark_results"

    # Prepare locust command based on benchmark type
    locust_cmd="locust --host=http://localhost \
        --users=$USERS \
        --spawn-rate=$SPAWN_RATE \
        --run-time=${RUN_TIME}s \
        --headless \
        --only-summary"

    # Add tags for read or write benchmarks
    if [[ "$BENCHMARK_TYPE" == "read" ]]; then
        locust_cmd="$locust_cmd --tags read"
    elif [[ "$BENCHMARK_TYPE" == "write" ]]; then
        locust_cmd="$locust_cmd --tags write"
    fi

    # Add CSV output filename based on benchmark type
    timestamp=$(date +%Y%m%d_%H%M%S)
    if [[ "$BENCHMARK_TYPE" == "read" ]]; then
        csv_file="$dir/benchmark_results/read_benchmark_$timestamp"
    elif [[ "$BENCHMARK_TYPE" == "write" ]]; then
        csv_file="$dir/benchmark_results/write_benchmark_$timestamp"
    else
        csv_file="$dir/benchmark_results/mixed_benchmark_${READ_RATIO}_${WRITE_RATIO}_$timestamp"
    fi

    locust_cmd="$locust_cmd --csv=$csv_file -f $dir/locustfile.py"

    # Run the benchmark
    echo "Command: $locust_cmd"
    eval $locust_cmd

    echo "Benchmark in $dir complete!"
    echo "Results saved to $csv_file"
    echo
done

echo "All benchmarks completed successfully!"
