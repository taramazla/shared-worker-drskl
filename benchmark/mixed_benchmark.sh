#!/bin/bash

# Mixed benchmark script with configurable read/write ratio
# Default: 80% reads, 20% writes

# Source the setup script to ensure the environment is ready
source ./setup_benchmark_env.sh

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

# Export environment variables for Locust (required for access in locustfile.py)
export DB_HOST
export DB_PORT
export DB_USER
export DB_PASSWORD
export DB_NAME
export READ_RATIO
export WRITE_RATIO

# Validate read/write ratio
if [ "$READ_RATIO" -lt 0 ] || [ "$WRITE_RATIO" -lt 0 ] || [ $(($READ_RATIO + $WRITE_RATIO)) -ne 100 ]; then
    echo "Error: READ_RATIO and WRITE_RATIO must be non-negative and sum to 100."
    exit 1
fi

echo "Starting mixed benchmark with $USERS users, spawn rate $SPAWN_RATE, runtime $RUN_TIME seconds"
echo "Read/Write ratio: $READ_RATIO/$WRITE_RATIO"
echo "Connecting to database at $DB_HOST:$DB_PORT"

# Create results directory if it doesn't exist
mkdir -p benchmark_results

# Run locust in headless mode with mixed read/write
locust --host=http://localhost \
       --users=$USERS \
       --spawn-rate=$SPAWN_RATE \
       --run-time=${RUN_TIME}s \
       --headless \
       --only-summary \
       --csv=benchmark_results/mixed_benchmark_${READ_RATIO}_${WRITE_RATIO}_$(date +%Y%m%d_%H%M%S) \
       -f locustfile.py

echo "Benchmark complete! Results saved to benchmark_results/ directory."
