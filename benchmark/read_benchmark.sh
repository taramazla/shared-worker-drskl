#!/bin/bash

# Read benchmark script
# Defaults: 100 users, 10 spawn rate, 60 seconds runtime

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
READ_RATIO=100
WRITE_RATIO=0

# Export environment variables for Locust (required for access in locustfile.py)
export DB_HOST
export DB_PORT
export DB_USER
export DB_PASSWORD
export DB_NAME
export READ_RATIO
export WRITE_RATIO

echo "Starting read-only benchmark with $USERS users, spawn rate $SPAWN_RATE, runtime $RUN_TIME seconds"
echo "Connecting to database at $DB_HOST:$DB_PORT"

# Create results directory if it doesn't exist
mkdir -p benchmark_results

# Run locust in headless mode with read-only tag
locust --host=http://localhost \
       --users=$USERS \
       --spawn-rate=$SPAWN_RATE \
       --run-time=${RUN_TIME}s \
       --headless \
       --only-summary \
       --tags read \
       --csv=benchmark_results/read_benchmark_$(date +%Y%m%d_%H%M%S) \
       -f locustfile.py

echo "Benchmark complete! Results saved to benchmark_results/ directory."
