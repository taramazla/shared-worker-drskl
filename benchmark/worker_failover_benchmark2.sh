#!/bin/bash

# Script to run worker node failover tests for benchmark-2

# Change to benchmark directory
cd "$(dirname "$0")"

# Make the worker failover script executable
chmod +x ./worker_failover_benchmark.sh

# Run the worker failover benchmark with worker1 by default
./worker_failover_benchmark.sh --dir benchmark-2 --failure-type worker1 "$@"
