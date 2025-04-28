#!/bin/bash
set -e

# Default parameters
USERS=${USERS:-80}  # Moderate user load for failover testing
SPAWN_RATE=${SPAWN_RATE:-10}
RUN_TIME=${RUN_TIME:-240}  # Extended runtime to allow for multiple failover scenarios
FAILOVER_DELAY=${FAILOVER_DELAY:-30}  # Wait time before triggering failover
RECOVERY_WAIT=${RECOVERY_WAIT:-20}    # Wait time to observe recovery
WORKER_FAILOVER=${WORKER_FAILOVER:-1} # Whether to test worker failover (1) or not (0)
WORKER_FAILOVER_DELAY=${WORKER_FAILOVER_DELAY:-120} # When to trigger worker failover
FAILOVER_TYPE=${FAILOVER_TYPE:-"both"} # Options: coordinator, worker, both, sequence

# Get script directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create a descriptive results directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="benchmark_results/failover_benchmark_${TIMESTAMP}"
mkdir -p $RESULTS_DIR

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}Starting FAILOVER benchmark with $USERS users${NC}"
echo -e "${GREEN}Total duration: $RUN_TIME seconds${NC}"
echo -e "${YELLOW}Failover type: $FAILOVER_TYPE${NC}"

if [[ "$FAILOVER_TYPE" == "coordinator" || "$FAILOVER_TYPE" == "both" || "$FAILOVER_TYPE" == "sequence" ]]; then
    echo -e "${YELLOW}Coordinator failover will be triggered after $FAILOVER_DELAY seconds${NC}"
fi

if [[ "$FAILOVER_TYPE" == "worker" || "$FAILOVER_TYPE" == "both" ]]; then
    echo -e "${YELLOW}Worker failover will be triggered after $WORKER_FAILOVER_DELAY seconds${NC}"
fi

echo -e "${BLUE}============================================${NC}"
echo -e "Results will be saved to $RESULTS_DIR"

# Function to get current primary coordinator container ID
get_primary_container_id() {
  docker ps --filter "name=citus_coordinator_primary" --format "{{.ID}}"
}

# Function to get worker container ID
get_worker_container_id() {
  local worker_num=$1
  docker ps --filter "name=citus_worker${worker_num}" --format "{{.ID}}"
}

# Start Locust benchmark in the background with mixed workload
echo -e "${GREEN}Starting benchmark load in background...${NC}"
USERS=$USERS SPAWN_RATE=$SPAWN_RATE RUN_TIME=$RUN_TIME READ_WEIGHT=70 WRITE_WEIGHT=30 \
  nohup "$SCRIPT_DIR/run_locust_benchmark.sh" > "$RESULTS_DIR/locust_output.log" 2>&1 &

BENCHMARK_PID=$!
echo -e "${GREEN}Benchmark process started with PID: $BENCHMARK_PID${NC}"

# Record start time
START_TIME=$(date +%s)
echo -e "${YELLOW}Benchmark started at $(date)${NC}"
echo "Benchmark start time: $(date)" > "$RESULTS_DIR/events.log"
echo "Failover type: $FAILOVER_TYPE" >> "$RESULTS_DIR/events.log"

# Handle coordinator failover if specified
if [[ "$FAILOVER_TYPE" == "coordinator" || "$FAILOVER_TYPE" == "both" || "$FAILOVER_TYPE" == "sequence" ]]; then
    # Wait for the specified delay before triggering coordinator failover
    echo -e "${YELLOW}Waiting $FAILOVER_DELAY seconds before triggering coordinator failover...${NC}"
    sleep $FAILOVER_DELAY

    # Record pre-failover metrics
    echo -e "${YELLOW}Capturing pre-coordinator-failover metrics...${NC}"
    echo "Pre-coordinator-failover timestamp: $(date)" >> "$RESULTS_DIR/events.log"
    PRIMARY_ID=$(get_primary_container_id)
    echo "Primary coordinator container ID: $PRIMARY_ID" >> "$RESULTS_DIR/events.log"

    # Execute failover by stopping the primary coordinator
    echo -e "${RED}Triggering coordinator failover by stopping primary coordinator...${NC}"
    COORD_FAILOVER_TIME=$(date +%s)
    echo "Coordinator failover triggered at $(date), timestamp: $COORD_FAILOVER_TIME" >> "$RESULTS_DIR/events.log"

    # Stop the primary coordinator to simulate failure
    docker stop citus_coordinator_primary
    echo -e "${RED}Primary coordinator stopped. Testing HAProxy failover...${NC}"

    # Record failover time in the results
    FAILOVER_SECONDS=$(($(date +%s) - START_TIME))
    echo "Coordinator failover occurred after $FAILOVER_SECONDS seconds of test start" >> "$RESULTS_DIR/events.log"

    # Wait for recovery period
    echo -e "${YELLOW}Waiting $RECOVERY_WAIT seconds to observe coordinator recovery behavior...${NC}"
    sleep $RECOVERY_WAIT

    # Restart the primary coordinator to restore HA capability
    echo -e "${GREEN}Restoring primary coordinator...${NC}"
    docker start citus_coordinator_primary
    COORD_RECOVERY_TIME=$(date +%s)
    echo "Primary coordinator restored at $(date), timestamp: $COORD_RECOVERY_TIME" >> "$RESULTS_DIR/events.log"
    echo "Coordinator recovery delay: $((COORD_RECOVERY_TIME - COORD_FAILOVER_TIME)) seconds" >> "$RESULTS_DIR/events.log"
fi

# If we're doing a sequence, add a delay between coordinator and worker failover
if [[ "$FAILOVER_TYPE" == "sequence" ]]; then
    SEQUENCE_DELAY=30
    echo -e "${YELLOW}Waiting $SEQUENCE_DELAY seconds between coordinator and worker failover...${NC}"
    echo "Sequence delay between failovers: $SEQUENCE_DELAY seconds" >> "$RESULTS_DIR/events.log"
    sleep $SEQUENCE_DELAY
fi

# Handle worker failover if specified
if [[ "$FAILOVER_TYPE" == "worker" || "$FAILOVER_TYPE" == "both" || "$FAILOVER_TYPE" == "sequence" ]]; then
    # If we're testing worker only, we need to wait for the initial delay
    if [[ "$FAILOVER_TYPE" == "worker" ]]; then
        echo -e "${YELLOW}Waiting $WORKER_FAILOVER_DELAY seconds before triggering worker failover...${NC}"
        sleep $WORKER_FAILOVER_DELAY
    fi

    # Select the worker to fail (worker1 in this case, but could be randomized)
    WORKER_TO_FAIL=1

    # Record pre-worker-failover metrics
    echo -e "${YELLOW}Capturing pre-worker-failover metrics...${NC}"
    echo "Pre-worker-failover timestamp: $(date)" >> "$RESULTS_DIR/events.log"
    WORKER_ID=$(get_worker_container_id $WORKER_TO_FAIL)
    echo "Worker${WORKER_TO_FAIL} container ID: $WORKER_ID" >> "$RESULTS_DIR/events.log"

    # Execute worker failover by stopping the selected worker
    echo -e "${RED}Triggering worker failover by stopping worker${WORKER_TO_FAIL}...${NC}"
    WORKER_FAILOVER_TIME=$(date +%s)
    echo "Worker${WORKER_TO_FAIL} failover triggered at $(date), timestamp: $WORKER_FAILOVER_TIME" >> "$RESULTS_DIR/events.log"

    # Stop the worker to simulate failure
    docker stop citus_worker${WORKER_TO_FAIL}
    echo -e "${RED}Worker${WORKER_TO_FAIL} stopped. Testing shard replication failover...${NC}"

    # Record worker failover time in the results
    WORKER_FAILOVER_SECONDS=$(($(date +%s) - START_TIME))
    echo "Worker failover occurred after $WORKER_FAILOVER_SECONDS seconds of test start" >> "$RESULTS_DIR/events.log"

    # Wait for recovery period
    echo -e "${YELLOW}Waiting $RECOVERY_WAIT seconds to observe worker recovery behavior...${NC}"
    sleep $RECOVERY_WAIT

    # Restart the worker to restore full system capacity
    echo -e "${GREEN}Restoring worker${WORKER_TO_FAIL}...${NC}"
    docker start citus_worker${WORKER_TO_FAIL}
    WORKER_RECOVERY_TIME=$(date +%s)
    echo "Worker${WORKER_TO_FAIL} restored at $(date), timestamp: $WORKER_RECOVERY_TIME" >> "$RESULTS_DIR/events.log"
    echo "Worker recovery delay: $((WORKER_RECOVERY_TIME - WORKER_FAILOVER_TIME)) seconds" >> "$RESULTS_DIR/events.log"
fi

# Wait for the benchmark to complete
echo -e "${YELLOW}Waiting for benchmark to complete...${NC}"
wait $BENCHMARK_PID || true

# Copy the results to the specific directory
echo -e "${GREEN}Collecting benchmark results...${NC}"
cp benchmark_results/locust_mixed_70_30.log $RESULTS_DIR/ 2>/dev/null || true
cp benchmark_results/locust_metrics_*.json $RESULTS_DIR/ 2>/dev/null || true

# Add metadata about the test to the events log
echo "Test configuration:" >> "$RESULTS_DIR/events.log"
echo "- Users: $USERS" >> "$RESULTS_DIR/events.log"
echo "- Spawn rate: $SPAWN_RATE" >> "$RESULTS_DIR/events.log"
echo "- Run time: $RUN_TIME seconds" >> "$RESULTS_DIR/events.log"
echo "- Failover type: $FAILOVER_TYPE" >> "$RESULTS_DIR/events.log"
if [[ "$FAILOVER_TYPE" == "coordinator" || "$FAILOVER_TYPE" == "both" || "$FAILOVER_TYPE" == "sequence" ]]; then
    echo "- Coordinator failover delay: $FAILOVER_DELAY seconds" >> "$RESULTS_DIR/events.log"
fi
if [[ "$FAILOVER_TYPE" == "worker" || "$FAILOVER_TYPE" == "both" || "$FAILOVER_TYPE" == "sequence" ]]; then
    echo "- Worker failover delay: $WORKER_FAILOVER_DELAY seconds" >> "$RESULTS_DIR/events.log"
    echo "- Worker failed: worker${WORKER_TO_FAIL}" >> "$RESULTS_DIR/events.log"
fi
echo "- Recovery wait: $RECOVERY_WAIT seconds" >> "$RESULTS_DIR/events.log"

# Generate a report summary
echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}Failover benchmark complete!${NC}"
echo -e "${YELLOW}Results saved to $RESULTS_DIR${NC}"
echo -e "${YELLOW}See events.log for detailed failover timeline${NC}"
echo -e "${BLUE}============================================${NC}"

# Run the analysis script if available
if [ -f "$SCRIPT_DIR/analyze_failover_results.py" ]; then
    echo -e "${GREEN}Running automatic analysis of failover results...${NC}"
    python "$SCRIPT_DIR/analyze_failover_results.py" "$RESULTS_DIR"
fi