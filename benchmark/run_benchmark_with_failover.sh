#!/bin/bash
set -e

# This script runs the Locust benchmark and simulates failover on workers.
# It uses a fixed test duration with regular failover events.

# --- CONFIGURABLE ---
BENCHMARK_SCRIPT="./benchmark/run_locust_benchmark.sh"

# Total benchmark duration in seconds (10 minutes by default)
TEST_DURATION=${TEST_DURATION:-600}

# How often to trigger a failover event (every 120 seconds by default)
FAILOVER_INTERVAL=${FAILOVER_INTERVAL:-120}

# Failover simulation settings
FAILOVER_DURATION=10  # seconds to keep worker down before each stage
WORKER_ROTATION=true  # if true, rotate through all workers for failover
WORKER_FAILOVER_LIST=("worker1" "worker2" "worker3" "worker4" "worker5" "worker6")

# By default, maximum concurrent connections will equal the user count
# Setting MAX_CONCURRENT_QUERIES will override this behavior
# MAX_CONCURRENT_QUERIES=${MAX_CONCURRENT_QUERIES}  # Uncomment to set a specific value

# Default values
READ_WEIGHT=50
WRITE_WEIGHT=50
OPERATION_MODE="mixed"
WEB_UI_MODE=false
WEB_UI_PORT=8089

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --read-only)
      READ_WEIGHT=100
      WRITE_WEIGHT=0
      OPERATION_MODE="read-only"
      shift
      ;;
    --write-only)
      READ_WEIGHT=0
      WRITE_WEIGHT=100
      OPERATION_MODE="write-only"
      shift
      ;;
    --read-write-ratio=*)
      RATIO=${1#*=}
      READ_WEIGHT=${RATIO%:*}
      WRITE_WEIGHT=${RATIO#*:}
      OPERATION_MODE="custom-${READ_WEIGHT}-${WRITE_WEIGHT}"
      shift
      ;;
    --web-ui)
      WEB_UI_MODE=true
      shift
      ;;
    --web-ui-port=*)
      WEB_UI_PORT=${1#*=}
      WEB_UI_MODE=true
      shift
      ;;
    --max-connections=*)
      MAX_CONCURRENT_QUERIES=${1#*=}
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# --- END CONFIGURABLE ---

# Build command with arguments
BENCHMARK_CMD="$BENCHMARK_SCRIPT"

# Apply read/write settings
if [ "$OPERATION_MODE" == "read-only" ]; then
  BENCHMARK_CMD="$BENCHMARK_CMD --read-only"
elif [ "$OPERATION_MODE" == "write-only" ]; then
  BENCHMARK_CMD="$BENCHMARK_CMD --write-only"
elif [[ $OPERATION_MODE == custom* ]]; then
  BENCHMARK_CMD="$BENCHMARK_CMD --read-write-ratio=${READ_WEIGHT}:${WRITE_WEIGHT}"
fi

# Apply web UI settings
if [ "$WEB_UI_MODE" = true ]; then
  BENCHMARK_CMD="$BENCHMARK_CMD --web-ui"
  if [ "$WEB_UI_PORT" != "8089" ]; then
    BENCHMARK_CMD="$BENCHMARK_CMD --web-ui-port=$WEB_UI_PORT"
  fi
fi

echo "[Failover] Starting benchmark with operation mode: $OPERATION_MODE (read:$READ_WEIGHT/write:$WRITE_WEIGHT)"

# Start the benchmark in the background with configured settings
$BENCHMARK_CMD &

# Calculate how many failover events we'll have
NUM_FAILOVERS=$((TEST_DURATION / FAILOVER_INTERVAL))

# Start counter
ELAPSED_TIME=0

for i in $(seq 1 $NUM_FAILOVERS); do
  FAILOVER_NUM=$i

  echo "[Failover] Running test interval $FAILOVER_NUM for ${FAILOVER_INTERVAL}s."
  sleep $FAILOVER_INTERVAL
  ELAPSED_TIME=$((ELAPSED_TIME + FAILOVER_INTERVAL))

  # If we've reached the test duration, stop
  if [ $ELAPSED_TIME -ge $TEST_DURATION ]; then
    echo "[Failover] Test duration reached. No more failovers."
    break
  fi

  # Select the worker to fail over based on rotation or fixed worker
  if [ "$WORKER_ROTATION" = true ]; then
    WORKER_INDEX=$(( (i-1) % ${#WORKER_FAILOVER_LIST[@]} ))
    WORKER_TO_FAILOVER=${WORKER_FAILOVER_LIST[$WORKER_INDEX]}
  else
    WORKER_TO_FAILOVER="worker1"
  fi

  # Stop selected worker for FAILOVER_DURATION seconds
  echo "[Failover] Interval $FAILOVER_NUM complete. Stopping $WORKER_TO_FAILOVER for $FAILOVER_DURATION seconds."
  docker stop citus_$WORKER_TO_FAILOVER
  sleep $FAILOVER_DURATION
  docker start citus_$WORKER_TO_FAILOVER
  echo "[Failover] $WORKER_TO_FAILOVER restarted after failover $FAILOVER_NUM."

  ELAPSED_TIME=$((ELAPSED_TIME + FAILOVER_DURATION))
done

echo "[Failover] Test complete. Waiting for benchmark to finish."

echo "[Failover] Benchmark and failover scenario complete."
