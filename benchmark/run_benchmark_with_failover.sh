#!/bin/bash
set -e

# This script runs the Locust benchmark and simulates failover on workers at every stage.
# It assumes the stages and durations are defined in benchmark/locust_benchmark.py.

# --- CONFIGURABLE ---
BENCHMARK_SCRIPT="./benchmark/run_locust_benchmark.sh"
STAGES=(
  120  # Overnight baseline
  180  # Morning peak
  180  # Late morning decline
  180  # Lunch spike
  180  # Afternoon steady
  60   # Flash sale – build up
  120  # Flash sale – peak
  60   # Flash sale – post sale
  180  # Evening peak
  60   # Late night wind down – 1
  120  # Late night wind down – 2
)

# Failover simulation settings
FAILOVER_DURATION=10  # seconds to keep worker down before each stage
WORKER_ROTATION=true  # if true, rotate through all workers for failover
WORKER_FAILOVER_LIST=("worker1" "worker2" "worker3" "worker4" "worker5" "worker6")

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

for i in "${!STAGES[@]}"; do
  STAGE_NUM=$((i+1))
  DURATION=${STAGES[$i]}
  echo "[Failover] Stage $STAGE_NUM: Running stage for ${DURATION}s."
  sleep $DURATION

  # Select the worker to fail over based on rotation or fixed worker
  if [ "$WORKER_ROTATION" = true ]; then
    WORKER_INDEX=$((i % ${#WORKER_FAILOVER_LIST[@]}))
    WORKER_TO_FAILOVER=${WORKER_FAILOVER_LIST[$WORKER_INDEX]}
  else
    WORKER_TO_FAILOVER="worker1"
  fi

  # At the end of the stage, stop selected worker for FAILOVER_DURATION seconds
  echo "[Failover] Stage $STAGE_NUM complete. Stopping $WORKER_TO_FAILOVER for $FAILOVER_DURATION seconds."
  docker stop citus_$WORKER_TO_FAILOVER
  sleep $FAILOVER_DURATION
  docker start citus_$WORKER_TO_FAILOVER
  echo "[Failover] $WORKER_TO_FAILOVER restarted after failover at end of stage $STAGE_NUM."

  # If last stage, just finish
  if [ $((i+1)) -eq ${#STAGES[@]}]; then
    break
  fi
done

echo "[Failover] All stages complete. Waiting for benchmark to finish."

echo "[Failover] Benchmark and failover scenario complete."
