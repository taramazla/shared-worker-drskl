#!/bin/bash
set -e

# This script runs the Locust benchmark and simulates failover on worker1 at every stage.
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

FAILOVER_DURATION=10  # seconds to keep worker1 down before each stage

# --- END CONFIGURABLE ---

# Start the benchmark in the background with web UI enabled
$BENCHMARK_SCRIPT --web-ui &

for i in "${!STAGES[@]}"; do
  STAGE_NUM=$((i+1))
  DURATION=${STAGES[$i]}
  echo "[Failover] Stage $STAGE_NUM: Running stage for ${DURATION}s."
  sleep $DURATION
  # At the end of the stage, stop worker1 for FAILOVER_DURATION seconds
  echo "[Failover] Stage $STAGE_NUM complete. Stopping worker1 for $FAILOVER_DURATION seconds."
  docker stop citus_worker1
  sleep $FAILOVER_DURATION
  docker start citus_worker1
  echo "[Failover] worker1 restarted after failover at end of stage $STAGE_NUM."
  # If last stage, just finish
  if [ $((i+1)) -eq ${#STAGES[@]} ]; then
    break
  fi
done

echo "[Failover] All stages complete. Waiting for benchmark to finish."

echo "[Failover] Benchmark and failover scenario complete."
