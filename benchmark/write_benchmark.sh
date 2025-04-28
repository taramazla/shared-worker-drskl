#!/bin/bash
set -e

# Default parameters
USERS=${USERS:-50}  # Default to fewer users for write benchmarks
SPAWN_RATE=${SPAWN_RATE:-5}  # Slower spawn rate for writes
RUN_TIME=${RUN_TIME:-60}

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Create a descriptive results directory
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RESULTS_DIR="benchmark_results/write_benchmark_${TIMESTAMP}"
mkdir -p $RESULTS_DIR

echo -e "${GREEN}===============================${NC}"
echo -e "${GREEN}Starting WRITE-ONLY benchmark${NC}"
echo -e "${GREEN}===============================${NC}"
echo -e "Users: ${USERS}, Spawn rate: ${SPAWN_RATE}, Duration: ${RUN_TIME}s"
echo -e "Results will be saved to ${RESULTS_DIR}"

# Run the benchmark with write-only flag
USERS=$USERS SPAWN_RATE=$SPAWN_RATE RUN_TIME=$RUN_TIME "$SCRIPT_DIR/run_locust_benchmark.sh" --write-only

# Find the most recent metrics file and log file
METRICS_FILE=$(ls -t benchmark_results/locust_metrics_*.json 2>/dev/null | head -1)
LOG_FILE=$(ls -t benchmark_results/locust_write-only.log 2>/dev/null | head -1)

# Check if files exist
if [ -z "$METRICS_FILE" ]; then
    echo -e "${RED}No metrics file found. The benchmark may have failed.${NC}"
    exit 1
fi

if [ -z "$LOG_FILE" ]; then
    echo -e "${YELLOW}No log file found. Will continue without log data.${NC}"
fi

# Copy the results to the specific directory
echo -e "${GREEN}Copying results to benchmark directory...${NC}"
[ -n "$METRICS_FILE" ] && cp "$METRICS_FILE" "$RESULTS_DIR/"
[ -n "$LOG_FILE" ] && cp "$LOG_FILE" "$RESULTS_DIR/"

# Generate a summary report
echo -e "${GREEN}Benchmark complete! Results saved to $RESULTS_DIR${NC}"
echo -e "${GREEN}Summary of throughput and latency metrics:${NC}"
echo -e "${GREEN}----------------------------------------${NC}"

# Extract metrics from the JSON file
if [ -n "$METRICS_FILE" ]; then
    # Check if jq is available
    if command -v jq &> /dev/null; then
        WRITE_THROUGHPUT=$(jq '.throughput_ops_sec.write' "$METRICS_FILE")
        AVG_LATENCY=$(jq '.write_latency_ms.avg' "$METRICS_FILE")
        P95_LATENCY=$(jq '.write_latency_ms.p95' "$METRICS_FILE")
        SUCCESS_RATE=$(jq '.success_rate' "$METRICS_FILE")
        TOTAL_OPS=$(jq '.operations.total' "$METRICS_FILE")

        echo -e "Write Throughput: ${GREEN}${WRITE_THROUGHPUT}${NC} ops/sec"
        echo -e "Avg Latency: ${GREEN}${AVG_LATENCY}${NC} ms"
        echo -e "P95 Latency: ${YELLOW}${P95_LATENCY}${NC} ms"
        echo -e "Success Rate: ${GREEN}${SUCCESS_RATE}%${NC}"
        echo -e "Total Operations: ${GREEN}${TOTAL_OPS}${NC}"
    else
        # If jq is not available, print the file path
        echo -e "${YELLOW}jq tool not available. To see detailed metrics: ${NC}"
        echo -e "  cat $METRICS_FILE"
    fi
else
    echo -e "${RED}No metrics file found to display summary.${NC}"
fi

echo -e "${GREEN}----------------------------------------${NC}"
echo -e "To visualize these results, run:"
echo -e "  python3 ${SCRIPT_DIR}/visualize_benchmarks.py --data-dir=${RESULTS_DIR%/*}"