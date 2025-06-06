#!/bin/bash

# Script to compare different worker node failover scenarios

# Colors for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BENCHMARK_DIRS="benchmark-1"  # Default is benchmark-1, can be comma-separated list
BENCHMARK_TYPE="mixed"
USERS=100
SPAWN_RATE=10
RUN_TIME=180 # Longer runtime to accommodate all the scenarios
FAILURE_DELAY=30
FAILURE_DURATION=60
DB_HOST="localhost"
DB_PORT="5432"

# Function to print usage information
print_usage() {
    echo -e "${BLUE}Usage:${NC} $0 [OPTIONS]"
    echo "Run multiple worker node failover scenarios for comparison"
    echo
    echo -e "${BLUE}Options:${NC}"
    echo "  -d, --dirs DIRS         Comma-separated list of benchmark directories to use (default: $BENCHMARK_DIRS)"
    echo "                          Example: -d benchmark-1,benchmark-3"
    echo "  -t, --type TYPE         Benchmark type: read, write, or mixed (default: $BENCHMARK_TYPE)"
    echo "  -u, --users N           Number of simulated users (default: $USERS)"
    echo "  -s, --spawn-rate N      User spawn rate per second (default: $SPAWN_RATE)"
    echo "  -r, --runtime N         Runtime in seconds (default: $RUN_TIME)"
    echo "  --delay N               Seconds to wait before failure (default: $FAILURE_DELAY)"
    echo "  --duration N            Seconds to keep node down (default: $FAILURE_DURATION)"
    echo "  -h, --host HOST         Database host (default: $DB_HOST)"
    echo "  -p, --port PORT         Database port (default: $DB_PORT)"
    echo "  --help                  Display this help message"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -d|--dirs)
            BENCHMARK_DIRS="$2"
            shift 2
            ;;
        -t|--type)
            BENCHMARK_TYPE="$2"
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
        --delay)
            FAILURE_DELAY="$2"
            shift 2
            ;;
        --duration)
            FAILURE_DURATION="$2"
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

# Change to benchmark directory
cd "$(dirname "$0")"

# Check if worker_failover_benchmark.sh exists
if [ ! -f "./worker_failover_benchmark.sh" ]; then
    echo -e "${RED}Error:${NC} worker_failover_benchmark.sh not found. Make sure it exists in the current directory."
    exit 1
fi

# Make the script executable if it's not already
chmod +x ./worker_failover_benchmark.sh

echo -e "\n${BLUE}=== Running Multiple Worker Failover Scenarios for Comparison ===${NC}"
echo -e "${BLUE}Benchmark directories:${NC} $BENCHMARK_DIRS"
echo -e "${BLUE}Benchmark type:${NC} $BENCHMARK_TYPE"
echo -e "${BLUE}Benchmark parameters:${NC} $USERS users, spawn rate $SPAWN_RATE, runtime $RUN_TIME seconds"
echo -e "${BLUE}Failure timing:${NC} After $FAILURE_DELAY seconds, duration $FAILURE_DURATION seconds\n"

# Create timestamp for report file
timestamp=$(date +%Y%m%d_%H%M%S)
report_dir="benchmark_results"
mkdir -p "$report_dir"
report_file="$report_dir/worker_failover_comparison_${BENCHMARK_TYPE}_$timestamp.md"

# Create comparison report header
cat > "$report_file" << EOF
# Worker Node Failover Comparison Report

- **Date:** $(date)
- **Benchmark Directories:** $BENCHMARK_DIRS
- **Benchmark Type:** $BENCHMARK_TYPE
- **Users:** $USERS
- **Spawn Rate:** $SPAWN_RATE
- **Runtime:** $RUN_TIME seconds
- **Failure Delay:** $FAILURE_DELAY seconds
- **Failure Duration:** $FAILURE_DURATION seconds
- **Database:** $DB_HOST:$DB_PORT

## Scenarios Tested

EOF

# Parse the comma-separated list of benchmark directories
IFS=',' read -ra BENCHMARK_ARRAY <<< "$BENCHMARK_DIRS"

# Run tests for each selected benchmark directory
for benchmark_dir in "${BENCHMARK_ARRAY[@]}"; do
    echo -e "\n${BLUE}=== Testing benchmark directory: $benchmark_dir ===${NC}"

    # Create directory for benchmark results if it doesn't exist
    mkdir -p "../$benchmark_dir/benchmark_results"

    # Add benchmark directory heading to report
    cat >> "$report_file" << EOF

## Benchmark Directory: $benchmark_dir

EOF

    # Run baseline benchmark without failures for comparison
    echo -e "${BLUE}Running baseline benchmark without failures...${NC}"
    baseline_cmd="./run_all_benchmarks.sh -t $BENCHMARK_TYPE -u $USERS -s $SPAWN_RATE -r $RUN_TIME -h $DB_HOST -p $DB_PORT"
    BENCHMARK_DIRS="../$benchmark_dir" eval $baseline_cmd

    # Capture baseline results
    baseline_file=$(ls -t "../$benchmark_dir/benchmark_results/"*"_stats.csv" | head -1)
    if [ -f "$baseline_file" ]; then
        baseline_median=$(awk -F, 'NR>1 {print $6}' "$baseline_file" | sort -n | awk '{count[NR]=$1} END {if (NR%2==1) {print count[(NR+1)/2]} else {print (count[NR/2]+count[NR/2+1])/2}}')
        baseline_failures=$(awk -F, 'NR>1 {sum+=$9} END {print sum}' "$baseline_file")

        # Add to report
        cat >> "$report_file" << EOF
### 1. Baseline (No Failures)
- Median Response Time: ${baseline_median}ms
- Total Failures: $baseline_failures
- Results File: $(basename "$baseline_file")

EOF
    else
        echo -e "${YELLOW}Warning:${NC} Could not find baseline results file"

        # Add to report
        cat >> "$report_file" << EOF
### 1. Baseline (No Failures)
- Results not available

EOF
    fi

    # Array of worker node failure scenarios to test
    WORKER_NODES=("worker1" "worker2" "worker3" "worker4")

    # Run each worker failure scenario
    for i in "${!WORKER_NODES[@]}"; do
        worker="${WORKER_NODES[$i]}"
        scenario_num=$((i+2))

        echo -e "\n${BLUE}Running scenario $scenario_num: $worker failure...${NC}"
        ./worker_failover_benchmark.sh --dir "$benchmark_dir" --failure-type "$worker" \
            --type "$BENCHMARK_TYPE" --users "$USERS" --spawn-rate "$SPAWN_RATE" \
            --runtime "$RUN_TIME" --delay "$FAILURE_DELAY" --duration "$FAILURE_DURATION" \
            --host "$DB_HOST" --port "$DB_PORT"

        # Capture results
        scenario_file=$(ls -t "../$benchmark_dir/benchmark_results/worker_failover_${worker}_"*"_stats.csv" | head -1)
    if [ -f "$scenario_file" ]; then
        scenario_median=$(awk -F, 'NR>1 {print $6}' "$scenario_file" | sort -n | awk '{count[NR]=$1} END {if (NR%2==1) {print count[(NR+1)/2]} else {print (count[NR/2]+count[NR/2+1])/2}}')
        scenario_failures=$(awk -F, 'NR>1 {sum+=$9} END {print sum}' "$scenario_file")

        # Calculate impact percentage compared to baseline
        if [ ! -z "$baseline_median" ] && [ "$baseline_median" != "0" ]; then
            median_impact=$(awk "BEGIN {print (($scenario_median/$baseline_median - 1) * 100)}")
            median_impact=$(printf "%.2f" $median_impact)
        else
            median_impact="N/A"
        fi

        # Add to report
        cat >> "$report_file" << EOF
### $scenario_num. $worker Failure
- Median Response Time: ${scenario_median}ms (${median_impact}% change from baseline)
- Total Failures: $scenario_failures
- Results File: $(basename "$scenario_file")

EOF
    else
        echo -e "${YELLOW}Warning:${NC} Could not find results file for $worker"

        # Add to report
        cat >> "$report_file" << EOF
### $scenario_num. $worker Failure
- Results not available

EOF
    fi

    # Give some time between scenarios for system to stabilize
    echo -e "${YELLOW}Waiting 30 seconds for system to stabilize before next scenario...${NC}"
    sleep 30
done

# Add conclusion to report for this benchmark directory
    echo -e "${GREEN}✓ All worker failover scenarios completed for $benchmark_dir!${NC}"
done

# Add overall conclusion to report
cat >> "$report_file" << EOF
## Conclusion

This report compares the performance impact of different worker node failures on the Citus distributed database system.
The metrics show how each worker failure affects response times and error rates compared to normal operation.

Key observations:
- TBD (Fill in after analyzing results)

## Recommendations

- TBD (Fill in after analyzing results)
EOF

echo -e "\n${GREEN}✓ All benchmark directories tested!${NC}"
echo -e "${GREEN}✓ Comparison report saved to:${NC} $report_file"
