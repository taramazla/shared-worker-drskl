#!/bin/bash

# Script to run benchmark with worker node failover scenarios
# This focuses only on worker node failures, not coordinator failures

set -e

# Default values
BENCHMARK_DIR="benchmark-1"
FAILURE_TYPE="worker1" # Options: worker1, worker2, worker3, worker4
FAILURE_DELAY=15 # Seconds to wait before triggering failure
FAILURE_DURATION=30 # Seconds to keep node down
BENCHMARK_TYPE="mixed" # Options: read, write, mixed
USERS=100
SPAWN_RATE=10
RUN_TIME=120 # Needs to be greater than FAILURE_DELAY + FAILURE_DURATION
DB_HOST="localhost"
DB_PORT="5432"

# Colors for better readability
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print usage information
print_usage() {
    echo -e "${BLUE}Usage:${NC} $0 [OPTIONS]"
    echo "Run benchmarks with simulated worker node failure scenarios"
    echo
    echo -e "${BLUE}Options:${NC}"
    echo "  -d, --dir DIR           Benchmark directory to use (default: $BENCHMARK_DIR)"
    echo "  -f, --failure-type TYPE Worker node to fail (default: $FAILURE_TYPE)"
    echo "                          Options: worker1, worker2, worker3, worker4"
    echo "  --delay N               Seconds to wait before failure (default: $FAILURE_DELAY)"
    echo "  --duration N            Seconds to keep node down (default: $FAILURE_DURATION)"
    echo "  -t, --type TYPE         Benchmark type: read, write, or mixed (default: $BENCHMARK_TYPE)"
    echo "  -u, --users N           Number of simulated users (default: $USERS)"
    echo "  -s, --spawn-rate N      User spawn rate per second (default: $SPAWN_RATE)"
    echo "  -r, --runtime N         Runtime in seconds (default: $RUN_TIME)"
    echo "  -h, --host HOST         Database host (default: $DB_HOST)"
    echo "  -p, --port PORT         Database port (default: $DB_PORT)"
    echo "  --help                  Display this help message"
    echo
    echo -e "${BLUE}Examples:${NC}"
    echo "  $0 --dir benchmark-1 --failure-type worker1 --delay 15 --duration 30"
    echo "  $0 --dir benchmark-2 --failure-type worker2 --type read --runtime 180"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -d|--dir)
            BENCHMARK_DIR="$2"
            shift 2
            ;;
        -f|--failure-type)
            FAILURE_TYPE="$2"
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

# Validate benchmark directory
if [[ ! "$BENCHMARK_DIR" =~ ^benchmark-[1-3]$ && "$BENCHMARK_DIR" != "benchmark-1" && "$BENCHMARK_DIR" != "benchmark-2" && "$BENCHMARK_DIR" != "benchmark-3" ]]; then
    echo -e "${RED}Error:${NC} Invalid benchmark directory. Must be one of: benchmark-1, benchmark-2, benchmark-3"
    exit 1
fi

# Validate failure type - only allow worker nodes
VALID_FAILURE_TYPES=("worker1" "worker2" "worker3" "worker4")
VALID_TYPE=false
for type in "${VALID_FAILURE_TYPES[@]}"; do
    if [[ "$FAILURE_TYPE" == "$type" ]]; then
        VALID_TYPE=true
        break
    fi
done

if [[ "$VALID_TYPE" == "false" ]]; then
    echo -e "${RED}Error:${NC} Invalid failure type. Must be one of: ${VALID_FAILURE_TYPES[*]}"
    exit 1
fi

# Validate benchmark type
if [[ "$BENCHMARK_TYPE" != "read" && "$BENCHMARK_TYPE" != "write" && "$BENCHMARK_TYPE" != "mixed" ]]; then
    echo -e "${RED}Error:${NC} Invalid benchmark type. Must be one of: read, write, mixed"
    exit 1
fi

# Validate timing
if [[ "$RUN_TIME" -le $(( FAILURE_DELAY + FAILURE_DURATION )) ]]; then
    echo -e "${RED}Error:${NC} Runtime must be greater than failure delay + failure duration"
    echo -e "Current settings: Runtime=$RUN_TIME, Delay=$FAILURE_DELAY, Duration=$FAILURE_DURATION"
    exit 1
fi

# Check if docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error:${NC} Docker is required but could not be found"
    exit 1
fi

# Get the absolute path to the benchmark directory
BENCHMARK_DIR="../$BENCHMARK_DIR"
if [[ ! -d "$BENCHMARK_DIR" ]]; then
    echo -e "${RED}Error:${NC} Directory $BENCHMARK_DIR not found"
    exit 1
fi

# Get the container name based on failure type
get_container_name() {
    local failure_type=$1
    case $failure_type in
        worker1)
            echo "citus_worker1"
            ;;
        worker2)
            echo "citus_worker2"
            ;;
        worker3)
            echo "citus_worker3"
            ;;
        worker4)
            echo "citus_worker4"
            ;;
        *)
            echo -e "${RED}Error:${NC} Unknown worker node: $failure_type"
            exit 1
            ;;
    esac
}

# Function to stop a container
stop_container() {
    local container=$1
    echo -e "${YELLOW}Stopping worker container:${NC} $container"
    docker stop $container
    echo -e "${RED}✓ Worker container stopped:${NC} $container"
}

# Function to start a container
start_container() {
    local container=$1
    echo -e "${GREEN}Starting worker container:${NC} $container"
    docker start $container
    echo -e "${GREEN}✓ Worker container started:${NC} $container"

    # Monitor health check status
    echo -e "${BLUE}Monitoring worker health check status...${NC}"
    for i in {1..10}; do
        status=$(docker inspect --format='{{.State.Health.Status}}' $container 2>/dev/null || echo "unknown")

        if [[ "$status" == "healthy" ]]; then
            echo -e "${GREEN}✓ Worker container is healthy:${NC} $container"
            break
        elif [[ "$status" == "unknown" ]]; then
            echo -e "${YELLOW}Worker container has no health check:${NC} $container"
            break
        else
            echo -e "${YELLOW}Waiting for worker container to be healthy:${NC} $container (status: $status)"
            sleep 5
        fi

        if [[ $i -eq 10 && "$status" != "healthy" && "$status" != "unknown" ]]; then
            echo -e "${YELLOW}Warning: Worker container did not become healthy within timeout:${NC} $container"
        fi
    done
}

# Function to check if a container is running
is_container_running() {
    local container=$1
    local status=$(docker inspect --format='{{.State.Status}}' $container 2>/dev/null || echo "not_found")

    if [[ "$status" == "running" ]]; then
        return 0  # True
    else
        return 1  # False
    fi
}

# Source the setup script to ensure the environment is ready
echo -e "${BLUE}Setting up Python virtual environment for benchmarks...${NC}"
source ./setup_benchmark_env.sh

# Container name for failure
CONTAINER_NAME=$(get_container_name $FAILURE_TYPE)

# Check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^$CONTAINER_NAME$"; then
    echo -e "${RED}Error:${NC} Worker container $CONTAINER_NAME does not exist. Make sure the Citus cluster is running."
    exit 1
fi

# Export environment variables for Locust
export DB_HOST
export DB_PORT
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
    export READ_RATIO=80
    export WRITE_RATIO=20
fi

# Create results directory if it doesn't exist
mkdir -p "$BENCHMARK_DIR/benchmark_results"

# Make timestamp for unique file naming
timestamp=$(date +%Y%m%d_%H%M%S)

# Create CSV file name based on benchmark type and failure scenario
if [[ "$BENCHMARK_TYPE" == "read" ]]; then
    csv_file="$BENCHMARK_DIR/benchmark_results/worker_failover_${FAILURE_TYPE}_read_benchmark_$timestamp"
elif [[ "$BENCHMARK_TYPE" == "write" ]]; then
    csv_file="$BENCHMARK_DIR/benchmark_results/worker_failover_${FAILURE_TYPE}_write_benchmark_$timestamp"
else
    csv_file="$BENCHMARK_DIR/benchmark_results/worker_failover_${FAILURE_TYPE}_mixed_benchmark_${READ_RATIO}_${WRITE_RATIO}_$timestamp"
fi

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

# Add CSV output filename
locust_cmd="$locust_cmd --csv=$csv_file -f $BENCHMARK_DIR/locustfile.py"

echo -e "\n${BLUE}=== Starting benchmark with worker node failover scenario ===${NC}"
echo -e "${BLUE}Benchmark directory:${NC} $BENCHMARK_DIR"
echo -e "${BLUE}Benchmark type:${NC} $BENCHMARK_TYPE"
echo -e "${BLUE}Worker node failure:${NC} $FAILURE_TYPE (container: $CONTAINER_NAME)"
echo -e "${BLUE}Failure timing:${NC} After $FAILURE_DELAY seconds, duration $FAILURE_DURATION seconds"
echo -e "${BLUE}Benchmark parameters:${NC} $USERS users, spawn rate $SPAWN_RATE, runtime $RUN_TIME seconds"
if [[ "$BENCHMARK_TYPE" == "mixed" ]]; then
    echo -e "${BLUE}Read/Write ratio:${NC} $READ_RATIO/$WRITE_RATIO"
fi
echo -e "${BLUE}Database connection:${NC} $DB_HOST:$DB_PORT\n"

# Check if container is running
if ! is_container_running $CONTAINER_NAME; then
    echo -e "${YELLOW}Warning:${NC} Worker container $CONTAINER_NAME is not running. Attempting to start it..."
    start_container $CONTAINER_NAME
    # Give it time to fully start
    sleep 10
fi

# Start the benchmark in the background
echo -e "${BLUE}Starting benchmark:${NC} $locust_cmd"
eval $locust_cmd &
LOCUST_PID=$!

# Wait for the specified delay before triggering failure
echo -e "${YELLOW}Waiting $FAILURE_DELAY seconds before triggering worker failure...${NC}"
sleep $FAILURE_DELAY

# Trigger worker failure
echo -e "\n${RED}⚠️ TRIGGERING WORKER NODE FAILURE: $FAILURE_TYPE ${NC}"
stop_container $CONTAINER_NAME

# Wait for the specified duration
echo -e "${YELLOW}Keeping worker $CONTAINER_NAME down for $FAILURE_DURATION seconds...${NC}"
sleep $FAILURE_DURATION

# Restore the worker node
echo -e "\n${GREEN}⚠️ RESTORING WORKER NODE: $FAILURE_TYPE ${NC}"
start_container $CONTAINER_NAME

# Wait for the benchmark to complete
echo -e "${BLUE}Waiting for benchmark to complete...${NC}"
wait $LOCUST_PID

# Create a report file with benchmark and failure details
report_file="${csv_file}_report.txt"
echo -e "Worker Node Failover Benchmark Report" > $report_file
echo -e "=================================\n" >> $report_file
echo -e "Date: $(date)" >> $report_file
echo -e "Benchmark Directory: $BENCHMARK_DIR" >> $report_file
echo -e "Benchmark Type: $BENCHMARK_TYPE" >> $report_file
echo -e "Worker Node Failure: $FAILURE_TYPE (container: $CONTAINER_NAME)" >> $report_file
echo -e "Failure Timing: After $FAILURE_DELAY seconds, duration $FAILURE_DURATION seconds" >> $report_file
echo -e "Benchmark Parameters:" >> $report_file
echo -e "  - Users: $USERS" >> $report_file
echo -e "  - Spawn Rate: $SPAWN_RATE" >> $report_file
echo -e "  - Runtime: $RUN_TIME seconds" >> $report_file
echo -e "  - Database: $DB_HOST:$DB_PORT" >> $report_file
if [[ "$BENCHMARK_TYPE" == "mixed" ]]; then
    echo -e "  - Read/Write Ratio: $READ_RATIO/$WRITE_RATIO" >> $report_file
fi
echo -e "\nResults saved to: ${csv_file}*.csv" >> $report_file

echo -e "\n${GREEN}✓ Worker node failover benchmark completed successfully!${NC}"
echo -e "${GREEN}✓ Results saved to:${NC} ${csv_file}*.csv"
echo -e "${GREEN}✓ Detailed report saved to:${NC} $report_file"
