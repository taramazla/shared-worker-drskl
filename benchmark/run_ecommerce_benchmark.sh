#!/bin/bash

# Run e-commerce stock benchmark with different concurrency control settings

# Set default values
USER_COUNT=50
TEST_DURATION=30
SPAWN_RATE=10

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}E-Commerce Stock Update Benchmark${NC}"
echo "This script demonstrates the importance of proper concurrency control"
echo "in e-commerce systems by simulating multiple users trying to purchase the same product."

# Check if locust is installed
if ! command -v locust &> /dev/null; then
    echo -e "${RED}Error: Locust is not installed. Please install it with 'pip install locust'${NC}"
    exit 1
fi

# Function to run benchmark with specific settings
run_benchmark() {
    local isolation=$1
    local isolation_level=$2
    local description=$3

    echo -e "\n${YELLOW}======================================${NC}"
    echo -e "${YELLOW}Running benchmark: $description${NC}"
    echo -e "${YELLOW}======================================${NC}"

    # Set environment variables
    export USE_TRANSACTION_ISOLATION=$isolation
    export ISOLATION_LEVEL=$isolation_level
    export USER_COUNT=$USER_COUNT
    export TEST_DURATION=$TEST_DURATION
    export SPAWN_RATE=$SPAWN_RATE

    # Run the benchmark
    echo "Starting Locust in headless mode..."
    locust -f benchmark/ecommerce_stock_benchmark.py --headless -u $USER_COUNT -r $SPAWN_RATE --run-time ${TEST_DURATION}s --host=http://localhost

    echo -e "\n${GREEN}Benchmark complete!${NC}"
    echo -e "${YELLOW}Check the benchmark_results directory for detailed metrics.${NC}\n"
}

# Show configuration
echo -e "\n${YELLOW}Configuration:${NC}"
echo "Virtual Users: $USER_COUNT"
echo "Test Duration: $TEST_DURATION seconds"
echo "Spawn Rate: $SPAWN_RATE users/sec"

# Ask user which scenario to run
echo -e "\n${YELLOW}Select a benchmark scenario:${NC}"
echo "1) Without proper concurrency control (demonstrates overselling)"
echo "2) With READ COMMITTED isolation level"
echo "3) With REPEATABLE READ isolation level"
echo "4) With SERIALIZABLE isolation level (strongest)"
echo "5) Run all scenarios sequentially"
echo "6) Exit"

read -p "Enter your choice (1-6): " choice

case $choice in
    1)
        run_benchmark "false" "none" "Without proper concurrency control"
        ;;
    2)
        run_benchmark "true" "READ COMMITTED" "With READ COMMITTED isolation level"
        ;;
    3)
        run_benchmark "true" "REPEATABLE READ" "With REPEATABLE READ isolation level"
        ;;
    4)
        run_benchmark "true" "SERIALIZABLE" "With SERIALIZABLE isolation level"
        ;;
    5)
        echo -e "\n${YELLOW}Running all scenarios sequentially...${NC}"
        run_benchmark "false" "none" "Without proper concurrency control"
        run_benchmark "true" "READ COMMITTED" "With READ COMMITTED isolation level"
        run_benchmark "true" "REPEATABLE READ" "With REPEATABLE READ isolation level"
        run_benchmark "true" "SERIALIZABLE" "With SERIALIZABLE isolation level"
        ;;
    6)
        echo "Exiting"
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid choice. Exiting.${NC}"
        exit 1
        ;;
esac

echo -e "${GREEN}All benchmarks completed!${NC}"
echo -e "Check the ${YELLOW}benchmark_results${NC} directory for detailed metrics."
echo -e "You can compare the results to see how different isolation levels affect stock integrity."
