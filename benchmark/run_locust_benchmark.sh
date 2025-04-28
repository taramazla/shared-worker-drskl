#!/bin/bash
set -e

# Configuration
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"citus"}
DB_PASSWORD=${DB_PASSWORD:-"citus"}
DB_NAME=${DB_NAME:-"citus"}
CONTAINER=${CONTAINER:-"citus_loadbalancer"}

# Test parameters
USERS=${USERS:-500}
SPAWN_RATE=${SPAWN_RATE:-10}
RUN_TIME=${RUN_TIME:-60}
LOCUST_HOST=${LOCUST_HOST:-"http://localhost:8089"}
READ_WEIGHT=${READ_WEIGHT:-80}  # Default 80% reads
WRITE_WEIGHT=${WRITE_WEIGHT:-20} # Default 20% writes
OPERATION_MODE="" # Empty for mixed mode

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

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
      shift
      ;;
    --users=*)
      USERS=${1#*=}
      shift
      ;;
    --run-time=*)
      RUN_TIME=${1#*=}
      shift
      ;;
    --spawn-rate=*)
      SPAWN_RATE=${1#*=}
      shift
      ;;
    *)
      shift
      ;;
  esac
done

# Export variables for Locust
export DB_HOST DB_PORT DB_USER DB_PASSWORD DB_NAME CONTAINER
export READ_WEIGHT WRITE_WEIGHT

# Function to check dependencies
check_dependencies() {
  echo -e "${YELLOW}Checking dependencies...${NC}"

  # Check for Python
  if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python 3 is not installed.${NC}"
    return 1
  fi

  # Try to import required modules
  if ! python3 -c "import sys; exit(0 if all(m in sys.modules or __import__(m) for m in ['locust', 'psycopg2', 'json']) else 1)" 2>/dev/null; then
    echo -e "${YELLOW}Missing required Python modules.${NC}"
    return 1
  fi

  echo -e "${GREEN}All dependencies satisfied!${NC}"
  return 0
}

# Function to setup environment
setup_environment() {
  # Check if we're in a virtual environment
  if [[ -z "$VIRTUAL_ENV" ]]; then
    if [[ -f "venv/bin/activate" ]]; then
      echo -e "${YELLOW}Activating virtual environment...${NC}"
      source venv/bin/activate
      PYTHON_CMD="python"
      PIP_CMD="pip"
    elif [[ -f "env/bin/activate" ]]; then
      echo -e "${YELLOW}Activating virtual environment...${NC}"
      source env/bin/activate
      PYTHON_CMD="python"
      PIP_CMD="pip"
    else
      echo -e "${YELLOW}No virtual environment found. Using system Python.${NC}"
      PYTHON_CMD="python3"
      PIP_CMD="pip3"
    fi
  else
    echo -e "${GREEN}Already in virtual environment: $VIRTUAL_ENV${NC}"
    PYTHON_CMD="python"
    PIP_CMD="pip"
  fi

  # Install missing packages
  echo -e "${YELLOW}Checking for required packages...${NC}"
  if ! $PYTHON_CMD -c "import locust" &> /dev/null; then
    echo -e "${YELLOW}Installing Locust...${NC}"
    $PIP_CMD install locust
  fi

  if ! $PYTHON_CMD -c "import psycopg2" &> /dev/null; then
    echo -e "${YELLOW}Installing psycopg2...${NC}"
    $PIP_CMD install psycopg2-binary
  fi

  echo -e "${GREEN}Environment is ready!${NC}"
}

# Function to check database connection
check_database() {
  echo -e "${YELLOW}Checking database connection to ${DB_HOST}:${DB_PORT}/${DB_NAME}...${NC}"

  # Try direct connection first
  if $PYTHON_CMD -c "import psycopg2; conn=psycopg2.connect(host='${DB_HOST}', port=${DB_PORT}, user='${DB_USER}', password='${DB_PASSWORD}', dbname='${DB_NAME}'); conn.close()" 2>/dev/null; then
    echo -e "${GREEN}Successfully connected to database directly.${NC}"
    return 0
  fi

  # Try through Docker container if direct connection fails
  echo -e "${YELLOW}Direct connection failed. Trying through Docker container...${NC}"

  if command -v docker &> /dev/null; then
    if docker ps | grep -q ${CONTAINER}; then
      if docker exec -i ${CONTAINER} psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -c "SELECT 1" ${DB_NAME} &>/dev/null; then
        echo -e "${GREEN}Connected to database through Docker container.${NC}"
        DOCKER_MODE=1
        return 0
      fi
    else
      echo -e "${RED}Container ${CONTAINER} not found or not running.${NC}"
    fi
  else
    echo -e "${RED}Docker not available.${NC}"
  fi

  echo -e "${RED}Failed to connect to database.${NC}"
  return 1
}

# Function to prepare results directory
prepare_results_dir() {
  # Create benchmark_results directory if it doesn't exist
  mkdir -p benchmark_results

  # Generate a tag for the results file based on the mode
  if [ -n "$OPERATION_MODE" ]; then
    RESULTS_TAG="_${OPERATION_MODE}"
  else
    RESULTS_TAG="_mixed_${READ_WEIGHT}_${WRITE_WEIGHT}"
  fi

  echo -e "${GREEN}Results will be saved with tag: ${RESULTS_TAG}${NC}"
}

# Function to run the benchmark
run_benchmark() {
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

  echo -e "${GREEN}=== Locust PostgreSQL Benchmark ===${NC}"
  echo -e "Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
  if [ -n "$OPERATION_MODE" ]; then
    echo -e "Mode: ${OPERATION_MODE}"
  else
    echo -e "Mode: mixed (${READ_WEIGHT}% read, ${WRITE_WEIGHT}% write)"
  fi
  echo -e "Users: ${USERS}, Spawn rate: ${SPAWN_RATE}, Run time: ${RUN_TIME}s"

  if [ -n "$DOCKER_MODE" ]; then
    # Run in Docker mode
    echo -e "${YELLOW}Running benchmark through Docker container...${NC}"

    # Copy the benchmark script to the container
    docker cp ${SCRIPT_DIR}/locust_benchmark.py ${CONTAINER}:/locust_benchmark.py

    # Make sure locust is installed in the container
    docker exec -i ${CONTAINER} bash -c "pip install locust psycopg2-binary &>/dev/null || pip3 install locust psycopg2-binary &>/dev/null"

    # Run locust in the container
    docker exec -i ${CONTAINER} locust -f /locust_benchmark.py --headless -u ${USERS} -r ${SPAWN_RATE} -t ${RUN_TIME}s --host ${LOCUST_HOST} --logfile /tmp/locust.log

    # Copy the log file back
    docker cp ${CONTAINER}:/tmp/locust.log ${SCRIPT_DIR}/benchmark_results/locust${RESULTS_TAG}.log
  else
    # Run locally
    echo -e "${GREEN}Starting Locust benchmark...${NC}"
    $PYTHON_CMD -m locust -f ${SCRIPT_DIR}/locust_benchmark.py --headless -u ${USERS} -r ${SPAWN_RATE} -t ${RUN_TIME}s \
      --host ${LOCUST_HOST} --logfile ${SCRIPT_DIR}/benchmark_results/locust${RESULTS_TAG}.log
  fi
}

# Main execution flow
main() {
  # Print header
  echo -e "${GREEN}===============================${NC}"
  echo -e "${GREEN}PostgreSQL/Citus Load Testing${NC}"
  echo -e "${GREEN}===============================${NC}"

  # Setup environment
  setup_environment || { echo -e "${RED}Failed to setup environment. Exiting.${NC}"; exit 1; }

  # Check database connection
  check_database || { echo -e "${RED}Database connection failed. Exiting.${NC}"; exit 1; }

  # Prepare results directory
  prepare_results_dir

  # Run the benchmark
  run_benchmark

  # Print completion message
  echo -e "${GREEN}Benchmark complete!${NC}"
  echo -e "Check the results in the benchmark_results directory."
}

# Run the main function
main
