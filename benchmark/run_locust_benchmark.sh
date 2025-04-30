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
WEB_UI_MODE=false # Default to headless mode
WEB_UI_PORT=${WEB_UI_PORT:-8089} # Default web UI port

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
    --web-ui)
      WEB_UI_MODE=true
      shift
      ;;
    --web-ui-port=*)
      WEB_UI_PORT=${1#*=}
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

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if we're running in virtual environment
if [[ -z "$VIRTUAL_ENV" && ! -f "venv/bin/python" ]]; then
    echo -e "${YELLOW}Not running in a virtual environment. Using system Python.${NC}"
    echo -e "${YELLOW}If you encounter package installation issues, run:${NC}"
    echo -e "${GREEN}  ./setup_benchmark_env.sh${NC}"
    echo -e "${YELLOW}Then run:${NC}"
    echo -e "${GREEN}  ./run_benchmark_with_venv.sh${NC}"
    PYTHON_CMD="python3"
    PIP_CMD="pip3"
else
    if [[ -z "$VIRTUAL_ENV" ]]; then
        # Activate virtual environment if not already activated
        echo -e "${YELLOW}Activating virtual environment...${NC}"
        source venv/bin/activate
    fi
    PYTHON_CMD="python"
    PIP_CMD="pip"
    echo -e "${GREEN}Using Python from virtual environment: $(which python)${NC}"
fi

# Check if Python and locust are installed
if ! command -v $PYTHON_CMD &> /dev/null; then
    echo -e "${RED}Python is not installed.${NC}"
    exit 1
fi

if ! $PYTHON_CMD -c "import locust" &> /dev/null; then
    echo -e "${YELLOW}Locust is not installed. Installing...${NC}"
    $PIP_CMD install locust psycopg2-binary geojson
    if ! $PYTHON_CMD -c "import locust" &> /dev/null; then
        echo -e "${RED}Failed to install Locust.${NC}"
        echo -e "${YELLOW}Try setting up a virtual environment:${NC}"
        echo -e "${GREEN}  ./setup_benchmark_env.sh${NC}"
        exit 1
    fi
fi

# Print configuration
echo -e "${GREEN}=== Locust PostgreSQL Benchmark ===${NC}"
echo -e "Database: ${DB_HOST}:${DB_PORT}/${DB_NAME}"
if [ -n "$OPERATION_MODE" ]; then
  echo -e "Mode: ${OPERATION_MODE}"
else
  echo -e "Mode: mixed (${READ_WEIGHT}% read, ${WRITE_WEIGHT}% write)"
fi
echo -e "Users: ${USERS}, Spawn rate: ${SPAWN_RATE}, Run time: ${RUN_TIME}s"
if [ "$WEB_UI_MODE" = true ]; then
  echo -e "Web UI: Enabled on port ${WEB_UI_PORT}"
else
  echo -e "Web UI: Disabled (running in headless mode)"
fi

# Check if the database is accessible
echo -e "${YELLOW}Checking database connection...${NC}"
if ! $PYTHON_CMD -c "import psycopg2; conn=psycopg2.connect(host='${DB_HOST}', port=${DB_PORT}, user='${DB_USER}', password='${DB_PASSWORD}', dbname='${DB_NAME}'); conn.close()" 2>/dev/null; then
    echo -e "${RED}Failed to connect to the database. Trying through Docker...${NC}"

    # Try to connect through Docker
    if ! docker exec -i ${CONTAINER} psql -h ${DB_HOST} -p ${DB_PORT} -U ${DB_USER} -c "SELECT 1" ${DB_NAME} &>/dev/null; then
        echo -e "${RED}Failed to connect to the database.${NC}"
        exit 1
    else
        echo -e "${GREEN}Connected to database through Docker.${NC}"
        # We'll need to run Locust from within Docker
        echo -e "${YELLOW}Installing requirements in Docker container...${NC}"
        docker exec -i ${CONTAINER} bash -c "pip3 install locust psycopg2-binary geojson || apt-get update && apt-get install -y python3-pip && pip3 install locust psycopg2-binary geojson"

        # Copy the locust file to the container
        echo -e "${YELLOW}Copying locust file to container...${NC}"
        docker cp locust_benchmark.py ${CONTAINER}:/locust_benchmark.py

        # Run locust from within the container with or without web UI
        echo -e "${GREEN}Running Locust from within Docker container...${NC}"
        if [ "$WEB_UI_MODE" = true ]; then
          echo -e "${YELLOW}Web UI will be available at http://localhost:${WEB_UI_PORT}${NC}"
          docker exec -i ${CONTAINER} locust -f /locust_benchmark.py -u ${USERS} -r ${SPAWN_RATE} --web-port ${WEB_UI_PORT} --host ${LOCUST_HOST}
        else
          docker exec -i ${CONTAINER} locust -f /locust_benchmark.py --headless -u ${USERS} -r ${SPAWN_RATE} -t ${RUN_TIME}s --host ${LOCUST_HOST}
        fi
        exit $?
    fi
fi

# Create benchmark_results directory if it doesn't exist
mkdir -p benchmark_results

# Generate a tag for the results file based on the mode
if [ -n "$OPERATION_MODE" ]; then
  RESULTS_TAG="_${OPERATION_MODE}"
else
  RESULTS_TAG="_mixed_${READ_WEIGHT}_${WRITE_WEIGHT}"
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Run the Locust test
echo -e "${GREEN}Starting Locust benchmark...${NC}"
if [ "$WEB_UI_MODE" = true ]; then
  echo -e "${YELLOW}Web UI will be available at http://localhost:${WEB_UI_PORT}${NC}"
  echo -e "${YELLOW}Press Ctrl+C to stop the test when finished${NC}"
  $PYTHON_CMD -m locust -f ${SCRIPT_DIR}/locust_benchmark.py --web-port ${WEB_UI_PORT} --host ${LOCUST_HOST}
else
  # Run in headless mode with specified duration
  $PYTHON_CMD -m locust -f ${SCRIPT_DIR}/locust_benchmark.py --headless -u ${USERS} -r ${SPAWN_RATE} -t ${RUN_TIME}s \
    --host ${LOCUST_HOST} --logfile ${SCRIPT_DIR}/benchmark_results/locust${RESULTS_TAG}.log

  # Print summary
  echo -e "${GREEN}Benchmark complete!${NC}"
  echo -e "Check the results in the benchmark_results directory."
fi
