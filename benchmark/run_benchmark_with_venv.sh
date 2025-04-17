#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

VENV_DIR="venv"

# Check if virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}Virtual environment not found. Setting it up...${NC}"
    ./setup_benchmark_env.sh
fi

# Activate virtual environment
echo -e "${YELLOW}Activating virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Parse command-line arguments
BENCHMARK_SCRIPT="run_locust_benchmark.sh"
ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --script=*)
        BENCHMARK_SCRIPT="${1#*=}"
        shift
        ;;
        *)
        ARGS+=("$1")
        shift
        ;;
    esac
done

# Run the specified benchmark script with the virtual environment's Python
echo -e "${GREEN}Running benchmark script: $BENCHMARK_SCRIPT${NC}"
PYTHONPATH="$VENV_DIR/bin/python" ./$BENCHMARK_SCRIPT "${ARGS[@]}"

# Deactivate virtual environment when done
deactivate
