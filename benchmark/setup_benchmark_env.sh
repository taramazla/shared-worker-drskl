#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

VENV_DIR="venv"

echo -e "${YELLOW}Setting up Python virtual environment for benchmarks...${NC}"

# Check if Python3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Python3 is not installed. Please install Python3 first.${NC}"
    exit 1
fi

# Create virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${YELLOW}Creating virtual environment in $VENV_DIR...${NC}"
    python3 -m venv "$VENV_DIR"
else
    echo -e "${YELLOW}Virtual environment already exists in $VENV_DIR${NC}"
fi

# Activate virtual environment and install dependencies
echo -e "${YELLOW}Installing required packages in virtual environment...${NC}"
source "$VENV_DIR/bin/activate"

# Upgrade pip
pip install --upgrade pip

# Install required packages from requirements.txt
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    # Install core requirements if no file exists
    pip install matplotlib pandas numpy seaborn locust psycopg2-binary geojson
fi

# Verify installation
echo -e "${YELLOW}Verifying installations...${NC}"
pip list | grep -E "matplotlib|pandas|numpy|seaborn|locust|psycopg2|geojson"

echo -e "${GREEN}Virtual environment setup complete!${NC}"
echo -e "${YELLOW}To activate the environment, run:${NC}"
echo -e "  source $VENV_DIR/bin/activate"
echo -e "${YELLOW}To run benchmarks using this environment, use:${NC}"
echo -e "  ./run_benchmark_with_venv.sh"

# Deactivate the virtual environment
deactivate
