#!/bin/bash

# Main script to run Locust benchmark with web UI for interactive testing
# This allows you to manually control the benchmark through the Locust web interface

# Source the setup script to ensure the environment is ready
source ./setup_benchmark_env.sh

# Default values for database connection
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
DB_USER=${DB_USER:-"citus"}
DB_PASSWORD=${DB_PASSWORD:-"citus"}
DB_NAME=${DB_NAME:-"citus"}
READ_RATIO=${READ_RATIO:-80}
WRITE_RATIO=${WRITE_RATIO:-20}
PORT=${PORT:-8089}
BENCHMARK_DIR=${BENCHMARK_DIR:-"benchmark-1"}

# Export environment variables for Locust (required for access in locustfile.py)
export DB_HOST
export DB_PORT
export DB_USER
export DB_PASSWORD
export DB_NAME
export READ_RATIO
export WRITE_RATIO

# Function to print usage information
print_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Run Locust benchmark with web UI for interactive testing"
    echo
    echo "Options:"
    echo "  -d, --dir DIR        Benchmark directory to run (default: $BENCHMARK_DIR)"
    echo "  -p, --port PORT      Port for Locust web interface (default: $PORT)"
    echo "  --help               Display this help message"
    echo
    echo "Environment variables:"
    echo "  READ_RATIO           Read percentage for mixed benchmark (default: $READ_RATIO)"
    echo "  WRITE_RATIO          Write percentage for mixed benchmark (default: $WRITE_RATIO)"
    echo "  DB_HOST, DB_PORT     Database connection settings"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        -d|--dir)
            BENCHMARK_DIR="$2"
            shift 2
            ;;
        -p|--port)
            PORT="$2"
            shift 2
            ;;
        --help)
            print_usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            print_usage
            exit 1
            ;;
    esac
done

# Validate read/write ratio
if [ "$READ_RATIO" -lt 0 ] || [ "$WRITE_RATIO" -lt 0 ] || [ $(($READ_RATIO + $WRITE_RATIO)) -ne 100 ]; then
    echo "Error: READ_RATIO and WRITE_RATIO must be non-negative and sum to 100."
    exit 1
fi

echo "Starting Locust web interface on http://localhost:$PORT"
echo "Benchmark directory: $BENCHMARK_DIR"
echo "Read/Write ratio: $READ_RATIO/$WRITE_RATIO"
echo "Connecting to database at $DB_HOST:$DB_PORT"

# Validate benchmark directory
if [[ ! -d "../$BENCHMARK_DIR" ]]; then
    echo "Error: Benchmark directory '../$BENCHMARK_DIR' does not exist"
    echo "Valid options are: benchmark-1, benchmark-2, benchmark-3"
    exit 1
fi

# Run locust with web UI using the selected benchmark directory
locust --host=http://localhost \
       --web-port=$PORT \
       -f "../$BENCHMARK_DIR/locustfile.py"
