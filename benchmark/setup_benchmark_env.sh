#!/bin/bash

# Setup the Python virtual environment for benchmark tests
echo "Setting up Python virtual environment for benchmarks..."

# Create virtual environment if it doesn't exist
if [ ! -d "../env" ]; then
    echo "Creating virtual environment..."
    python3 -m venv ../env
fi

# Activate virtual environment
source ../env/bin/activate

# Install required packages
echo "Installing required Python packages..."
pip install -q --upgrade pip
pip install -q -r ../requirements.txt

# Install required packages for benchmarking
echo "Installing specific required packages..."

# Install psycopg2-binary (PostgreSQL driver)
pip install -q psycopg2-binary

# Install locust (for load testing)
pip install -q locust

echo "Benchmark environment setup complete!"
