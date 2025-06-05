# Distributed PostgreSQL Benchmarking Tools

This directory contains tools for benchmarking the performance of the distributed PostgreSQL cluster.

## Prerequisites

Before running benchmarks, ensure:

1. The Citus cluster is running (`./scripts/run.sh` from project root)
2. Python 3.6+ is installed
3. Setup the benchmark environment: `./setup_benchmark_env.sh`

## Available Benchmarks

### Read-Only Benchmark

Tests read performance with various query patterns:
- Simple product information retrieval
- Complex product searches with filtering and joins

```bash
./read_benchmark.sh
# or with custom parameters
USERS=200 SPAWN_RATE=20 RUN_TIME=120 ./read_benchmark.sh
```

### Write-Only Benchmark

Tests write performance with:
- Stock updates (simulating an e-commerce inventory system)
- Order creation with multiple items

```bash
./write_benchmark.sh
# or with custom parameters
USERS=100 SPAWN_RATE=10 RUN_TIME=60 ./write_benchmark.sh
```

### Mixed Workload Benchmark

Tests a combination of read and write operations with configurable ratios:

```bash
# Default: 80% read, 20% write
./mixed_benchmark.sh

# Custom ratio: 70% read, 30% write
READ_RATIO=70 WRITE_RATIO=30 ./mixed_benchmark.sh

# Custom everything
USERS=250 SPAWN_RATE=25 RUN_TIME=180 READ_RATIO=90 WRITE_RATIO=10 ./mixed_benchmark.sh
```

### Interactive Benchmark with Web UI

For more detailed testing and real-time monitoring, use the Locust web interface:

```bash
./run_locust_benchmark.sh
# or specify a different port
./run_locust_benchmark.sh 8090
```

Then open `http://localhost:8089` (or the port you specified) in your browser.

## Environment Variables

All benchmark scripts accept the following environment variables:

| Variable      | Description                    | Default     |
|---------------|--------------------------------|------------|
| USERS         | Number of simulated users      | 100        |
| SPAWN_RATE    | User spawn rate (users/second) | 10         |
| RUN_TIME      | Test duration (seconds)        | 60         |
| DB_HOST       | Database hostname              | localhost  |
| DB_PORT       | Database port                  | 5432       |
| DB_USER       | Database username              | citus      |
| DB_PASSWORD   | Database password              | citus      |
| DB_NAME       | Database name                  | citus      |
| READ_RATIO    | Read operations percentage     | Varies     |
| WRITE_RATIO   | Write operations percentage    | Varies     |

## Results

All benchmark results are saved in the `benchmark_results/` directory with
timestamps for easy tracking. Results are stored in CSV format for further analysis.

## Example Usage

To run a test simulating high load against the haproxy load balancer:

```bash
DB_HOST=haproxy DB_PORT=5000 USERS=500 SPAWN_RATE=50 RUN_TIME=300 ./mixed_benchmark.sh
```

This runs a 5-minute test with 500 concurrent users connecting through HAProxy.
