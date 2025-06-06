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

You can run Locust with the web interface for interactive testing on a specific benchmark directory:

```bash
# Run interactive test on benchmark-1 (default)
./run_locust_benchmark.sh

# Run interactive test on benchmark-2
./run_locust_benchmark.sh -d benchmark-2

# Run on benchmark-3 with custom port
./run_locust_benchmark.sh -d benchmark-3 -p 8099
```

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

## Multi-Directory Benchmarks

The repository includes multiple benchmark directories (benchmark-1, benchmark-2, benchmark-3) with their own locustfiles. Use these scripts to run benchmarks across these directories:

### Run Benchmarks Across All Directories

```bash
# Run mixed benchmark across benchmark-1, benchmark-2, benchmark-3 directories
./run_all_benchmarks.sh

# Run read-only benchmark with custom parameters
./run_all_benchmarks.sh -t read -u 200 -s 20 -r 120 -h localhost -p 5432

# Run write-only benchmark
./run_all_benchmarks.sh --type write
```

### Run Benchmark for Specific Directory

```bash
# Run benchmark for benchmark-1 directory
./run_benchmark1.sh

# Run benchmark for benchmark-2 directory with custom parameters
./run_benchmark2.sh -t read -u 300 -s 30 -r 180

# Run benchmark for benchmark-3 directory
./run_benchmark3.sh --type write
```

### Run Benchmarks Sequentially for Comparison

Run each benchmark directory one after the other with the same parameters for easy comparison:

```bash
# Run all three benchmarks sequentially with default settings
./run_sequential_benchmarks.sh

# Run all three benchmarks sequentially with custom parameters
./run_sequential_benchmarks.sh -t read -u 200 -s 20 -r 120
```

## Running Benchmarks on Specific Scenarios

You can now run benchmarks on selected benchmark directories:

### Running All Benchmarks on Selected Directories

```bash
# Run on benchmark-1 and benchmark-3
./run_all_benchmarks.sh -d benchmark-1,benchmark-3

# Run read-only benchmark on benchmark-2
./run_all_benchmarks.sh -t read -d benchmark-2
```

### Running Sequential Benchmarks

```bash
# Run mixed benchmarks sequentially on benchmark-1 and benchmark-3
./run_sequential_benchmarks.sh -b benchmark-1,benchmark-3

# Run read benchmarks on all directories
./run_sequential_benchmarks.sh -t read
```

### Worker Failover Comparison Across Multiple Benchmarks

```bash
# Compare worker failover performance across benchmark-1 and benchmark-3
./compare_worker_failovers.sh -d benchmark-1,benchmark-3 -t mixed
```

## Worker Node Failover Testing

The failover scripts allow testing the system's resilience when worker nodes fail. These tests help measure how the system
handles the loss of a worker node and later recovers when the worker is restored.

### Run Worker Failover Test on Specific Benchmark

```bash
# Run worker1 failover test on benchmark-1 directory
./worker_failover_benchmark1.sh

# Run worker2 failover test on benchmark-2 with custom parameters
./worker_failover_benchmark2.sh --failure-type worker2 -t read -u 200 -s 20 -r 180

# Run worker3 failover test on benchmark-3 with custom delay and duration
./worker_failover_benchmark3.sh --failure-type worker3 --delay 20 --duration 40
```

### Run General Worker Failover Test

```bash
# Run worker failover test with specific parameters
./worker_failover_benchmark.sh --dir benchmark-1 --failure-type worker4 -t mixed -r 180
```

### Compare Worker Failover Scenarios

Compare the impact of different worker node failures on performance:

```bash
# Compare all worker node failures on benchmark-1
./compare_worker_failovers.sh -d benchmark-1 -t mixed -r 240

# Compare with custom parameters
./compare_worker_failovers.sh -d benchmark-2 -t read -u 200 -s 20 --delay 20 --duration 40
```

## Simplified Benchmark Scenario Runner

For the most common use cases, you can use the simplified scenario runner:

```bash
# Run read benchmark on benchmark-1 and benchmark-3
./run_benchmark_scenario.sh --scenario read --dirs benchmark-1,benchmark-3

# Run worker failover test on benchmark-2 with worker2 node
./run_benchmark_scenario.sh --scenario failover --dirs benchmark-2 --worker worker2

# Compare failover performance across multiple directories
./run_benchmark_scenario.sh --scenario compare-failover --dirs benchmark-1,benchmark-3
```

Available scenarios:
- `read`: Run read-only benchmarks
- `write`: Run write-only benchmarks
- `mixed`: Run mixed read/write benchmarks
- `failover`: Run worker failover tests
- `compare-failover`: Compare different worker failover scenarios

Use `--help` for more options.

## Benchmark Script Structure

The benchmark system includes several scripts:

- **run_all_benchmarks.sh**: Main script for running benchmarks on one or more benchmark directories
- **run_sequential_benchmarks.sh**: Runs benchmarks sequentially on selected directories
- **worker_failover_benchmark.sh**: Tests scenarios with worker node failures
- **compare_worker_failovers.sh**: Compares different worker failover scenarios
- **read_benchmark.sh**, **write_benchmark.sh**, **mixed_benchmark.sh**: Run specific benchmark types
- **run_benchmark1.sh**, **run_benchmark2.sh**, **run_benchmark3.sh**: Run benchmarks on specific directories
