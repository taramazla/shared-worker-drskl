# Distributed PostgreSQL with Citus

This project implements a distributed PostgreSQL cluster using Citus extension with shared worker nodes with primary-secondary replication.. It is designed to provide high availability, scalability, and performance for large datasets and complex queries. The architecture includes a coordinator layer for query routing and a worker layer for data storage and processing. The project also integrates PostGIS for spatial data capabilities and includes benchmarking tools to evaluate performance under various workloads. The project is built using Docker and Docker Compose, making it easy to set up and run in a local or cloud environment.

## Architecture

This project builds a distributed PostgreSQL cluster with:

- **Coordinator Layer**: Primary and secondary coordinators for high availability
- **Worker Layer**: Three worker nodes with logical replication (using shard replication factor)
- **Load Balancer**: HAProxy for connection routing between coordinators
- **PostGIS Integration**: Spatial data capabilities on all nodes
- **Benchmarking Tools**: Locust-based tools for performance testing

## Project Structure

```
/
├── benchmark/                 # Performance benchmark tools
│   ├── locust_benchmark.py             # Locust benchmark script
│   ├── read_benchmark.sh               # Read operation benchmark
│   ├── write_benchmark.sh              # Write operation benchmark
│   ├── run_benchmark_with_venv.sh      # Runner script with virtual env
│   ├── run_locust_benchmark.sh         # Script to run locust benchmarks
│   ├── setup_benchmark_env.sh          # Environment setup for benchmarks
│   └── benchmark_results/              # Directory for benchmark results
│
├── docker/                    # Docker-related files
│   ├── Dockerfile             # Main Dockerfile for PostgreSQL nodes
│   └── Dockerfile.loadbalancer # Dockerfile for HAProxy load balancer
│
├── config/                    # Configuration files
│   ├── haproxy/               # HAProxy configuration
│   │   └── haproxy.cfg        # HAProxy configuration file
│   └── postgres/              # PostgreSQL configuration
│       ├── pg_hba.conf        # PostgreSQL host-based authentication config
│       └── worker-pg_hba.conf # Worker node authentication config
│
├── scripts/                   # Shell scripts
│   ├── coordinator/           # Coordinator scripts
│   │   ├── primary-entrypoint.sh   # Primary coordinator entrypoint
│   │   └── secondary-entrypoint.sh # Secondary coordinator entrypoint
│   ├── run.sh                 # Main script to start the cluster
│   └── setup.sh               # General cluster setup script
│
├── sql/                       # SQL files
│   └── init.sql               # Initial data and schema setup
│
├── docs/                      # Documentation
│   └── shared-worker.png      # Architecture diagram
│
├── env/                       # Python virtual environment
│
├── docker-compose.yml         # Docker Compose definition
└── requirements.txt           # Python dependencies
```

## Getting Started

To run the cluster:

```bash
./scripts/run.sh
```

This will start all the components defined in the docker-compose.yml file.

## Benchmarking

The project includes several benchmark scripts to evaluate performance:

```bash
# Setup benchmark environment
./benchmark/setup_benchmark_env.sh

# Run specific benchmarks
./benchmark/read_benchmark.sh
./benchmark/write_benchmark.sh

# Mixed workload (80% read, 20% write)
READ_RATIO=80 WRITE_RATIO=20 ./mixed_benchmark.sh

# Custom parameters
USERS=200 SPAWN_RATE=20 RUN_TIME=120 ./benchmark/run_locust_benchmark.sh
USERS=200 RUN_TIME=120 ./benchmark/read_benchmark.sh
```

Benchmark results are stored in the `benchmark/benchmark_results/` directory.