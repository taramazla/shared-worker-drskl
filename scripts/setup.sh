#!/bin/bash
set -e

# Set the PostgreSQL password for non-interactive psql commands
export PGPASSWORD=citus

# Function to wait for a host to be ready
wait_for() {
  local host=$1
  local port=$2
  echo "Waiting for $host to be ready on port $port..."
  until pg_isready -h "$host" -p "$port"; do
    echo "Retrying connection to $host..."
    sleep 2
  done
  echo "$host is ready."
}

# Wait for all nodes to be ready
wait_for coordinator_primary 5432
wait_for coordinator_secondary 5432
wait_for worker1 5432
wait_for worker2 5432
wait_for worker3 5432

echo "All nodes are ready."

# Create the Citus extension on the coordinators and workers
echo "Creating Citus extension on primary coordinator..."
psql -h coordinator_primary -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS citus;"

echo "Creating Citus extension on secondary coordinator..."
psql -h coordinator_secondary -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS citus;"

echo "Creating Citus extension on worker1..."
psql -h worker1 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS citus;"

echo "Creating Citus extension on worker2..."
psql -h worker2 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS citus;"

echo "Creating Citus extension on worker3..."
psql -h worker3 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS citus;"

# Create the PostGIS extension on the coordinators and workers
echo "Creating PostGIS extension on primary coordinator..."
psql -h coordinator_primary -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Creating PostGIS extension on secondary coordinator..."
psql -h coordinator_secondary -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Creating PostGIS extension on worker1..."
psql -h worker1 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Creating PostGIS extension on worker2..."
psql -h worker2 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS postgis;"

echo "Creating PostGIS extension on worker3..."
psql -h worker3 -U citus -d citus -c "CREATE EXTENSION IF NOT EXISTS postgis;"

# Set shard replication factor before creating distributed tables
echo "Setting shard replication factor on primary coordinator..."
psql -h coordinator_primary -U citus -d citus -c "ALTER SYSTEM SET citus.shard_replication_factor = 2;"
psql -h coordinator_primary -U citus -d citus -c "SELECT pg_reload_conf();"

echo "Setting shard replication factor on secondary coordinator..."
psql -h coordinator_secondary -U citus -d citus -c "ALTER SYSTEM SET citus.shard_replication_factor = 2;"
psql -h coordinator_secondary -U citus -d citus -c "SELECT pg_reload_conf();"

# Add worker nodes to both coordinators
echo "Adding worker nodes to the primary coordinator..."
psql -h coordinator_primary -U citus -d citus -c "SELECT * FROM citus_add_node('worker1', 5432);"
psql -h coordinator_primary -U citus -d citus -c "SELECT * FROM citus_add_node('worker2', 5432);"
psql -h coordinator_primary -U citus -d citus -c "SELECT * FROM citus_add_node('worker3', 5432);"

echo "Adding worker nodes to the secondary coordinator..."
psql -h coordinator_secondary -U citus -d citus -c "SELECT * FROM citus_add_node('worker1', 5432);"
psql -h coordinator_secondary -U citus -d citus -c "SELECT * FROM citus_add_node('worker2', 5432);"
psql -h coordinator_secondary -U citus -d citus -c "SELECT * FROM citus_add_node('worker3', 5432);"

# Verify replication setup
echo "Verifying replication setup on primary coordinator..."
psql -h coordinator_primary -U citus -d citus -c "SELECT nodename, nodeport, noderack FROM pg_dist_node;"

echo "Verifying replication setup on secondary coordinator..."
psql -h coordinator_secondary -U citus -d citus -c "SELECT nodename, nodeport, noderack FROM pg_dist_node;"

echo "Active-Active Citus cluster with load balancer setup complete."
echo "You can connect to the cluster through the load balancer at localhost:5432"