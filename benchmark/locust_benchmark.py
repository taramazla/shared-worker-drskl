#!/usr/bin/env python3
import time
import random
import os
from locust import User, task, between, events
import psycopg2
from psycopg2 import pool
import statistics
import json
from datetime import datetime, timedelta
from locust.exception import StopUser

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "citus")
DB_PASSWORD = os.getenv("DB_PASSWORD", "citus")
DB_NAME = os.getenv("DB_NAME", "citus")
CONTAINER = os.getenv("CONTAINER", "citus_loadbalancer")

# Task weights and settings - can be overridden by environment variables
READ_WEIGHT = int(os.getenv("READ_WEIGHT", "80"))  # Default 80% reads
WRITE_WEIGHT = int(os.getenv("WRITE_WEIGHT", "20"))  # Default 20% writes

# Only calculate SPATIAL_WEIGHT if we're doing reads, but treat as part of READ
SPATIAL_WEIGHT = min(30, READ_WEIGHT) if READ_WEIGHT > 0 else 0

# Print the operation mode for clarity
if READ_WEIGHT > 0 and WRITE_WEIGHT > 0:
    print(f"Running in MIXED mode: {READ_WEIGHT}% read, {WRITE_WEIGHT}% write")
elif READ_WEIGHT > 0:
    print(f"Running in READ-ONLY mode")
elif WRITE_WEIGHT > 0:
    print(f"Running in WRITE-ONLY mode")
else:
    print("WARNING: Both READ_WEIGHT and WRITE_WEIGHT are 0. No operations will run.")

# Connection pooling settings
MIN_CONNECTIONS = 100
MAX_CONNECTIONS = 250

# Store metrics for custom reporting
custom_metrics = {
    "read_latencies": [],
    "write_latencies": [],
    "errors": [],
    "success_count": 0,
    "error_count": 0,
    "start_time": None,
    "read_count": 0,
    "write_count": 0,
    "spatial_count": 0  # Keep this for backward compatibility
}

# Initialize PostgreSQL connection pool
conn_pool = None

# Table existence flag
table_exists = False

def ensure_vehicle_locations_table_exists():
    """Create the vehicle_locations table if it doesn't exist"""
    global table_exists

    conn = None
    try:
        conn = get_connection_pool().getconn()
        with conn.cursor() as cursor:
            # First check if the table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'vehicle_locations'
                );
            """)

            table_exists = cursor.fetchone()[0]

            if not table_exists:
                print("Creating vehicle_locations table...")

                # Create PostGIS extension if needed
                try:
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                    conn.commit()
                    print("PostGIS extension created or already exists")
                except Exception as e:
                    print(f"Warning: Could not create PostGIS extension: {e}")

                # Create the table
                cursor.execute("""
                    CREATE TABLE vehicle_locations (
                      id bigserial,
                      vehicle_id int NOT NULL,
                      location geometry(Point, 4326) NOT NULL,
                      recorded_at timestamptz NOT NULL,
                      region_code text NOT NULL
                    );
                """)

                # Create indexes
                cursor.execute("""
                    CREATE INDEX idx_vehicle_locations_location
                    ON vehicle_locations USING GIST (location);
                """)

                cursor.execute("""
                    CREATE INDEX idx_vehicle_locations_region_code
                    ON vehicle_locations (region_code);
                """)

                # Insert some sample data
                print("Inserting sample data...")
                cursor.execute("""
                    INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code)
                    SELECT
                        (floor(random() * 10000) + 1)::int AS vehicle_id,
                        ST_SetSRID(
                            ST_MakePoint(
                            -74.0 + random() * 0.5,
                            40.7 + random() * 0.5
                            ),
                            4326
                        ) AS location,
                        NOW() - (random() * interval '30 days') AS recorded_at,
                        CASE
                            WHEN random() < 0.33 THEN 'region_north'
                            WHEN random() < 0.66 THEN 'region_south'
                            ELSE 'region_central'
                        END AS region_code
                    FROM generate_series(1, 10000) s(i);
                """)

                conn.commit()
                table_exists = True
                print("Successfully created vehicle_locations table with sample data.")
            else:
                print("vehicle_locations table already exists.")

    except Exception as e:
        print(f"Error creating table: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            get_connection_pool().putconn(conn)

    return table_exists

def get_connection_pool():
    """Create or get the PostgreSQL connection pool"""
    global conn_pool
    if conn_pool is None:
        conn_pool = psycopg2.pool.SimpleConnectionPool(
            MIN_CONNECTIONS,
            MAX_CONNECTIONS,
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
    return conn_pool

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize the connection pool when the test starts"""
    print("Initializing database connection pool...")
    try:
        get_connection_pool()
        print(f"Connection pool established to {DB_HOST}:{DB_PORT}")

        # Ensure vehicle_locations table exists
        if ensure_vehicle_locations_table_exists():
            print("Table setup complete")
        else:
            print("Failed to setup table. Tests may fail.")

        # Record test start time for throughput calculation
        custom_metrics["start_time"] = time.time()

    except Exception as e:
        print(f"Failed to initialize connection pool: {e}")
        environment.process_exit()

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Close all connections in the pool when the test ends"""
    global conn_pool
    if conn_pool:
        conn_pool.closeall()
        print("Connection pool closed")

    # Calculate test duration for throughput
    end_time = time.time()
    if custom_metrics["start_time"] is not None:
        test_duration = end_time - custom_metrics["start_time"]

        # Calculate throughput metrics (operations per second)
        # Count spatial operations as read operations for throughput calculation
        total_read_count = custom_metrics["read_count"] + custom_metrics["spatial_count"]
        total_ops = total_read_count + custom_metrics["write_count"]
        total_throughput = total_ops / test_duration if test_duration > 0 else 0
        read_throughput = total_read_count / test_duration if test_duration > 0 else 0
        write_throughput = custom_metrics["write_count"] / test_duration if test_duration > 0 else 0

        print(f"Overall throughput: {total_throughput:.2f} ops/sec ({total_ops} operations in {test_duration:.2f} seconds)")
        print(f"Read throughput: {read_throughput:.2f} ops/sec ({total_read_count} operations, includes {custom_metrics['spatial_count']} spatial)")
        print(f"Write throughput: {write_throughput:.2f} ops/sec ({custom_metrics['write_count']} operations)")
    else:
        print("Warning: Test start time was not recorded. Cannot calculate throughput.")
        total_throughput = read_throughput = write_throughput = 0
        test_duration = 0
        total_read_count = custom_metrics["read_count"] + custom_metrics["spatial_count"]

    # Generate summary statistics
    if custom_metrics["read_latencies"]:
        avg_read = statistics.mean(custom_metrics["read_latencies"])
        p95_read = statistics.quantiles(custom_metrics["read_latencies"], n=20)[18] if len(custom_metrics["read_latencies"]) >= 20 else avg_read
        print(f"Read latency (including spatial) - Avg: {avg_read:.2f}ms, p95: {p95_read:.2f}ms")

    if custom_metrics["write_latencies"]:
        avg_write = statistics.mean(custom_metrics["write_latencies"])
        p95_write = statistics.quantiles(custom_metrics["write_latencies"], n=20)[18] if len(custom_metrics["write_latencies"]) >= 20 else avg_write
        print(f"Write latency - Avg: {avg_write:.2f}ms, p95: {p95_write:.2f}ms")

    # Calculate success rate
    total = custom_metrics["success_count"] + custom_metrics["error_count"]
    success_rate = (custom_metrics["success_count"] / total * 100) if total > 0 else 0
    print(f"Success rate: {success_rate:.2f}% ({custom_metrics['success_count']}/{total} requests)")

    # Save metrics to file
    results_dir = "benchmark_results"
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    with open(f"{results_dir}/locust_metrics_{timestamp}.json", "w") as f:
        json.dump({
            "read_latency_ms": {
                "avg": statistics.mean(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "min": min(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "max": max(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "p95": statistics.quantiles(custom_metrics["read_latencies"], n=20)[18] if len(custom_metrics["read_latencies"]) > 19 else 0
            },
            "write_latency_ms": {
                "avg": statistics.mean(custom_metrics["write_latencies"]) if custom_metrics["write_latencies"] else 0,
                "min": min(custom_metrics["write_latencies"]) if custom_metrics["write_latencies"] else 0,
                "max": max(custom_metrics["write_latencies"]) if custom_metrics["write_latencies"] else 0,
                "p95": statistics.quantiles(custom_metrics["write_latencies"], n=20)[18] if len(custom_metrics["write_latencies"]) > 19 else 0
            },
            # For backward compatibility, include spatial_latency_ms but point to read metrics
            "spatial_latency_ms": {
                "avg": statistics.mean(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "min": min(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "max": max(custom_metrics["read_latencies"]) if custom_metrics["read_latencies"] else 0,
                "p95": statistics.quantiles(custom_metrics["read_latencies"], n=20)[18] if len(custom_metrics["read_latencies"]) > 19 else 0
            },
            "throughput_ops_sec": {
                "total": total_throughput,
                "read": read_throughput,
                "write": write_throughput,
                "spatial": 0  # Included in read throughput
            },
            "operations": {
                "total": total_ops,
                "read": total_read_count,
                "write": custom_metrics["write_count"],
                "spatial": custom_metrics["spatial_count"]  # Keep for reference
            },
            "success_count": custom_metrics["success_count"],
            "error_count": custom_metrics["error_count"],
            "success_rate": success_rate,
            "timestamp": datetime.now().isoformat()
        }, f, indent=2)
    print(f"Metrics saved to {results_dir}/locust_metrics_{timestamp}.json")

def execute_query(query, params=None, query_type="read"):
    """Execute a database query and measure latency"""
    conn = None
    try:
        # Get a connection from the pool
        conn = conn_pool.getconn()

        # Start timing
        start_time = time.time()

        # Execute query
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch results if it's a read query (including spatial)
            if query_type in ["read", "spatial"]:
                results = cursor.fetchall()
                count = len(results)
            else:  # For write operations
                conn.commit()
                count = cursor.rowcount

        # End timing and calculate latency
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        # Store latency in custom metrics - treat spatial as read
        if query_type == "read" or query_type == "spatial":
            custom_metrics["read_latencies"].append(latency_ms)
            if query_type == "spatial":
                custom_metrics["spatial_count"] += 1
            else:
                custom_metrics["read_count"] += 1
        elif query_type == "write":
            custom_metrics["write_latencies"].append(latency_ms)
            custom_metrics["write_count"] += 1

        custom_metrics["success_count"] += 1

        # Return the result count and latency
        return count, latency_ms

    except Exception as e:
        custom_metrics["errors"].append(str(e))
        custom_metrics["error_count"] += 1
        # Re-raise the exception with more context
        raise Exception(f"Query failed: {e}")

    finally:
        # Return the connection to the pool
        if conn:
            conn_pool.putconn(conn)

class PostgresUser(User):
    """Locust user class for PostgreSQL benchmark"""

    # Wait between 1 and 5 seconds between tasks
    wait_time = between(1, 5)

    def on_start(self):
        """Called when a User starts running"""
        # Check if connection pool is working
        try:
            # Execute a simple query to verify connection
            execute_query("SELECT 1")
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise StopUser()

    # Only include read task if READ_WEIGHT > 0
    if READ_WEIGHT > 0:
        @task(READ_WEIGHT - SPATIAL_WEIGHT)
        def read_operation(self):
            """Perform standard read operations"""
            try:
                # Choose a random read query
                query_type = random.choice([
                    "count_all",
                    "recent_vehicles",
                    "random_vehicle",
                    "region_summary"
                ])

                if query_type == "count_all":
                    count, latency = execute_query("SELECT COUNT(*) FROM vehicle_locations")
                    self.environment.events.request.fire(
                        request_type="Read",
                        name="Count All Vehicles",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif query_type == "recent_vehicles":
                    days = random.randint(1, 30)
                    count, latency = execute_query(
                        "SELECT vehicle_id, recorded_at, region_code FROM vehicle_locations " +
                        "WHERE recorded_at > %s ORDER BY recorded_at DESC LIMIT 100",
                        (datetime.now() - timedelta(days=days),)
                    )
                    self.environment.events.request.fire(
                        request_type="Read",
                        name="Recent Vehicles",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif query_type == "random_vehicle":
                    count, latency = execute_query(
                        "SELECT * FROM vehicle_locations ORDER BY random() LIMIT 1"
                    )
                    self.environment.events.request.fire(
                        request_type="Read",
                        name="Random Vehicle",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif query_type == "region_summary":
                    count, latency = execute_query(
                        "SELECT region_code, COUNT(*) FROM vehicle_locations GROUP BY region_code"
                    )
                    self.environment.events.request.fire(
                        request_type="Read",
                        name="Region Summary",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

            except Exception as e:
                self.environment.events.request.fire(
                    request_type="Read",
                    name="Read Operation",
                    response_time=0,
                    response_length=0,
                    exception=e
                )

        # Only include spatial task if SPATIAL_WEIGHT > 0
        if SPATIAL_WEIGHT > 0:
            @task(SPATIAL_WEIGHT)
            def spatial_operation(self):
                """Perform spatial read operations"""
                try:
                    # Choose a random spatial query
                    query_type = random.choice([
                        "distance_search",
                        "bounding_box",
                    ])

                    if query_type == "distance_search":
                        # Generate a random point near NYC
                        lat = 40.7 + random.uniform(-0.1, 0.1)
                        lon = -74.0 + random.uniform(-0.1, 0.1)
                        radius = random.randint(1000, 10000)  # 1-10 km radius

                        count, latency = execute_query(
                            "SELECT id, vehicle_id, recorded_at, region_code FROM vehicle_locations " +
                            "WHERE ST_DWithin(location::geography, ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, %s) " +
                            "LIMIT 500",
                            (lon, lat, radius),
                            "spatial"  # Still mark as spatial for counting
                        )
                        self.environment.events.request.fire(
                            request_type="Read",  # Changed from Spatial to Read
                            name="Distance Search",
                            response_time=latency,
                            response_length=count,
                            exception=None
                        )

                    elif query_type == "bounding_box":
                        # Generate a random bounding box near NYC
                        min_lat = 40.7 + random.uniform(-0.1, 0)
                        min_lon = -74.0 + random.uniform(-0.1, 0)
                        max_lat = min_lat + random.uniform(0.05, 0.1)
                        max_lon = min_lon + random.uniform(0.05, 0.1)

                        count, latency = execute_query(
                            "SELECT * FROM vehicle_locations " +
                            "WHERE ST_Within(location, ST_MakeEnvelope(%s, %s, %s, %s, 4326)) " +
                            "LIMIT 500",
                            (min_lon, min_lat, max_lon, max_lat),
                            "spatial"  # Still mark as spatial for counting
                        )
                        self.environment.events.request.fire(
                            request_type="Read",  # Changed from Spatial to Read
                            name="Bounding Box",
                            response_time=latency,
                            response_length=count,
                            exception=None
                        )

                except Exception as e:
                    self.environment.events.request.fire(
                        request_type="Read",  # Changed from Spatial to Read
                        name="Spatial Operation",
                        response_time=0,
                        response_length=0,
                        exception=e
                    )

    # Only include write task if WRITE_WEIGHT > 0
    if WRITE_WEIGHT > 0:
        @task(WRITE_WEIGHT)
        def write_operation(self):
            """Perform write operations"""
            try:
                # Choose a random write operation
                op_type = random.choice([
                    "single_insert",
                    "small_batch",
                    "update_vehicle",
                    "delete_vehicle"
                ])

                if op_type == "single_insert":
                    # Generate a random location near NYC
                    lat = 40.7 + random.uniform(-0.5, 0.5)
                    lon = -74.0 + random.uniform(-0.5, 0.5)
                    vehicle_id = random.randint(1, 10000)
                    region = random.choice(["region_north", "region_south", "region_central"])

                    count, latency = execute_query(
                        "INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code) " +
                        "VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s) RETURNING id",
                        (vehicle_id, lon, lat, datetime.now(), region),
                        "write"
                    )
                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Single Insert",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif op_type == "small_batch":
                    # Prepare batch insert query
                    batch_size = random.randint(5, 20)
                    values_list = []
                    insert_params = []

                    for _ in range(batch_size):
                        lat = 40.7 + random.uniform(-0.5, 0.5)
                        lon = -74.0 + random.uniform(-0.5, 0.5)
                        vehicle_id = random.randint(1, 10000)
                        region = random.choice(["region_north", "region_south", "region_central"])
                        recorded_at = datetime.now() - timedelta(seconds=random.randint(0, 86400))

                        values_list.append(f"(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)")
                        insert_params.extend([vehicle_id, lon, lat, recorded_at, region])

                    # Build and execute the batch insert
                    query = "INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code) VALUES " + ", ".join(values_list)
                    count, latency = execute_query(query, insert_params, "write")

                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Batch Insert",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif op_type == "update_vehicle":
                    # Update the timestamp for a random vehicle
                    vehicle_id = random.randint(1, 10000)
                    count, latency = execute_query(
                        "UPDATE vehicle_locations SET recorded_at = %s WHERE vehicle_id = %s AND id IN (SELECT id FROM vehicle_locations WHERE vehicle_id = %s LIMIT 1)",
                        (datetime.now(), vehicle_id, vehicle_id),
                        "write"
                    )
                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Update Vehicle",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif op_type == "delete_vehicle":
                    # Delete a random vehicle entry created more than 25 days ago
                    count, latency = execute_query(
                        "DELETE FROM vehicle_locations WHERE id IN (SELECT id FROM vehicle_locations WHERE recorded_at < %s ORDER BY random() LIMIT 1)",
                        (datetime.now() - timedelta(days=25),),
                        "write"
                    )
                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Delete Vehicle",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

            except Exception as e:
                self.environment.events.request.fire(
                    request_type="Write",
                    name="Write Operation",
                    response_time=0,
                    response_length=0,
                    exception=e
                )
