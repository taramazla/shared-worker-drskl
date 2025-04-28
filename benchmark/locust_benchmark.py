#!/usr/bin/env python3
import time
import random
import os
from locust import User, task, between, events, TaskSet
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

# User behavioral patterns - each pattern represents a realistic user workflow
USER_PATTERNS = {
    "data_analyst": {
        "description": "Data analyst who primarily runs read-heavy analytical queries",
        "read_ratio": 90,
        "spatial_ratio": 40,
        "complex_ratio": 50,
        "write_ratio": 10,
        "batch_size": (1, 5),
        "think_time": (2, 8)
    },
    "fleet_manager": {
        "description": "Fleet manager who monitors vehicles and occasionally updates records",
        "read_ratio": 70,
        "spatial_ratio": 50,
        "complex_ratio": 10,
        "write_ratio": 30,
        "batch_size": (1, 10),
        "think_time": (1, 3)
    },
    "data_ingestion": {
        "description": "Automated system that primarily writes data with occasional reads",
        "read_ratio": 20,
        "spatial_ratio": 10,
        "complex_ratio": 5,
        "write_ratio": 80,
        "batch_size": (50, 200),
        "think_time": (0.1, 0.5)
    }
}

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
        try:
            # Adjust connections based on expected users/load
            min_conn = min(MIN_CONNECTIONS, os.cpu_count() * 5 if os.cpu_count() else 50)
            max_conn = max(MAX_CONNECTIONS, min_conn * 2)

            print(f"Initializing connection pool with {min_conn}-{max_conn} connections")
            conn_pool = pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )

            # Test a connection from the pool
            test_conn = conn_pool.getconn()
            test_conn.set_session(autocommit=True)
            test_cursor = test_conn.cursor()
            test_cursor.execute("SELECT 1")
            test_cursor.close()
            conn_pool.putconn(test_conn)

            print(f"Successfully initialized connection pool to {DB_HOST}:{DB_PORT}/{DB_NAME}")
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            # Don't raise here, as we'll handle the None pool case in execute_query
            conn_pool = None
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
    """Execute a database query and measure latency with retries and better error handling"""
    conn = None
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        try:
            if not conn_pool:
                # If pool doesn't exist yet, try to create it
                pool = get_connection_pool()
                if not pool:
                    raise Exception("Connection pool initialization failed")

            # Get a connection from the pool
            conn = conn_pool.getconn()
            conn.set_session(autocommit=True if query_type == "write" else False)

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
                    if query_type == "read" and not conn.autocommit:
                        conn.commit()
                else:  # For write operations
                    count = cursor.rowcount
                    if not conn.autocommit:
                        conn.commit()

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

        except psycopg2.OperationalError as e:
            # This is a connection error, retry
            retry_count += 1
            error_msg = str(e).strip()
            custom_metrics["errors"].append(f"Connection error (attempt {retry_count}): {error_msg}")

            if retry_count >= max_retries:
                custom_metrics["error_count"] += 1
                return 0, 0

            # Wait before retrying with exponential backoff
            time.sleep(0.5 * (2 ** (retry_count - 1)))

        except Exception as e:
            # Other errors - log but don't retry
            custom_metrics["errors"].append(str(e))
            custom_metrics["error_count"] += 1
            return 0, 0

        finally:
            # Return the connection to the pool
            if conn and conn_pool:
                try:
                    conn_pool.putconn(conn)
                except Exception:
                    # If we can't return the connection, just close it
                    try:
                        conn.close()
                    except:
                        pass

# Base User class for basic operations
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

# Task sets for user behavioral patterns
class DataAnalystTasks(TaskSet):
    """Task set simulating data analyst behavior - heavy on analytics queries"""

    # Longer think time between operations to simulate analysis
    wait_time = between(2, 8)

    @task(50)
    def run_complex_analysis(self):
        """Run complex analytical queries"""
        try:
            query = """
                SELECT
                    region_code,
                    COUNT(*) as vehicle_count,
                    AVG(ST_X(location)) as avg_longitude,
                    AVG(ST_Y(location)) as avg_latitude,
                    MIN(recorded_at) as earliest_record,
                    MAX(recorded_at) as latest_record
                FROM vehicle_locations
                GROUP BY region_code
                ORDER BY vehicle_count DESC;
            """
            count, latency = execute_query(query, query_type="read")
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Complex Region Analysis",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Complex Region Analysis",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(40)
    def run_spatial_analysis(self):
        """Run spatial analysis to find clusters"""
        try:
            # Center point approximating Manhattan
            center_lat = 40.7580
            center_lon = -73.9855

            # Create random radius between 1-8km
            radius = random.randint(1000, 8000)

            query = """
                WITH vehicles_in_area AS (
                    SELECT
                        id,
                        vehicle_id,
                        location,
                        recorded_at
                    FROM
                        vehicle_locations
                    WHERE
                        ST_DWithin(
                            location::geography,
                            ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                            %s
                        )
                )
                SELECT
                    COUNT(*) as total_vehicles,
                    MIN(recorded_at) as earliest_record,
                    MAX(recorded_at) as latest_record
                FROM
                    vehicles_in_area;
            """

            count, latency = execute_query(
                query,
                params=(center_lon, center_lat, radius),
                query_type="spatial"
            )

            self.user.environment.events.request.fire(
                request_type="Read",
                name="Spatial Density Analysis",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Spatial Density Analysis",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(10)
    def create_summary_report(self):
        """Create a summary report with occasional data insertion"""
        try:
            # First get a summary
            query = """
                SELECT
                    DATE_TRUNC('day', recorded_at) as day,
                    region_code,
                    COUNT(*) as daily_count
                FROM
                    vehicle_locations
                WHERE
                    recorded_at > %s
                GROUP BY
                    DATE_TRUNC('day', recorded_at), region_code
                ORDER BY
                    day DESC, daily_count DESC
                LIMIT 30;
            """

            # Look back about a week
            lookback_days = random.randint(3, 10)
            count, latency = execute_query(
                query,
                params=(datetime.now() - timedelta(days=lookback_days),),
                query_type="read"
            )

            self.user.environment.events.request.fire(
                request_type="Read",
                name="Daily Summary Report",
                response_time=latency,
                response_length=count,
                exception=None
            )

            # Occasionally store a result (simulating saving a report)
            if random.random() < 0.2:  # 20% chance to perform a write after analysis
                # Create a simulated summary record
                summary_insert = """
                    INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code)
                    VALUES (-999, ST_SetSRID(ST_MakePoint(-74.0, 40.7), 4326), %s, 'summary_report')
                    RETURNING id
                """

                w_count, w_latency = execute_query(
                    summary_insert,
                    params=(datetime.now(),),
                    query_type="write"
                )

                self.user.environment.events.request.fire(
                    request_type="Write",
                    name="Save Analysis Report",
                    response_time=w_latency,
                    response_length=w_count,
                    exception=None
                )

        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Mixed",
                name="Create Summary Report",
                response_time=0,
                response_length=0,
                exception=e
            )

class FleetManagerTasks(TaskSet):
    """Task set simulating fleet manager behavior - monitoring and updates"""

    # Moderate think time to simulate operational work
    wait_time = between(1, 3)

    @task(40)
    def check_vehicle_status(self):
        """Check on status of random vehicles"""
        try:
            # Get a random number of vehicles to check
            vehicle_count = random.randint(3, 15)

            # Generate random vehicle IDs to check (simulate monitoring specific vehicles)
            vehicle_ids = random.sample(range(1, 10000), vehicle_count)
            placeholders = ", ".join(["%s"] * len(vehicle_ids))

            query = f"""
                SELECT
                    vehicle_id,
                    recorded_at,
                    ST_X(location) as longitude,
                    ST_Y(location) as latitude,
                    region_code
                FROM
                    vehicle_locations
                WHERE
                    vehicle_id IN ({placeholders})
                ORDER BY
                    recorded_at DESC
                LIMIT 100
            """

            count, latency = execute_query(
                query,
                params=vehicle_ids,
                query_type="read"
            )

            self.user.environment.events.request.fire(
                request_type="Read",
                name="Vehicle Status Check",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Vehicle Status Check",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(30)
    def monitor_region(self):
        """Monitor vehicles in a specific region with spatial query"""
        try:
            # Select a random region
            region = random.choice(["region_north", "region_south", "region_central"])

            # Create a bounding box - simulating looking at a map area
            center_lat = 40.7 + random.uniform(-0.2, 0.2)
            center_lon = -74.0 + random.uniform(-0.2, 0.2)

            # Size of the box - approximately 1-3 km
            delta = random.uniform(0.01, 0.03)
            min_lat = center_lat - delta
            max_lat = center_lat + delta
            min_lon = center_lon - delta
            max_lon = center_lon + delta

            query = """
                SELECT
                    vehicle_id,
                    recorded_at,
                    ST_X(location) as longitude,
                    ST_Y(location) as latitude
                FROM
                    vehicle_locations
                WHERE
                    region_code = %s AND
                    ST_Within(
                        location,
                        ST_MakeEnvelope(%s, %s, %s, %s, 4326)
                    )
                ORDER BY
                    recorded_at DESC
                LIMIT 200
            """

            count, latency = execute_query(
                query,
                params=(region, min_lon, min_lat, max_lon, max_lat),
                query_type="spatial"
            )

            self.user.environment.events.request.fire(
                request_type="Read",
                name="Region Monitoring",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Region Monitoring",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(30)
    def update_vehicle_locations(self):
        """Update locations of vehicles - simulating fleet management"""
        try:
            # Choose how many vehicles to update
            num_vehicles = random.randint(1, 5)

            for _ in range(num_vehicles):
                # Choose a random vehicle
                vehicle_id = random.randint(1, 10000)

                # Generate new position
                lat = 40.7 + random.uniform(-0.5, 0.5)
                lon = -74.0 + random.uniform(-0.5, 0.5)
                region = random.choice(["region_north", "region_south", "region_central"])

                # Update or insert
                if random.random() < 0.7:  # 70% update, 30% insert
                    query = """
                        UPDATE vehicle_locations
                        SET
                            location = ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                            recorded_at = %s,
                            region_code = %s
                        WHERE
                            vehicle_id = %s AND
                            id IN (SELECT id FROM vehicle_locations WHERE vehicle_id = %s LIMIT 1)
                        RETURNING id
                    """
                    params = (lon, lat, datetime.now(), region, vehicle_id, vehicle_id)
                    op_name = "Update Vehicle Location"
                else:
                    # New vehicle entry
                    query = """
                        INSERT INTO vehicle_locations
                            (vehicle_id, location, recorded_at, region_code)
                        VALUES
                            (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)
                        RETURNING id
                    """
                    params = (vehicle_id, lon, lat, datetime.now(), region)
                    op_name = "Insert Vehicle Location"

                count, latency = execute_query(
                    query,
                    params=params,
                    query_type="write"
                )

                self.user.environment.events.request.fire(
                    request_type="Write",
                    name=op_name,
                    response_time=latency,
                    response_length=count,
                    exception=None
                )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Write",
                name="Update Vehicle Locations",
                response_time=0,
                response_length=0,
                exception=e
            )

class DataIngestionTasks(TaskSet):
    """Task set simulating automated data ingestion - bulk writes with occasional reads"""

    # Very short think time - simulating automated system
    wait_time = between(0.1, 0.5)

    @task(80)
    def bulk_insert_data(self):
        """Insert batches of data - simulating data feeds"""
        try:
            # Large batch inserts
            batch_size = random.randint(50, 200)
            values_list = []
            insert_params = []

            for _ in range(batch_size):
                lat = 40.7 + random.uniform(-0.5, 0.5)
                lon = -74.0 + random.uniform(-0.5, 0.5)
                vehicle_id = random.randint(1, 10000)
                region = random.choice(["region_north", "region_south", "region_central"])

                # Simulate timestamped events coming in recent time
                time_offset = random.randint(0, 3600)  # Last hour
                recorded_at = datetime.now() - timedelta(seconds=time_offset)

                values_list.append(f"(%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), %s, %s)")
                insert_params.extend([vehicle_id, lon, lat, recorded_at, region])

            query = "INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code) VALUES " + ", ".join(values_list)
            count, latency = execute_query(query, insert_params, "write")

            self.user.environment.events.request.fire(
                request_type="Write",
                name=f"Bulk Insert ({batch_size} records)",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Write",
                name="Bulk Insert",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(10)
    def verify_recent_inserts(self):
        """Check that recent inserts were successful"""
        try:
            # Check the most recent data - simulating verification
            minutes = random.randint(5, 30)

            query = """
                SELECT
                    COUNT(*) as recent_inserts,
                    MIN(recorded_at) as earliest,
                    MAX(recorded_at) as latest
                FROM
                    vehicle_locations
                WHERE
                    recorded_at > %s
            """

            count, latency = execute_query(
                query,
                params=(datetime.now() - timedelta(minutes=minutes),),
                query_type="read"
            )

            self.user.environment.events.request.fire(
                request_type="Read",
                name="Verify Recent Inserts",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Read",
                name="Verify Recent Inserts",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(10)
    def purge_old_data(self):
        """Delete old data - simulating cleanup processes"""
        try:
            # Remove older data
            days_old = random.randint(60, 120)  # Simulate removing data 2-4 months old

            query = """
                DELETE FROM vehicle_locations
                WHERE recorded_at < %s
                RETURNING id
            """

            count, latency = execute_query(
                query,
                params=(datetime.now() - timedelta(days=days_old),),
                query_type="write"
            )

            self.user.environment.events.request.fire(
                request_type="Write",
                name="Purge Old Data",
                response_time=latency,
                response_length=count,
                exception=None
            )
        except Exception as e:
            self.user.environment.events.request.fire(
                request_type="Write",
                name="Purge Old Data",
                response_time=0,
                response_length=0,
                exception=e
            )

# New user class that simulates realistic behavioral patterns
class BehavioralPatternUser(User):
    """Simulates users with specific behavioral patterns"""

    # Tasks can be dynamic based on environment variables
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Select a user pattern based on weights or environment variables
        pattern_weights = {
            "data_analyst": int(os.getenv("DATA_ANALYST_WEIGHT", "30")),
            "fleet_manager": int(os.getenv("FLEET_MANAGER_WEIGHT", "50")),
            "data_ingestion": int(os.getenv("DATA_INGESTION_WEIGHT", "20"))
        }

        # Normalize weights
        total_weight = sum(pattern_weights.values())
        if total_weight == 0:
            # Default equal weights if all are 0
            self.user_patterns = list(pattern_weights.keys())
            self.user_weights = [1] * len(self.user_patterns)
        else:
            self.user_patterns = list(pattern_weights.keys())
            self.user_weights = [pattern_weights[p] for p in self.user_patterns]

    # Dynamic wait time based on selected pattern
    wait_time = between(0.1, 5)  # Will be overridden in task sets

    def on_start(self):
        """Initialize the user with a behavioral pattern"""
        try:
            # Check connection and table
            execute_query("SELECT 1")

            # Select a user pattern based on weights
            self.selected_pattern = random.choices(
                self.user_patterns,
                weights=self.user_weights,
                k=1
            )[0]

            print(f"User started with behavioral pattern: {self.selected_pattern}")
        except Exception as e:
            print(f"Database connection failed: {e}")
            raise StopUser()

    # Task distribution based on the selected pattern
    @task
    def run_pattern_tasks(self):
        pattern = self.selected_pattern

        if pattern == "data_analyst":
            # Data analysts run analytical queries
            self.schedule_task(DataAnalystTasks)
        elif pattern == "fleet_manager":
            # Fleet managers monitor and update vehicle data
            self.schedule_task(FleetManagerTasks)
        elif pattern == "data_ingestion":
            # Automated systems primarily write data
            self.schedule_task(DataIngestionTasks)
