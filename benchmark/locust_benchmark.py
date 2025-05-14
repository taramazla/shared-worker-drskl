#!/usr/bin/env python3
import time
import random
import os
from locust import User, task, between, events
import psycopg2
import statistics
import json
from datetime import datetime, timedelta
from locust.exception import StopUser
from collections import deque
from locust import LoadTestShape
import threading

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

# Concurrent connection settings
# Set MAX_CONCURRENT_QUERIES based on USER_COUNT (virtual users) or a provided value
USER_COUNT = int(os.getenv("USER_COUNT", "100"))
MAX_CONCURRENT_QUERIES = int(os.getenv("MAX_CONCURRENT_QUERIES", str(USER_COUNT)))  # Default to match user count
CONCURRENT_MODE = os.getenv("CONCURRENT_MODE", "true").lower() == "true"  # Enable/disable concurrent mode

# Create a semaphore to limit concurrent connections
concurrent_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_QUERIES)
metrics_lock = threading.Lock()

# Print the operation mode for clarity
if READ_WEIGHT > 0 and WRITE_WEIGHT > 0:
    print(f"Running in MIXED mode: {READ_WEIGHT}% read, {WRITE_WEIGHT}% write")
elif READ_WEIGHT > 0:
    print(f"Running in READ-ONLY mode")
elif WRITE_WEIGHT > 0:
    print(f"Running in WRITE-ONLY mode")
else:
    print("WARNING: Both READ_WEIGHT and WRITE_WEIGHT are 0. No operations will run.")

# Print concurrent connection mode
print(f"Concurrent mode: {'ENABLED' if CONCURRENT_MODE else 'DISABLED'}")
if CONCURRENT_MODE:
    if MAX_CONCURRENT_QUERIES == USER_COUNT:
        print(f"Maximum concurrent connections: {MAX_CONCURRENT_QUERIES} (matching virtual user count)")
    else:
        print(f"Maximum concurrent connections: {MAX_CONCURRENT_QUERIES} (custom setting, virtual user count: {USER_COUNT})")

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
    "spatial_count": 0,  # Keep this for backward compatibility
    "recent_response_times": deque(maxlen=100),  # Store recent response times
    "error_rate": 0.0,  # Current error rate
    "last_adjustment_time": time.time(),  # Track when we last adjusted the wait time
    "concurrent_queries": 0,  # Track active concurrent queries
    "max_concurrent": 0,  # Track maximum concurrent queries observed
}

# Dynamic load parameters
BASE_MEAN_WAIT = float(os.getenv("BASE_MEAN_WAIT", "3.0"))  # Base mean wait time in seconds
MIN_MEAN_WAIT = float(os.getenv("MIN_MEAN_WAIT", "1.0"))    # Minimum mean wait time in seconds
MAX_MEAN_WAIT = float(os.getenv("MAX_MEAN_WAIT", "10.0"))   # Maximum mean wait time in seconds
RESPONSE_TIME_THRESHOLD = float(os.getenv("RESPONSE_TIME_THRESHOLD", "1000.0"))  # ms
ERROR_RATE_THRESHOLD = float(os.getenv("ERROR_RATE_THRESHOLD", "0.05"))  # 5%
ADJUSTMENT_INTERVAL = float(os.getenv("ADJUSTMENT_INTERVAL", "5.0"))  # seconds
ADJUSTMENT_FACTOR = float(os.getenv("ADJUSTMENT_FACTOR", "1.2"))  # How aggressively to adjust

# Global variable to track current mean wait time
current_mean_wait = BASE_MEAN_WAIT

# No stages or workload profiles defined - using constant load

# Define simple wait time function with fixed delay
def fixed_wait_time():
    """
    Return a consistent wait time based on current_mean_wait
    """
    print("Using fixed wait time")
    def wait_time(user_instance=None):
        # Simply use current_mean_wait as the wait time
        return current_mean_wait
    return wait_time

# Function to adjust wait time based on system load
def adjust_wait_time():
    """Adjust the mean wait time based on current system load metrics"""
    global current_mean_wait

    now = time.time()
    # Only adjust at most once every ADJUSTMENT_INTERVAL seconds
    if now - custom_metrics["last_adjustment_time"] < ADJUSTMENT_INTERVAL:
        return

    custom_metrics["last_adjustment_time"] = now

    # Calculate average recent response time
    recent_times = list(custom_metrics["recent_response_times"])
    if not recent_times:
        return  # Not enough data yet

    avg_response_time = statistics.mean(recent_times)
    current_error_rate = custom_metrics["error_rate"]

    # Log current metrics
    print(f"Current load metrics - Avg Response Time: {avg_response_time:.2f}ms, Error Rate: {current_error_rate:.2%}")

    # Adjust wait time based on response time and error rate
    if avg_response_time > RESPONSE_TIME_THRESHOLD or current_error_rate > ERROR_RATE_THRESHOLD:
        # System is under high load, increase wait time (reduce load)
        new_mean_wait = min(current_mean_wait * ADJUSTMENT_FACTOR, MAX_MEAN_WAIT)
        if new_mean_wait > current_mean_wait:
            print(f"High load detected! Increasing wait time from {current_mean_wait:.2f}s to {new_mean_wait:.2f}s")
            current_mean_wait = new_mean_wait
    elif avg_response_time < RESPONSE_TIME_THRESHOLD / 2 and current_error_rate < ERROR_RATE_THRESHOLD / 2:
        # System is handling load well, decrease wait time (increase load)
        new_mean_wait = max(current_mean_wait / ADJUSTMENT_FACTOR, MIN_MEAN_WAIT)
        if new_mean_wait < current_mean_wait:
            print(f"Low load detected! Decreasing wait time from {current_mean_wait:.2f}s to {new_mean_wait:.2f}s")
            current_mean_wait = new_mean_wait

# Initialize PostgreSQL connection pool
conn_pool = None

# Table existence flag
table_exists = False

def ensure_vehicle_locations_table_exists():
    """Create the vehicle_locations table if it doesn't exist"""
    global table_exists

    max_attempts = 3
    for attempt in range(max_attempts):
        conn = None
        try:
            print(f"Checking table existence (attempt {attempt+1}/{max_attempts})...")
            conn = get_connection()

            # Check if PostGIS extension exists
            with conn.cursor() as cursor:
                cursor.execute("SELECT count(*) FROM pg_extension WHERE extname = 'postgis';")
                has_postgis = cursor.fetchone()[0] > 0

                if not has_postgis:
                    print("PostGIS extension not found. Attempting to create it...")
                    cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
                    conn.commit()
                    print("PostGIS extension created successfully")
                else:
                    print("PostGIS extension already exists")

            # Now check if the table exists
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'vehicle_locations'
                    );
                """)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    print("Creating vehicle_locations table...")

                    # Create the table
                    cursor.execute("""
                        CREATE TABLE vehicle_locations (
                          id bigserial PRIMARY KEY,
                          vehicle_id int NOT NULL,
                          location geometry(Point, 4326) NOT NULL,
                          recorded_at timestamptz NOT NULL,
                          region_code text NOT NULL
                        );
                    """)
                    conn.commit()

                    # Create indexes
                    cursor.execute("""
                        CREATE INDEX idx_vehicle_locations_location
                        ON vehicle_locations USING GIST (location);
                    """)
                    conn.commit()

                    cursor.execute("""
                        CREATE INDEX idx_vehicle_locations_region_code
                        ON vehicle_locations (region_code);
                    """)
                    conn.commit()

                    cursor.execute("""
                        CREATE INDEX idx_vehicle_locations_vehicle_id
                        ON vehicle_locations (vehicle_id);
                    """)
                    conn.commit()

                    # Insert some sample data (commit in smaller batches)
                    print("Inserting sample data in batches...")
                    batch_size = 1000
                    for i in range(0, 10, 1):
                        cursor.execute(f"""
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
                            FROM generate_series(1, {batch_size}) s(i);
                        """)
                        conn.commit()
                        print(f"Inserted batch {i+1}/10 ({batch_size} rows)")

                    # Verify the table was created successfully
                    cursor.execute("SELECT COUNT(*) FROM vehicle_locations;")
                    row_count = cursor.fetchone()[0]

                    if row_count > 0:
                        table_exists = True
                        print(f"Successfully created vehicle_locations table with {row_count} sample data rows")
                    else:
                        print("Table created but no data was inserted")
                else:
                    # Table already exists, verify it has data
                    cursor.execute("SELECT COUNT(*) FROM vehicle_locations;")
                    row_count = cursor.fetchone()[0]
                    print(f"vehicle_locations table already exists with {row_count} rows")

                    if row_count == 0:
                        print("Table exists but has no data. Adding sample data...")
                        # Add sample data
                        batch_size = 1000
                        for i in range(0, 10, 1):
                            cursor.execute(f"""
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
                                FROM generate_series(1, {batch_size}) s(i);
                            """)
                            conn.commit()
                            print(f"Inserted batch {i+1}/10 ({batch_size} rows)")

            # If we get to this point without exceptions, we're good
            return table_exists

        except Exception as e:
            print(f"Error in table creation (attempt {attempt+1}): {e}")
            if conn:
                try:
                    conn.rollback()
                except:
                    pass
            # Sleep before retry
            if attempt < max_attempts - 1:
                sleep_time = 2 ** attempt  # Exponential backoff
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
        finally:
            if conn:
                try:
                    conn.close()
                except:
                    pass

    # If we get here, all attempts failed
    return False

def get_connection():
    """Create a new PostgreSQL connection"""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    # Set search_path for this connection to ensure schema visibility
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO citus;")
        conn.commit()

    return conn

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Initialize database connection when the test starts"""
    print(f"Initializing database connection to {DB_HOST}:{DB_PORT}...")
    try:
        # Test connection
        conn = get_connection()
        conn.close()
        print(f"Database connection successful")

        # Ensure vehicle_locations table exists
        if ensure_vehicle_locations_table_exists():
            print("Table setup complete")
        else:
            print("Failed to setup table. Tests may fail.")

        # Record test start time for throughput calculation
        custom_metrics["start_time"] = time.time()

    except Exception as e:
        print(f"Failed to initialize database connection: {e}")
        environment.process_exit()

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Clean up when the test ends and report metrics"""
    print("Test stopping. Generating metrics...")

    # Calculate test duration and throughput, using fallback if start_time was not recorded
    end_time = time.time()
    # Fallback to environment.runner.stats_start_time if custom_metrics start_time is None
    start_time = custom_metrics["start_time"] if custom_metrics["start_time"] is not None else getattr(environment.runner, "stats_start_time", end_time)
    if custom_metrics["start_time"] is None:
        print(f"Warning: Test start time was not recorded. Using runner.stats_start_time: {start_time}")
    test_duration = end_time - start_time

    # Count operations for throughput
    total_read_count = custom_metrics["read_count"] + custom_metrics["spatial_count"]
    total_ops = total_read_count + custom_metrics["write_count"]
    # Calculate throughput metrics (ops/sec)
    total_throughput = total_ops / test_duration if test_duration > 0 else 0
    read_throughput = total_read_count / test_duration if test_duration > 0 else 0
    write_throughput = custom_metrics["write_count"] / test_duration if test_duration > 0 else 0

    # Print throughput summary
    print(f"Overall throughput: {total_throughput:.2f} ops/sec ({total_ops} operations in {test_duration:.2f} seconds)")
    print(f"Read throughput: {read_throughput:.2f} ops/sec ({total_read_count} operations, includes {custom_metrics['spatial_count']} spatial)")
    print(f"Write throughput: {write_throughput:.2f} ops/sec ({custom_metrics['write_count']} operations)")
    print(f"Maximum concurrent connections: {custom_metrics['max_concurrent']}")

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
            "max_concurrent_connections": custom_metrics["max_concurrent"],
            "success_count": custom_metrics["success_count"],
            "error_count": custom_metrics["error_count"],
            "success_rate": success_rate,
            "timestamp": datetime.now().isoformat()
        }, f, indent=2)
    print(f"Metrics saved to {results_dir}/locust_metrics_{timestamp}.json")

def execute_query(query, params=None, query_type="read"):
    """Execute a database query and measure latency"""
    conn = None
    acquired_semaphore = False

    try:
        # Acquire semaphore before executing query
        if CONCURRENT_MODE:
            concurrent_semaphore.acquire()
            acquired_semaphore = True
            with metrics_lock:
                custom_metrics["concurrent_queries"] += 1
                custom_metrics["max_concurrent"] = max(custom_metrics["max_concurrent"], custom_metrics["concurrent_queries"])

        # Get a connection
        conn = get_connection()

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

        # Add to recent response times for load adjustment
        with metrics_lock:
            custom_metrics["recent_response_times"].append(latency_ms)

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

            # Update error rate
            total_ops = custom_metrics["success_count"] + custom_metrics["error_count"]
            if total_ops > 0:
                custom_metrics["error_rate"] = custom_metrics["error_count"] / total_ops

        # Adjust wait time based on system metrics
        adjust_wait_time()

        # Return the result count and latency
        return count, latency_ms

    except Exception as e:
        with metrics_lock:
            custom_metrics["errors"].append(str(e))
            custom_metrics["error_count"] += 1

            # Update error rate
            total_ops = custom_metrics["success_count"] + custom_metrics["error_count"]
            if total_ops > 0:
                custom_metrics["error_rate"] = custom_metrics["error_count"] / total_ops

        # Adjust wait time after error
        adjust_wait_time()

        # Re-raise the exception with more context
        raise Exception(f"Query failed: {e}")

    finally:
        # Release semaphore after executing query - only if we acquired it
        if CONCURRENT_MODE and acquired_semaphore:
            with metrics_lock:
                custom_metrics["concurrent_queries"] -= 1
            concurrent_semaphore.release()

        # Close the connection
        if conn:
            try:
                conn.close()
            except Exception as e:
                print(f"Error closing connection: {e}")

class PostgresUser(User):
    """Locust user class for PostgreSQL benchmark"""

    # Use fixed wait time
    wait_time = fixed_wait_time()

    def on_start(self):
        """Called when a User starts running"""
        # Check if database connection is working
        try:
            # Execute a simple query to verify connection
            conn = get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            conn.close()
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
                        "SELECT vehicle_id, recorded_at, region_code FROM vehicle_locations " +
                        "WHERE vehicle_id = %s ORDER BY recorded_at DESC LIMIT 1",
                        (random.randint(1, 10000),)
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
                            "SELECT id, vehicle_id, recorded_at, region_code FROM vehicle_locations " +
                            "WHERE location && ST_MakeEnvelope(%s, %s, %s, %s, 4326) " +
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
                query_type = random.choice([
                    "insert_vehicle",
                    "update_vehicle",
                    "delete_vehicle"
                ])

                if query_type == "insert_vehicle":
                    # Insert a new vehicle location
                    count, latency = execute_query(
                        "INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code) " +
                        "VALUES (%s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), NOW(), %s)",
                        (random.randint(1, 10000), -74.0 + random.uniform(-0.1, 0.1), 40.7 + random.uniform(-0.1, 0.1),
                         random.choice(['region_north', 'region_south', 'region_central'])),
                        "write"
                    )
                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Insert Vehicle",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif query_type == "update_vehicle":
                    # Update a vehicle location
                    count, latency = execute_query(
                        "UPDATE vehicle_locations SET location = ST_SetSRID(ST_MakePoint(%s, %s), 4326) " +
                        "WHERE vehicle_id = %s",
                        (-74.0 + random.uniform(-0.1, 0.1), 40.7 + random.uniform(-0.1, 0.1), random.randint(1, 10000)),
                        "write"
                    )
                    self.environment.events.request.fire(
                        request_type="Write",
                        name="Update Vehicle",
                        response_time=latency,
                        response_length=count,
                        exception=None
                    )

                elif query_type == "delete_vehicle":
                    # Delete a vehicle location
                    count, latency = execute_query(
                        "DELETE FROM vehicle_locations WHERE vehicle_id = %s",
                        (random.randint(1, 10000),),
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

# Standard constant load shape
class ConstantLoadShape(LoadTestShape):
    """
    Maintains a constant number of users throughout the test.
    """
    def __init__(self):
        # Default user count and spawn rate
        self.user_count = int(os.getenv("USER_COUNT", "100"))
        self.spawn_rate = int(os.getenv("SPAWN_RATE", "10"))
        self.test_duration = int(os.getenv("TEST_DURATION", "600"))  # 10 minutes by default
        print(f"Running constant load with {self.user_count} users, spawn rate of {self.spawn_rate}/s for {self.test_duration}s")

    def tick(self):
        # Determine elapsed time since test start
        start = custom_metrics.get("start_time") or time.time()
        elapsed = time.time() - start

        # Stop after test duration
        if elapsed >= self.test_duration:
            print("Test duration reached. Stopping test.")
            return None

        return (self.user_count, self.spawn_rate)
