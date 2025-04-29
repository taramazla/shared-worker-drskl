#!/usr/bin/env python3
"""
PostgreSQL/Citus Latency Monitor

A real-time monitoring tool for visualizing database latency and throughput metrics
for a PostgreSQL/Citus cluster. Supports multiple query types and visualization of results.
"""

import psycopg2
import time
import matplotlib.pyplot as plt
import numpy as np
import os
import random
import argparse
import threading
import queue
import signal
import json
from datetime import datetime, timedelta
from matplotlib.animation import FuncAnimation
import matplotlib.gridspec as gridspec
from matplotlib.ticker import MaxNLocator
import math
from psycopg2 import pool

# Default connection parameters - override with command line args
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432
DEFAULT_USER = "citus"
DEFAULT_PASSWORD = "citus"
DEFAULT_DB = "citus"

# Query types for benchmark
QUERY_TYPES = {
    "simple": "Simple query (SELECT COUNT(*) FROM test_table)",
    "spatial": "Spatial query (ST_DWithin for location search)",
    "complex": "Complex analytical query (aggregation with GROUP BY)",
    "mixed": "Mixed workload (read/write operations)",
    "oltp": "OLTP workload (short, frequent transactions)",
    "olap": "OLAP workload (complex analytical queries)",
    "realistic": "Realistic workload pattern with variable load",
    "all": "Run all query types"
}

# Chart settings
MAX_DATA_POINTS = 100
COLORS = {
    "latency": "#1f77b4",  # Blue
    "throughput": "#2ca02c",  # Green
    "avg_latency": "#ff7f0e",  # Orange
    "p95_latency": "#d62728",  # Red
    "p99_latency": "#9467bd",  # Purple
    "concurrent": "#8c564b",  # Brown
    "error_rate": "#e377c2",  # Pink
    "write": "#7f7f7f",      # Gray
    "read": "#bcbd22"        # Yellow-green
}

# Workload patterns
WORKLOAD_PATTERNS = {
    "constant": "Constant load",
    "sawtooth": "Sawtooth pattern (gradual increase then drop)",
    "sine": "Sine wave pattern",
    "step": "Step function (sudden jumps in load)",
    "spike": "Random spike pattern",
    "realistic": "Realistic daily pattern with peak hours"
}

# SQL statements to create test tables
CREATE_TEST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,
    value FLOAT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SPATIAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    location GEOMETRY(Point, 4326),
    region_code TEXT,
    metadata JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_TRANSACTION_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_transactions (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    amount DECIMAL(10,2),
    transaction_type TEXT,
    status TEXT,
    items JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_TEST_DATA_SQL = """
INSERT INTO test_table (name, value, metadata)
SELECT
    'test_' || i,
    random() * 100,
    jsonb_build_object(
        'category', CASE WHEN i % 5 = 0 THEN 'A' WHEN i % 5 = 1 THEN 'B' ELSE 'C' END,
        'priority', CASE WHEN i % 10 = 0 THEN 'high' WHEN i % 10 < 3 THEN 'medium' ELSE 'low' END,
        'tags', jsonb_build_array('tag' || (i % 5), 'tag' || (i % 3))
    )
FROM
    generate_series(1, 10000) i
ON CONFLICT DO NOTHING;
"""

INSERT_SPATIAL_DATA_SQL = """
INSERT INTO test_locations (name, location, region_code, metadata)
SELECT
    'poi_' || i,
    ST_SetSRID(ST_MakePoint(
        -74.0 + random() * 0.5,
        40.7 + random() * 0.5),
    4326),
    'region_' || (i % 10),
    jsonb_build_object(
        'type', CASE WHEN i % 4 = 0 THEN 'restaurant' WHEN i % 4 = 1 THEN 'store'
               WHEN i % 4 = 2 THEN 'hotel' ELSE 'landmark' END,
        'rating', (random() * 5)::numeric(3,1),
        'features', jsonb_build_array('feature' || (i % 5), 'feature' || (i % 7))
    )
FROM
    generate_series(1, 5000) i
ON CONFLICT DO NOTHING;
"""

INSERT_TRANSACTION_DATA_SQL = """
INSERT INTO test_transactions (customer_id, amount, transaction_type, status, items)
SELECT
    (random() * 1000)::integer,
    (random() * 500)::numeric(10,2),
    CASE WHEN random() < 0.7 THEN 'purchase' WHEN random() < 0.9 THEN 'refund' ELSE 'adjustment' END,
    CASE WHEN random() < 0.9 THEN 'completed' WHEN random() < 0.98 THEN 'pending' ELSE 'failed' END,
    jsonb_build_object(
        'count', (random() * 10 + 1)::integer,
        'products', jsonb_build_array(
            jsonb_build_object('id', (i*3)::text, 'price', (random() * 100)::numeric(10,2)),
            jsonb_build_object('id', (i*3+1)::text, 'price', (random() * 100)::numeric(10,2)),
            jsonb_build_object('id', (i*3+2)::text, 'price', (random() * 100)::numeric(10,2))
        )
    )
FROM
    generate_series(1, 3000) i
ON CONFLICT DO NOTHING;
"""

# SQL to verify PostGIS and Citus extensions
CHECK_EXTENSIONS_SQL = """
SELECT extname FROM pg_extension WHERE extname IN ('postgis', 'citus');
"""

class LatencyMonitor:
    """Monitor and visualize PostgreSQL/Citus query latency in real-time"""

    def __init__(self, host, port, user, password, dbname, query_type="simple",
                 client_count=1, interval=1.0, warmup_seconds=5,
                 workload_pattern="constant", duration=60, pool_size=5,
                 read_write_ratio=0.8, network_latency=0, error_rate=0):
        """Initialize the latency monitor"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.query_type = query_type
        self.base_client_count = client_count
        self.client_count = client_count
        self.interval = interval
        self.warmup_seconds = warmup_seconds
        self.workload_pattern = workload_pattern
        self.total_duration = duration
        self.pool_size = min(pool_size, client_count * 2)
        self.read_write_ratio = read_write_ratio  # 0.8 means 80% reads, 20% writes
        self.network_latency = network_latency
        self.error_rate = error_rate

        # Add new tracking metrics
        self.read_latencies = []
        self.write_latencies = []
        self.errors = []
        self.error_count = 0

        # Data storage
        self.timestamps = []
        self.latencies = []
        self.throughputs = []
        self.concurrent = []
        self.all_latencies = []  # For calculating percentiles
        self.current_concurrent = 0
        self.total_queries = 0
        self.warmup_queries = 0
        self.warmup_end_time = None
        self.read_count = 0
        self.write_count = 0

        # Connection tracking
        self.current_connections = 0
        self.max_connections = 0
        self.connection_timeouts = 0
        self.connection_failures = 0

        # Communication between data collection threads and visualization
        self.data_queue = queue.Queue()
        self.stop_event = threading.Event()

        # For realtime control of load
        self.load_factor = 1.0
        self.pattern_start_time = None

        # Initialize connection pool instead of single connection
        self.pool = self._create_connection_pool()

        # Check and setup test tables
        self._setup_test_tables()

        # Initialize plots
        self._init_plots()

    def _create_connection_pool(self):
        """Create a connection pool for more realistic database access"""
        try:
            conn_pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=self.pool_size,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            print(f"Connection pool created with {self.pool_size} max connections")
            return conn_pool
        except Exception as e:
            print(f"Connection pool creation failed: {e}")
            print("Falling back to direct connections")
            return None

    def _get_connection(self):
        """Get a connection from the pool or create a new one"""
        if self.pool:
            try:
                conn = self.pool.getconn()
                self.current_connections += 1
                self.max_connections = max(self.max_connections, self.current_connections)
                return conn
            except pool.PoolError:
                self.connection_timeouts += 1
                # If pool is exhausted, simulate waiting for a connection
                time.sleep(random.uniform(0.1, 1.0))
                return self._connect()
        else:
            return self._connect()

    def _return_connection(self, conn):
        """Return a connection to the pool"""
        if self.pool and conn:
            try:
                self.pool.putconn(conn)
                self.current_connections -= 1
            except Exception as e:
                print(f"Error returning connection to pool: {e}")
                try:
                    conn.close()
                except:
                    pass
                self.current_connections -= 1

    def _connect(self):
        """Connect to the PostgreSQL database directly (fallback)"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname,
                connect_timeout=5
            )
            self.current_connections += 1
            self.max_connections = max(self.max_connections, self.current_connections)
            return conn
        except Exception as e:
            self.connection_failures += 1
            print(f"Connection failed: {e}")
            return None

    def _check_extensions(self):
        """Verify that required extensions are installed"""
        conn = self._get_connection()
        if not conn:
            print("Cannot check extensions - no database connection")
            return []

        cursor = conn.cursor()
        cursor.execute(CHECK_EXTENSIONS_SQL)
        installed_extensions = [row[0] for row in cursor.fetchall()]
        cursor.close()
        self._return_connection(conn)

        print(f"Detected extensions: {', '.join(installed_extensions)}")

        # Check if PostGIS is needed and available
        if self.query_type in ["spatial", "all", "realistic"] and "postgis" not in installed_extensions:
            print("WARNING: PostGIS extension not found but spatial queries were requested.")
            print("         Spatial queries will be disabled.")
            if self.query_type == "spatial":
                self.query_type = "simple"
                print("Falling back to simple queries.")

        # Check if Citus is available
        if "citus" not in installed_extensions:
            print("WARNING: Citus extension not detected. Some distributed features may not work.")

        return installed_extensions

    def _setup_test_tables(self):
        """Create test tables if they don't exist"""
        extensions = self._check_extensions()
        conn = self._get_connection()
        if not conn:
            print("Cannot setup test tables - no database connection")
            return

        cursor = conn.cursor()

        try:
            # Create basic test tables
            print("Setting up test tables if they don't exist...")
            cursor.execute(CREATE_TEST_TABLE_SQL)
            cursor.execute(CREATE_TRANSACTION_TABLE_SQL)

            # Check if we need spatial tables
            if "postgis" in extensions and self.query_type in ["spatial", "all", "realistic", "mixed"]:
                cursor.execute(CREATE_SPATIAL_TABLE_SQL)

                # Insert spatial test data
                cursor.execute("SELECT COUNT(*) FROM test_locations")
                count = cursor.fetchone()[0]
                if count < 100:
                    print("Inserting spatial test data...")
                    cursor.execute(INSERT_SPATIAL_DATA_SQL)
                    print(f"Added spatial test data ({count} rows)")

            # Insert transaction test data
            cursor.execute("SELECT COUNT(*) FROM test_transactions")
            count = cursor.fetchone()[0]
            if count < 100:
                print("Inserting transaction test data...")
                cursor.execute(INSERT_TRANSACTION_DATA_SQL)
                print(f"Added transaction test data ({count} rows)")

            # Insert basic test data
            cursor.execute("SELECT COUNT(*) FROM test_table")
            count = cursor.fetchone()[0]
            if count < 100:
                print("Inserting test data...")
                cursor.execute(INSERT_TEST_DATA_SQL)
                print(f"Added test data ({count} rows)")

            conn.commit()
            print(f"Test tables are ready.")

        except Exception as e:
            conn.rollback()
            print(f"Error setting up test tables: {e}")
            # If spatial query type was requested but we can't set up tables,
            # fall back to simple queries
            if self.query_type == "spatial":
                self.query_type = "simple"
                print("Falling back to simple queries.")
        finally:
            cursor.close()
            self._return_connection(conn)

    def _init_plots(self):
        """Initialize the matplotlib plots"""
        self.fig = plt.figure(figsize=(14, 12))
        gs = gridspec.GridSpec(4, 1, height_ratios=[2, 1, 1, 1])

        # Latency plot
        self.ax_latency = plt.subplot(gs[0])
        self.latency_line, = self.ax_latency.plot([], [], color=COLORS["latency"], label="Query Latency (ms)")
        self.avg_line, = self.ax_latency.plot([], [], color=COLORS["avg_latency"], linestyle='--', label="Avg Latency")
        self.p95_line, = self.ax_latency.plot([], [], color=COLORS["p95_latency"], linestyle='-.', label="p95 Latency")
        self.p99_line, = self.ax_latency.plot([], [], color=COLORS["p99_latency"], linestyle=':', label="p99 Latency")

        # Add read/write lines for mixed workloads
        self.read_line, = self.ax_latency.plot([], [], color=COLORS["read"], linestyle='-', alpha=0.5, label="Read Latency")
        self.write_line, = self.ax_latency.plot([], [], color=COLORS["write"], linestyle='-', alpha=0.5, label="Write Latency")

        self.ax_latency.set_title(f"PostgreSQL/Citus Query Latency - {QUERY_TYPES.get(self.query_type, 'Custom query')}")
        self.ax_latency.set_xlabel("Time (s)")
        self.ax_latency.set_ylabel("Latency (ms)")
        self.ax_latency.grid(True)
        self.ax_latency.legend(loc="upper left")

        # Throughput plot
        self.ax_throughput = plt.subplot(gs[1], sharex=self.ax_latency)
        self.throughput_line, = self.ax_throughput.plot([], [], color=COLORS["throughput"], label="Queries/second")
        self.ax_throughput.set_xlabel("Time (s)")
        self.ax_throughput.set_ylabel("Throughput (qps)")
        self.ax_throughput.grid(True)
        self.ax_throughput.legend(loc="upper left")

        # Concurrency plot
        self.ax_concurrent = plt.subplot(gs[2], sharex=self.ax_latency)
        self.concurrent_line, = self.ax_concurrent.plot([], [], color=COLORS["concurrent"], label="Concurrent Queries")
        self.ax_concurrent.set_xlabel("Time (s)")
        self.ax_concurrent.set_ylabel("Concurrency")
        self.ax_concurrent.grid(True)
        self.ax_concurrent.legend(loc="upper left")

        # Error rate plot
        self.ax_error = plt.subplot(gs[3], sharex=self.ax_latency)
        self.error_line, = self.ax_error.plot([], [], color=COLORS["error_rate"], label="Error Rate (%)")
        self.ax_error.set_xlabel("Time (s)")
        self.ax_error.set_ylabel("Error %")
        self.ax_error.grid(True)
        self.ax_error.legend(loc="upper left")

        # Set y-axis to be integer only for concurrency
        self.ax_concurrent.yaxis.set_major_locator(MaxNLocator(integer=True))

        plt.tight_layout()

    def _get_query(self, query_type=None, is_write=False):
        """Get the appropriate query based on the query type"""
        if not query_type:
            query_type = self.query_type

        # For realistic and mixed workloads, randomly select a query type
        if query_type == "realistic":
            options = ["simple", "spatial", "complex", "oltp", "olap"]
            weights = [0.4, 0.2, 0.1, 0.2, 0.1]
            query_type = random.choices(options, weights=weights)[0]

            # Determine if this should be a write query
            if random.random() > self.read_write_ratio:
                is_write = True

        # Handle write operations
        if is_write:
            return self._get_write_query(query_type)

        # Handle read operations
        return self._get_read_query(query_type)

    def _get_read_query(self, query_type):
        """Generate a read query based on the type"""
        if query_type == "simple":
            return "SELECT COUNT(*) FROM test_table"

        elif query_type == "spatial":
            # Random parameters for a spatial query with more realistic variance
            lat = 40.7 + random.uniform(-0.3, 0.3)
            lon = -74.0 + random.uniform(-0.3, 0.3)
            radius = random.randint(500, 15000)  # meters

            # Add some complexity with additional filtering
            type_filter = random.choice(["restaurant", "store", "hotel", "landmark"])
            min_rating = random.uniform(1, 4)

            return f"""
                SELECT COUNT(*)
                FROM test_locations
                WHERE ST_DWithin(
                    location::geography,
                    ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                    {radius}
                )
                AND metadata->>'type' = '{type_filter}'
                AND (metadata->>'rating')::numeric >= {min_rating}
            """

        elif query_type == "complex":
            # Make the query more complex with window functions and subqueries
            days_ago = random.randint(1, 30)
            cutoff_date = datetime.now() - timedelta(days=days_ago)
            cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

            # Add randomness to WHERE clauses
            min_value = random.uniform(10, 50)
            categories = random.sample(['A', 'B', 'C'], random.randint(1, 3))
            categories_str = "', '".join(categories)

            return f"""
                WITH ranked_data AS (
                    SELECT
                        DATE_TRUNC('day', created_at) AS day,
                        name,
                        value,
                        metadata->>'category' AS category,
                        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', created_at)
                                          ORDER BY value DESC) AS rank
                    FROM
                        test_table
                    WHERE
                        value > {min_value} AND
                        metadata->>'category' IN ('{categories_str}')
                )
                SELECT
                    day,
                    COUNT(*) AS count,
                    AVG(value) AS avg_value,
                    MAX(value) AS max_value,
                    SUM(CASE WHEN rank <= 10 THEN value ELSE 0 END) AS top10_value_sum
                FROM
                    ranked_data
                GROUP BY
                    day
                ORDER BY
                    day DESC
                LIMIT 50
            """

        elif query_type == "oltp":
            # Simulate OLTP query patterns (point lookups, small range scans)
            if random.random() < 0.7:
                # Point lookup by ID
                id_value = random.randint(1, 5000)
                return f"SELECT * FROM test_table WHERE id = {id_value}"
            else:
                # Small range scan with a few predicates
                start_id = random.randint(1, 4000)
                end_id = start_id + random.randint(10, 100)
                return f"""
                    SELECT id, name, value, created_at
                    FROM test_table
                    WHERE id BETWEEN {start_id} AND {end_id}
                    AND metadata->>'priority' = '{random.choice(['high', 'medium', 'low'])}'
                    LIMIT 50
                """

        elif query_type == "olap":
            # Complex analytical query with joins and aggregations
            return f"""
                SELECT
                    t.metadata->>'category' AS category,
                    DATE_TRUNC('day', tr.created_at) AS day,
                    COUNT(tr.*) AS transaction_count,
                    SUM(tr.amount) AS total_amount,
                    AVG(tr.amount) AS avg_amount,
                    COUNT(DISTINCT tr.customer_id) AS unique_customers
                FROM test_transactions tr
                JOIN test_table t ON tr.customer_id % 1000 = t.id % 1000
                WHERE tr.status = 'completed'
                AND tr.transaction_type = '{random.choice(['purchase', 'refund'])}'
                GROUP BY t.metadata->>'category', DATE_TRUNC('day', tr.created_at)
                ORDER BY day DESC, total_amount DESC
                LIMIT 100
            """

        elif query_type == "mixed":
            # Choose a random query from the available types
            query_types = ["simple", "spatial", "complex", "oltp", "olap"]
            weights = [0.3, 0.2, 0.2, 0.2, 0.1]
            return self._get_read_query(random.choices(query_types, weights=weights)[0])

        else:
            return "SELECT 1"  # Fallback default query

    def _get_write_query(self, query_type):
        """Generate a write query based on the type"""
        # Select which table to write to
        if query_type == "spatial" and random.random() < 0.7:
            table = "test_locations"
        elif query_type == "oltp" and random.random() < 0.6:
            table = "test_transactions"
        else:
            table = "test_table"

        # Determine write operation type
        op_type = random.choices(
            ["insert", "update", "delete"],
            weights=[0.7, 0.25, 0.05]
        )[0]

        if op_type == "insert":
            if table == "test_table":
                return f"""
                    INSERT INTO test_table (name, value, metadata)
                    VALUES (
                        'test_generated_{random.randint(10000, 99999)}',
                        {random.uniform(0, 1000):.2f},
                        '{{"category": "{random.choice(['A', 'B', 'C', 'D'])}",
                          "priority": "{random.choice(['high', 'medium', 'low'])}",
                          "timestamp": "{datetime.now().isoformat()}",
                          "source": "benchmark"}}'::jsonb
                    )
                    RETURNING id
                """
            elif table == "test_locations":
                lat = 40.7 + random.uniform(-0.5, 0.5)
                lon = -74.0 + random.uniform(-0.5, 0.5)
                return f"""
                    INSERT INTO test_locations (name, location, region_code, metadata)
                    VALUES (
                        'poi_generated_{random.randint(10000, 99999)}',
                        ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326),
                        'region_{random.randint(0, 20)}',
                        '{{"type": "{random.choice(['restaurant', 'store', 'hotel', 'landmark'])}",
                          "rating": {random.uniform(1.0, 5.0):.1f},
                          "timestamp": "{datetime.now().isoformat()}",
                          "source": "benchmark"}}'::jsonb
                    )
                    RETURNING id
                """
            else:  # test_transactions
                return f"""
                    INSERT INTO test_transactions (
                        customer_id, amount, transaction_type, status, items
                    )
                    VALUES (
                        {random.randint(1, 5000)},
                        {random.uniform(10, 1000):.2f},
                        '{random.choice(['purchase', 'refund', 'adjustment'])}',
                        '{random.choices(['completed', 'pending', 'failed'], weights=[0.8, 0.15, 0.05])[0]}',
                        '{{"count": {random.randint(1, 10)},
                          "products": [
                            {{"id": "{random.randint(1000, 9999)}", "price": {random.uniform(10, 200):.2f}}},
                            {{"id": "{random.randint(1000, 9999)}", "price": {random.uniform(10, 200):.2f}}}
                          ],
                          "timestamp": "{datetime.now().isoformat()}",
                          "source": "benchmark"}}'::jsonb
                    )
                    RETURNING id
                """
        elif op_type == "update":
            if table == "test_table":
                return f"""
                    UPDATE test_table
                    SET
                        value = {random.uniform(0, 1000):.2f},
                        metadata = jsonb_set(
                            metadata,
                            '{{priority}}',
                            '"{random.choice(['high', 'medium', 'low'])}"',
                            true
                        )
                    WHERE id = {random.randint(1, 9000)}
                    RETURNING id
                """
            elif table == "test_locations":
                return f"""
                    UPDATE test_locations
                    SET
                        metadata = jsonb_set(
                            metadata,
                            '{{rating}}',
                            '{random.uniform(1.0, 5.0):.1f}',
                            true
                        )
                    WHERE id = {random.randint(1, 4500)}
                    RETURNING id
                """
            else:  # test_transactions
                return f"""
                    UPDATE test_transactions
                    SET
                        status = '{random.choice(['completed', 'pending', 'failed'])}',
                        amount = {random.uniform(10, 1000):.2f}
                    WHERE id = {random.randint(1, 3000)}
                    RETURNING id
                """
        else:  # delete
            id_val = random.randint(7500, 9999)  # Delete from higher IDs to avoid removing too many rows
            return f"""
                DELETE FROM {table}
                WHERE id = {id_val}
                RETURNING id
            """

    def _apply_workload_pattern(self, elapsed_seconds):
        """Calculate the current load factor based on the workload pattern"""
        if self.workload_pattern == "constant":
            return 1.0

        elif self.workload_pattern == "sawtooth":
            # Sawtooth pattern: gradual increase followed by sharp drop
            cycle = 30  # seconds per cycle
            position = (elapsed_seconds % cycle) / cycle
            return 0.2 + 1.5 * position if position < 0.9 else 0.2

        elif self.workload_pattern == "sine":
            # Sine wave pattern
            cycle = 60  # seconds per cycle
            return 0.5 + 0.5 * math.sin(elapsed_seconds * 2 * math.pi / cycle)

        elif self.workload_pattern == "step":
            # Step function with sudden jumps
            cycle = 60  # seconds per cycle
            step = int((elapsed_seconds % cycle) / (cycle / 4))
            levels = [0.25, 1.0, 0.5, 1.5]
            return levels[step]

        elif self.workload_pattern == "spike":
            # Random spike pattern
            cycle = 15
            if elapsed_seconds % cycle == 0 and random.random() < 0.3:
                return 2.5  # Spike to 2.5x load
            elif elapsed_seconds % cycle == 1 and random.random() < 0.5:
                return 1.8  # Possibly continue spike
            else:
                return 0.6 + random.random() * 0.4

        elif self.workload_pattern == "realistic":
            # Simulate a realistic daily pattern (assuming 60 seconds = 1 day)
            day_position = (elapsed_seconds % 60) / 60  # position in the day (0-1)

            # Morning ramp-up
            if 0.2 <= day_position < 0.35:
                return 0.3 + ((day_position - 0.2) / 0.15) * 0.7  # 0.3 to 1.0

            # Midday peak
            elif 0.35 <= day_position < 0.5:
                return 1.0

            # Afternoon plateau
            elif 0.5 <= day_position < 0.7:
                return 0.8

            # Evening peak
            elif 0.7 <= day_position < 0.85:
                return 1.2

            # Night wind-down
            elif 0.85 <= day_position <= 1.0 or day_position < 0.2:
                return 0.3

            else:
                return 0.5
        else:
            return 1.0

    def _run_query(self, client_id):
        """Run the specified query type and measure its latency"""
        # Local connection for this client
        conn = None
        error_backoff = 1  # Initial backoff time

        while not self.stop_event.is_set():
            is_write = random.random() > self.read_write_ratio
            is_error = random.random() < self.error_rate

            try:
                # Apply workload pattern to calculate the sleep time
                elapsed = time.time() - self.start_time
                load_factor = self._apply_workload_pattern(elapsed)

                # Adjust interval based on load factor (higher load = shorter interval)
                effective_interval = self.interval / max(0.1, load_factor)

                # Managed connection handling
                if not conn:
                    conn = self._get_connection()
                    if not conn:
                        time.sleep(min(5.0, error_backoff))
                        error_backoff = min(error_backoff * 1.5, 10)
                        continue
                    else:
                        error_backoff = 1  # Reset backoff time

                cursor = conn.cursor()
                query = self._get_query(is_write=is_write)

                # Signal that we're starting a query
                self.current_concurrent += 1

                # Simulate network latency (if configured)
                if self.network_latency > 0:
                    # Add jitter to network latency (±20%)
                    jitter = random.uniform(0.8, 1.2) * self.network_latency
                    time.sleep(jitter / 1000)  # Convert ms to seconds

                # Simulate error if needed
                if is_error:
                    raise Exception("Simulated database error")

                # Execute query and measure time
                start_time = time.time()
                cursor.execute(query)

                # For non-SELECT statements, need to commit
                if is_write or query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                    results = cursor.fetchall() if cursor.description else None
                    conn.commit()
                    self.write_count += 1
                else:
                    results = cursor.fetchall()
                    self.read_count += 1

                end_time = time.time()

                # Calculate latency in milliseconds
                latency_ms = (end_time - start_time) * 1000

                # Put results in queue for visualization thread
                timestamp = time.time()
                if self.warmup_end_time and timestamp >= self.warmup_end_time:
                    self.data_queue.put((timestamp, latency_ms, client_id, is_write, False))
                else:
                    self.warmup_queries += 1

                # Signal that we're done with this query
                self.current_concurrent -= 1

                # Close cursor
                cursor.close()

                # Sleep between queries - with some randomness
                jitter = random.uniform(0.9, 1.1)
                time.sleep(effective_interval * jitter)

            except Exception as e:
                # Mark as error in the data
                timestamp = time.time()
                self.error_count += 1
                if self.warmup_end_time and timestamp >= self.warmup_end_time:
                    self.data_queue.put((timestamp, 0, client_id, is_write, True))  # 0 latency for error

                # Signal that we're done with this query
                if self.current_concurrent > 0:
                    self.current_concurrent -= 1

                print(f"Query error (client {client_id}): {e}")

                # Close and reestablish connection on error
                if conn:
                    try:
                        conn.rollback()  # Try to rollback any transaction
                    except:
                        pass

                    self._return_connection(conn)
                    conn = None

                time.sleep(effective_interval * 2)  # Wait longer after an error

    def _update_plot(self, frame):
        """Update the visualization with new data points"""
        # Get all available data from the queue
        current_time = time.time()
        elapsed = current_time - self.start_time  # Define this at the beginning to ensure it's always available
        batch_size = 0
        batch_latency = 0
        error_in_batch = False

        while not self.data_queue.empty():
            timestamp, latency_ms, client_id, is_write, is_error = self.data_queue.get()

            # Use relative time (seconds since start) for x-axis
            relative_time = timestamp - self.start_time

            self.timestamps.append(relative_time)

            # Track errors separately
            if is_error:
                self.errors.append(1)
                error_in_batch = True
            else:
                self.errors.append(0)
                self.latencies.append(latency_ms)
                self.all_latencies.append(latency_ms)
                batch_size += 1
                batch_latency += latency_ms

                # Track read vs write latencies
                if is_write:
                    self.write_latencies.append((relative_time, latency_ms))
                else:
                    self.read_latencies.append((relative_time, latency_ms))

            # Only calculate metrics when we have data
            self.total_queries += 1

        # Limit the data points for better visualization
        if len(self.timestamps) > MAX_DATA_POINTS:
            self.timestamps = self.timestamps[-MAX_DATA_POINTS:]
            self.latencies = self.latencies[-MAX_DATA_POINTS:]
            self.errors = self.errors[-MAX_DATA_POINTS:]

            # Trim read/write latencies lists
            cutoff_time = self.timestamps[0]
            self.read_latencies = [(t, l) for t, l in self.read_latencies if t >= cutoff_time]
            self.write_latencies = [(t, l) for t, l in self.write_latencies if t >= cutoff_time]

        # Calculate throughput (queries per second)
        # Use a 5-second sliding window if we have enough data points
        window_size = min(5, len(self.timestamps))
        if window_size >= 2:
            time_diff = self.timestamps[-1] - self.timestamps[-window_size]
            if time_diff > 0:
                throughput = (window_size - 1) / time_diff
                self.throughputs.append(throughput)
            else:
                self.throughputs.append(0)
        else:
            self.throughputs.append(0)

        # Limit throughput data points
        if len(self.throughputs) > MAX_DATA_POINTS:
            self.throughputs = self.throughputs[-MAX_DATA_POINTS:]

        # Track concurrent queries
        self.concurrent.append(self.current_concurrent)
        if len(self.concurrent) > MAX_DATA_POINTS:
            self.concurrent = self.concurrent[-MAX_DATA_POINTS:]

        # Update the plot data
        if self.timestamps:
            # Update latency plot - ensure data arrays match
            self.latency_line.set_data(self.timestamps, self.latencies)

            # Calculate and update average latency
            if self.all_latencies:
                avg_latency = sum(self.all_latencies) / len(self.all_latencies)
                avg_line_data = [avg_latency] * len(self.timestamps)
                self.avg_line.set_data(self.timestamps, avg_line_data)

                # Calculate p95 latency if we have enough data
                if len(self.all_latencies) >= 20:
                    p95_latency = np.percentile(self.all_latencies, 95)
                    p95_line_data = [p95_latency] * len(self.timestamps)
                    self.p95_line.set_data(self.timestamps, p95_line_data)

                    # Also calculate p99 latency
                    p99_latency = np.percentile(self.all_latencies, 99)
                    p99_line_data = [p99_latency] * len(self.timestamps)
                    self.p99_line.set_data(self.timestamps, p99_line_data)

            # Update read/write lines if we have data
            if self.read_latencies:
                read_times, read_lats = zip(*self.read_latencies)
                self.read_line.set_data(read_times, read_lats)

            if self.write_latencies:
                write_times, write_lats = zip(*self.write_latencies)
                self.write_line.set_data(write_times, write_lats)

            # Update throughput plot - ensure arrays are non-empty and same length
            if self.throughputs:
                throughput_timestamps = self.timestamps[-len(self.throughputs):]
                # Ensure both arrays have the same length
                min_length = min(len(throughput_timestamps), len(self.throughputs))
                if min_length > 0:  # Only update if we have valid data
                    self.throughput_line.set_data(
                        throughput_timestamps[:min_length],
                        self.throughputs[:min_length]
                    )

            # Update concurrent queries plot - ensure arrays are non-empty and same length
            if self.concurrent:
                concurrent_timestamps = self.timestamps[-len(self.concurrent):]
                # Ensure both arrays have the same length
                min_length = min(len(concurrent_timestamps), len(self.concurrent))
                if min_length > 0:  # Only update if we have valid data
                    self.concurrent_line.set_data(
                        concurrent_timestamps[:min_length],
                        self.concurrent[:min_length]
                    )

            # Update error rate plot
            if self.errors:
                # Calculate rolling error rate over the last 20 requests
                window = min(20, len(self.errors))
                error_rates = []
                for i in range(len(self.errors) - window + 1):
                    error_count = sum(self.errors[i:i+window])
                    error_rates.append((error_count / window) * 100)

                # Make sure error_rates is the right length
                while len(error_rates) < len(self.timestamps):
                    error_rates.insert(0, 0)

                # Trim if necessary
                if len(error_rates) > len(self.timestamps):
                    error_rates = error_rates[-len(self.timestamps):]

                self.error_line.set_data(self.timestamps, error_rates)

            try:
                # Auto-scale axes with error protection
                self.ax_latency.relim()
                self.ax_latency.autoscale_view()

                # Only relim/autoscale if we have throughput data
                if self.throughputs and len(self.throughput_line.get_xdata()) > 0:
                    self.ax_throughput.relim()
                    self.ax_throughput.autoscale_view()

                # Only relim/autoscale if we have concurrency data
                if self.concurrent and len(self.concurrent_line.get_xdata()) > 0:
                    self.ax_concurrent.relim()
                    self.ax_concurrent.autoscale_view()

                # Only relim/autoscale if we have error rate data
                if len(self.error_line.get_xdata()) > 0:
                    self.ax_error.relim()
                    self.ax_error.autoscale_view()

            except Exception as e:
                # If there's any error in scaling, just skip it for this frame
                print(f"Warning: Error in plot scaling: {e}")

            # Update title with stats
            elapsed = current_time - self.start_time
            elapsed_since_warmup = current_time - self.warmup_end_time if self.warmup_end_time else 0

            # Skip the warmup queries in the statistics
            title = f"PostgreSQL/Citus Query Latency - {QUERY_TYPES.get(self.query_type, 'Custom query')} - "
            title += f"Pattern: {WORKLOAD_PATTERNS.get(self.workload_pattern, 'Custom')}\n"
            title += f"Time: {elapsed:.1f}s"
            if self.warmup_seconds > 0:
                title += f" (warmup: {self.warmup_seconds}s, {self.warmup_queries} queries)"

            if self.all_latencies:
                title += f" | Avg: {avg_latency:.1f}ms"
                if len(self.all_latencies) >= 20:
                    title += f" | p95: {p95_latency:.1f}ms | p99: {p99_latency:.1f}ms"

            if elapsed_since_warmup > 0:
                queries_since_warmup = self.total_queries - self.warmup_queries
                qps = queries_since_warmup / elapsed_since_warmup
                title += f" | QPS: {qps:.1f}"

            if self.read_count + self.write_count > 0:
                read_pct = (self.read_count / (self.read_count + self.write_count)) * 100
                write_pct = 100 - read_pct
                title += f" | R/W: {read_pct:.0f}%/{write_pct:.0f}%"

            if self.error_count > 0:
                error_pct = (self.error_count / (self.total_queries or 1)) * 100
                title += f" | Errors: {error_pct:.1f}%"

            # Add connection pool info
            title += f" | Conn: {self.current_connections}/{self.max_connections}"

            self.ax_latency.set_title(title)

        # Check if we've reached the total duration
        if elapsed > self.total_duration:
            self.stop_event.set()
            self._save_results()
            plt.close(self.fig)

        return (self.latency_line, self.avg_line, self.p95_line, self.p99_line,
                self.read_line, self.write_line, self.throughput_line,
                self.concurrent_line, self.error_line)

    def start_monitoring(self):
        """Start monitoring and visualization"""
        print("Starting monitoring...")
        print(f"Workload pattern: {WORKLOAD_PATTERNS.get(self.workload_pattern, 'Custom')}")
        print(f"Query type: {QUERY_TYPES.get(self.query_type, 'Custom')}")
        print(f"Read/Write ratio: {self.read_write_ratio:.1%}/{(1-self.read_write_ratio):.1%}")

        self.start_time = time.time()
        self.pattern_start_time = self.start_time
        self.warmup_end_time = self.start_time + self.warmup_seconds

        # Create and start client threads
        client_threads = []
        for i in range(self.base_client_count):
            thread = threading.Thread(target=self._run_query, args=(i,))
            thread.daemon = True
            client_threads.append(thread)
            thread.start()

        # Set up the animation
        ani = FuncAnimation(
            self.fig, self._update_plot, interval=500, blit=True, save_count=50
        )

        # Handle graceful shutdown on Ctrl+C
        def signal_handler(sig, frame):
            print("\nStopping monitoring...")
            self.stop_event.set()

            # Save results to JSON
            self._save_results()

            plt.close(self.fig)

        signal.signal(signal.SIGINT, signal_handler)

        # Display the plot
        plt.show()

        # Cleanup
        self.stop_event.set()
        for thread in client_threads:
            thread.join(timeout=1.0)

        if self.pool:
            self.pool.closeall()

    def _save_results(self):
        """Save benchmark results to a JSON file"""
        if not self.all_latencies:
            print("No data collected, skipping results save")
            return

        # Calculate result metrics
        avg_latency = sum(self.all_latencies) / len(self.all_latencies)
        min_latency = min(self.all_latencies)
        max_latency = max(self.all_latencies)
        p95_latency = np.percentile(self.all_latencies, 95) if len(self.all_latencies) >= 20 else avg_latency
        p99_latency = np.percentile(self.all_latencies, 99) if len(self.all_latencies) >= 20 else avg_latency

        # Create results directory if it doesn't exist
        os.makedirs("benchmark_results", exist_ok=True)

        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results/latency_monitor_{timestamp}.json"

        # Calculate throughput
        elapsed = time.time() - self.start_time
        elapsed_since_warmup = time.time() - self.warmup_end_time if self.warmup_end_time else elapsed
        qps = (self.total_queries - self.warmup_queries) / elapsed_since_warmup if elapsed_since_warmup > 0 else 0

        # Calculate error rate
        error_rate = (self.error_count / (self.total_queries or 1)) * 100

        # Calculate read/write stats
        total_operations = self.read_count + self.write_count
        read_percentage = (self.read_count / total_operations * 100) if total_operations > 0 else 0
        write_percentage = (self.write_count / total_operations * 100) if total_operations > 0 else 0

        # Save to file
        results = {
            "query_type": self.query_type,
            "workload_pattern": self.workload_pattern,
            "client_count": self.base_client_count,
            "read_write_ratio": f"{self.read_write_ratio:.2f}",
            "total_duration_sec": elapsed,
            "total_queries": self.total_queries,
            "warmup_queries": self.warmup_queries,
            "warmup_seconds": self.warmup_seconds,
            "latency_ms": {
                "avg": avg_latency,
                "min": min_latency,
                "max": max_latency,
                "p95": p95_latency,
                "p99": p99_latency
            },
            "throughput": {
                "qps": qps,
                "read_ops": self.read_count,
                "write_ops": self.write_count,
                "read_percentage": read_percentage,
                "write_percentage": write_percentage
            },
            "errors": {
                "count": self.error_count,
                "rate_percentage": error_rate
            },
            "connections": {
                "max": self.max_connections,
                "timeouts": self.connection_timeouts,
                "failures": self.connection_failures
            },
            "timestamp": datetime.now().isoformat()
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Results saved to {filename}")

        # Also print summary
        print("\n=== Benchmark Summary ===")
        print(f"Query Type: {QUERY_TYPES.get(self.query_type, 'Custom query')}")
        print(f"Workload Pattern: {WORKLOAD_PATTERNS.get(self.workload_pattern, 'Custom')}")
        print(f"Client Count: {self.base_client_count}")
        print(f"Total Duration: {elapsed:.2f} seconds")
        print(f"Queries Executed: {self.total_queries} (after {self.warmup_queries} warmup queries)")
        print(f"Throughput: {qps:.2f} queries/second")
        print(f"Read/Write Split: {read_percentage:.1f}%/{write_percentage:.1f}%")
        print(f"Latency - Avg: {avg_latency:.2f}ms, Min: {min_latency:.2f}ms, Max: {max_latency:.2f}ms, p95: {p95_latency:.2f}ms, p99: {p99_latency:.2f}ms")
        print(f"Errors: {self.error_count} ({error_rate:.2f}%)")
        print(f"Max Connections: {self.max_connections} (failures: {self.connection_failures}, timeouts: {self.connection_timeouts})")


def main():
    """Command line entry point"""
    parser = argparse.ArgumentParser(description="PostgreSQL/Citus Latency Monitor")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Database host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Database port (default: {DEFAULT_PORT})")
    parser.add_argument("--user", default=DEFAULT_USER, help=f"Database user (default: {DEFAULT_USER})")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="Database password")
    parser.add_argument("--dbname", default=DEFAULT_DB, help=f"Database name (default: {DEFAULT_DB})")
    parser.add_argument("--query-type", choices=list(QUERY_TYPES.keys()), default="simple",
                      help=f"Query type to benchmark (default: simple)")
    parser.add_argument("--clients", type=int, default=1,
                      help="Number of concurrent clients (default: 1)")
    parser.add_argument("--interval", type=float, default=1.0,
                      help="Interval between queries in seconds (default: 1.0)")
    parser.add_argument("--warmup", type=int, default=5,
                      help="Warmup period in seconds before collecting stats (default: 5)")
    parser.add_argument("--pattern", choices=list(WORKLOAD_PATTERNS.keys()), default="constant",
                      help=f"Workload pattern to simulate (default: constant)")
    parser.add_argument("--duration", type=int, default=60,
                      help="Total benchmark duration in seconds (default: 60)")
    parser.add_argument("--pool-size", type=int, default=10,
                      help="Connection pool size (default: 10)")
    parser.add_argument("--read-write-ratio", type=float, default=0.8,
                      help="Read/write ratio as a decimal (default: 0.8 = 80% reads)")
    parser.add_argument("--network-latency", type=float, default=0,
                      help="Simulated network latency in ms (default: 0)")
    parser.add_argument("--error-rate", type=float, default=0.0,
                      help="Simulated error rate (0.0-1.0) (default: 0)")

    args = parser.parse_args()

    # Handle 'all' query type by running benchmarks in sequence
    if args.query_type == "all":
        query_types = ["simple", "spatial", "complex", "mixed", "oltp", "olap", "realistic"]
        for qtype in query_types:
            print(f"\nRunning benchmark for query type: {qtype}")
            monitor = LatencyMonitor(
                host=args.host,
                port=args.port,
                user=args.user,
                password=args.password,
                dbname=args.dbname,
                query_type=qtype,
                client_count=args.clients,
                interval=args.interval,
                warmup_seconds=args.warmup,
                workload_pattern=args.pattern,
                duration=args.duration,
                pool_size=args.pool_size,
                read_write_ratio=args.read_write_ratio,
                network_latency=args.network_latency,
                error_rate=args.error_rate
            )
            monitor.start_monitoring()
    else:
        # Run a single benchmark
        monitor = LatencyMonitor(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            dbname=args.dbname,
            query_type=args.query_type,
            client_count=args.clients,
            interval=args.interval,
            warmup_seconds=args.warmup,
            workload_pattern=args.pattern,
            duration=args.duration,
            pool_size=args.pool_size,
            read_write_ratio=args.read_write_ratio,
            network_latency=args.network_latency,
            error_rate=args.error_rate
        )
        monitor.start_monitoring()


if __name__ == "__main__":
    main()