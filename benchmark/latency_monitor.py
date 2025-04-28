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
    "all": "Run all query types"
}

# Chart settings
MAX_DATA_POINTS = 100
COLORS = {
    "latency": "#1f77b4",  # Blue
    "throughput": "#2ca02c",  # Green
    "avg_latency": "#ff7f0e",  # Orange
    "p95_latency": "#d62728",  # Red
    "concurrent": "#9467bd"  # Purple
}

# SQL statements to create test tables
CREATE_TEST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    name TEXT,
    value FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_SPATIAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS test_locations (
    id SERIAL PRIMARY KEY,
    name TEXT,
    location GEOMETRY(Point, 4326),
    region_code TEXT,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

INSERT_TEST_DATA_SQL = """
INSERT INTO test_table (name, value)
SELECT
    'test_' || i,
    random() * 100
FROM
    generate_series(1, 1000) i
ON CONFLICT DO NOTHING;
"""

INSERT_SPATIAL_DATA_SQL = """
INSERT INTO test_locations (name, location, region_code)
SELECT
    'poi_' || i,
    ST_SetSRID(ST_MakePoint(
        -74.0 + random() * 0.5,
        40.7 + random() * 0.5),
    4326),
    'region_' || (i % 10)
FROM
    generate_series(1, 1000) i
ON CONFLICT DO NOTHING;
"""

# SQL to verify PostGIS and Citus extensions
CHECK_EXTENSIONS_SQL = """
SELECT extname FROM pg_extension WHERE extname IN ('postgis', 'citus');
"""

class LatencyMonitor:
    """Monitor and visualize PostgreSQL/Citus query latency in real-time"""

    def __init__(self, host, port, user, password, dbname, query_type="simple",
                 client_count=1, interval=1.0, warmup_seconds=5):
        """Initialize the latency monitor"""
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname
        self.query_type = query_type
        self.client_count = client_count
        self.interval = interval
        self.warmup_seconds = warmup_seconds

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

        # Communication between data collection threads and visualization
        self.data_queue = queue.Queue()
        self.stop_event = threading.Event()

        # Initialize connection
        self.conn = self._connect()

        # Check and setup test tables
        self._setup_test_tables()

        # Initialize plots
        self._init_plots()

    def _connect(self):
        """Connect to the PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            print(f"Connected to PostgreSQL on {self.host}:{self.port}")
            return conn
        except Exception as e:
            print(f"Connection failed: {e}")
            raise

    def _check_extensions(self):
        """Verify that required extensions are installed"""
        cursor = self.conn.cursor()
        cursor.execute(CHECK_EXTENSIONS_SQL)
        installed_extensions = [row[0] for row in cursor.fetchall()]
        cursor.close()

        print(f"Detected extensions: {', '.join(installed_extensions)}")

        # Check if PostGIS is needed and available
        if self.query_type in ["spatial", "all"] and "postgis" not in installed_extensions:
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
        cursor = self.conn.cursor()

        try:
            # Create basic test table
            print("Setting up test tables if they don't exist...")
            cursor.execute(CREATE_TEST_TABLE_SQL)

            # Check if we need spatial tables
            if "postgis" in extensions and self.query_type in ["spatial", "all"]:
                cursor.execute(CREATE_SPATIAL_TABLE_SQL)

                # Insert spatial test data
                cursor.execute("SELECT COUNT(*) FROM test_locations")
                count = cursor.fetchone()[0]
                if count < 100:
                    print("Inserting spatial test data...")
                    cursor.execute(INSERT_SPATIAL_DATA_SQL)

            # Insert basic test data
            cursor.execute("SELECT COUNT(*) FROM test_table")
            count = cursor.fetchone()[0]
            if count < 100:
                print("Inserting test data...")
                cursor.execute(INSERT_TEST_DATA_SQL)

            self.conn.commit()
            print(f"Test tables are ready. test_table has {count} rows.")

        except Exception as e:
            self.conn.rollback()
            print(f"Error setting up test tables: {e}")
            # If spatial query type was requested but we can't set up tables,
            # fall back to simple queries
            if self.query_type == "spatial":
                self.query_type = "simple"
                print("Falling back to simple queries.")
        finally:
            cursor.close()

    def _init_plots(self):
        """Initialize the matplotlib plots"""
        self.fig = plt.figure(figsize=(12, 10))
        gs = gridspec.GridSpec(3, 1, height_ratios=[2, 1, 1])

        # Latency plot
        self.ax_latency = plt.subplot(gs[0])
        self.latency_line, = self.ax_latency.plot([], [], color=COLORS["latency"], label="Query Latency (ms)")
        self.avg_line, = self.ax_latency.plot([], [], color=COLORS["avg_latency"], linestyle='--', label="Avg Latency")
        self.p95_line, = self.ax_latency.plot([], [], color=COLORS["p95_latency"], linestyle='-.', label="p95 Latency")
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

        # Set y-axis to be integer only for concurrency
        self.ax_concurrent.yaxis.set_major_locator(MaxNLocator(integer=True))

        plt.tight_layout()

    def _get_query(self):
        """Get the appropriate query based on the query type"""
        if self.query_type == "simple":
            return "SELECT COUNT(*) FROM test_table"
        elif self.query_type == "spatial":
            # Random parameters for a spatial query
            lat = 40.7 + random.uniform(-0.2, 0.2)
            lon = -74.0 + random.uniform(-0.2, 0.2)
            radius = random.randint(1000, 10000)  # meters

            return f"""
                SELECT COUNT(*)
                FROM test_locations
                WHERE ST_DWithin(
                    location::geography,
                    ST_SetSRID(ST_MakePoint({lon}, {lat}), 4326)::geography,
                    {radius}
                )
            """
        elif self.query_type == "complex":
            days_ago = random.randint(1, 30)
            cutoff_date = datetime.now() - timedelta(days=days_ago)
            cutoff_date_str = cutoff_date.strftime("%Y-%m-%d")

            return f"""
                SELECT
                    DATE_TRUNC('day', created_at) AS day,
                    COUNT(*) AS count,
                    AVG(value) AS avg_value,
                    MAX(value) AS max_value
                FROM
                    test_table
                GROUP BY
                    DATE_TRUNC('day', created_at)
                ORDER BY
                    day DESC
                LIMIT 100
            """
        else:
            return "SELECT 1"  # Fallback default query

    def _run_query(self, client_id):
        """Run the specified query type and measure its latency"""
        cursor = self.conn.cursor()
        query = self._get_query()

        while not self.stop_event.is_set():
            try:
                # Signal that we're starting a query
                self.current_concurrent += 1

                # Execute query and measure time
                start_time = time.time()
                cursor.execute(query)
                results = cursor.fetchall()
                end_time = time.time()

                # Calculate latency in milliseconds
                latency_ms = (end_time - start_time) * 1000

                # Put results in queue for visualization thread
                timestamp = time.time()
                if self.warmup_end_time and timestamp >= self.warmup_end_time:
                    self.data_queue.put((timestamp, latency_ms, client_id))
                else:
                    self.warmup_queries += 1

                # Signal that we're done with this query
                self.current_concurrent -= 1

                # Sleep between queries
                time.sleep(self.interval)

            except Exception as e:
                self.current_concurrent -= 1
                print(f"Query error (client {client_id}): {e}")
                time.sleep(self.interval * 2)  # Wait longer after an error

    def _update_plot(self, frame):
        """Update the visualization with new data points"""
        # Get all available data from the queue
        current_time = time.time()
        while not self.data_queue.empty():
            timestamp, latency_ms, client_id = self.data_queue.get()

            # Use relative time (seconds since start) for x-axis
            relative_time = timestamp - self.start_time

            self.timestamps.append(relative_time)
            self.latencies.append(latency_ms)
            self.all_latencies.append(latency_ms)

            # Only calculate metrics when we have data
            self.total_queries += 1

        # Limit the data points for better visualization
        if len(self.timestamps) > MAX_DATA_POINTS:
            self.timestamps = self.timestamps[-MAX_DATA_POINTS:]
            self.latencies = self.latencies[-MAX_DATA_POINTS:]

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
            except Exception as e:
                # If there's any error in scaling, just skip it for this frame
                print(f"Warning: Error in plot scaling: {e}")

            # Update title with stats
            elapsed = current_time - self.start_time
            elapsed_since_warmup = current_time - self.warmup_end_time if self.warmup_end_time else 0

            # Skip the warmup queries in the statistics
            title = f"PostgreSQL/Citus Query Latency - {QUERY_TYPES.get(self.query_type, 'Custom query')}\n"
            title += f"Time: {elapsed:.1f}s"
            if self.warmup_seconds > 0:
                title += f" (warmup: {self.warmup_seconds}s, {self.warmup_queries} queries)"

            if self.all_latencies:
                title += f" | Avg: {avg_latency:.1f}ms"
                if len(self.all_latencies) >= 20:
                    title += f" | p95: {p95_latency:.1f}ms"

            if elapsed_since_warmup > 0:
                queries_since_warmup = self.total_queries
                qps = queries_since_warmup / elapsed_since_warmup
                title += f" | Overall QPS: {qps:.1f}"

            self.ax_latency.set_title(title)

        return self.latency_line, self.avg_line, self.p95_line, self.throughput_line, self.concurrent_line

    def start_monitoring(self):
        """Start monitoring and visualization"""
        print("Starting monitoring...")
        self.start_time = time.time()
        self.warmup_end_time = self.start_time + self.warmup_seconds

        # Create and start client threads
        client_threads = []
        for i in range(self.client_count):
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

        if self.conn:
            self.conn.close()

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

        # Create results directory if it doesn't exist
        os.makedirs("benchmark_results", exist_ok=True)

        # Create timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results/latency_monitor_{timestamp}.json"

        # Calculate throughput
        elapsed = time.time() - self.start_time
        elapsed_since_warmup = time.time() - self.warmup_end_time if self.warmup_end_time else elapsed
        qps = self.total_queries / elapsed_since_warmup if elapsed_since_warmup > 0 else 0

        # Save to file
        results = {
            "query_type": self.query_type,
            "client_count": self.client_count,
            "total_duration_sec": elapsed,
            "total_queries": self.total_queries,
            "warmup_queries": self.warmup_queries,
            "warmup_seconds": self.warmup_seconds,
            "latency_ms": {
                "avg": avg_latency,
                "min": min_latency,
                "max": max_latency,
                "p95": p95_latency
            },
            "throughput_qps": qps,
            "timestamp": datetime.now().isoformat()
        }

        with open(filename, "w") as f:
            json.dump(results, f, indent=2)

        print(f"Results saved to {filename}")

        # Also print summary
        print("\n=== Benchmark Summary ===")
        print(f"Query Type: {QUERY_TYPES.get(self.query_type, 'Custom query')}")
        print(f"Client Count: {self.client_count}")
        print(f"Total Duration: {elapsed:.2f} seconds")
        print(f"Queries Executed: {self.total_queries} (after {self.warmup_queries} warmup queries)")
        print(f"Throughput: {qps:.2f} queries/second")
        print(f"Latency - Avg: {avg_latency:.2f}ms, Min: {min_latency:.2f}ms, Max: {max_latency:.2f}ms, p95: {p95_latency:.2f}ms")


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

    args = parser.parse_args()

    # Handle 'all' query type by running benchmarks in sequence
    if args.query_type == "all":
        query_types = ["simple", "spatial", "complex"]
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
                warmup_seconds=args.warmup
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
            warmup_seconds=args.warmup
        )
        monitor.start_monitoring()


if __name__ == "__main__":
    main()