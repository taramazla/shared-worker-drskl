#!/usr/bin/env python3
import time
import random
import os
import statistics
import json
from datetime import datetime
from collections import deque
import threading
import psycopg2
from locust import User, task, between, events
from locust.exception import StopUser
from locust import LoadTestShape

# Database connection parameters
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "citus")
DB_PASSWORD = os.getenv("DB_PASSWORD", "citus")
DB_NAME = os.getenv("DB_NAME", "citus")
CONTAINER = os.getenv("CONTAINER", "citus_loadbalancer")

# Task weights and settings
PURCHASE_WEIGHT = int(os.getenv("PURCHASE_WEIGHT", "80"))  # Purchase attempts
VIEW_WEIGHT = int(os.getenv("VIEW_WEIGHT", "20"))  # Product views

# Use transaction isolation level
USE_TRANSACTION_ISOLATION = os.getenv("USE_TRANSACTION_ISOLATION", "true").lower() == "true"
ISOLATION_LEVEL = os.getenv("ISOLATION_LEVEL", "SERIALIZABLE")  # Options: READ COMMITTED, REPEATABLE READ, SERIALIZABLE

# Concurrent connection settings
USER_COUNT = int(os.getenv("USER_COUNT", "100"))
MAX_CONCURRENT_QUERIES = int(os.getenv("MAX_CONCURRENT_QUERIES", str(USER_COUNT)))
CONCURRENT_MODE = os.getenv("CONCURRENT_MODE", "true").lower() == "true"

# Create a semaphore to limit concurrent connections
concurrent_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_QUERIES)
metrics_lock = threading.Lock()

# Print the operation mode
print(f"Running E-commerce Stock Benchmark: Simulating concurrent stock updates")
print(f"Transaction isolation level: {'ENABLED - ' + ISOLATION_LEVEL if USE_TRANSACTION_ISOLATION else 'DISABLED'}")

# Print concurrent connection mode
print(f"Concurrent mode: {'ENABLED' if CONCURRENT_MODE else 'DISABLED'}")
print(f"Maximum concurrent connections: {MAX_CONCURRENT_QUERIES}")

# Store metrics for custom reporting
custom_metrics = {
    "purchase_latencies": [],
    "view_latencies": [],
    "successful_purchases": 0,
    "failed_purchases": 0,
    "total_attempts": 0,
    "oversold_items": 0,  # Tracks number of times stock went below 0
    "start_stock": 50,
    "current_stock": 50,
    "expected_remaining": 50,  # Expected stock if all successful purchases are processed correctly
    "errors": [],
    "start_time": None,
    "recent_response_times": deque(maxlen=100),
    "error_rate": 0.0,
    "concurrent_queries": 0,
    "max_concurrent": 0,
}

# Dynamic load parameters
BASE_MEAN_WAIT = float(os.getenv("BASE_MEAN_WAIT", "1.0"))  # Base mean wait time in seconds
current_mean_wait = BASE_MEAN_WAIT

# Function to establish a database connection
def get_connection():
    """Get a connection to the PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

# Function to create the product_inventory table
def ensure_product_table_exists():
    """Create the product_inventory table if it doesn't exist"""
    max_attempts = 3

    for attempt in range(max_attempts):
        conn = None
        try:
            print(f"Checking product_inventory table (attempt {attempt+1}/{max_attempts})...")
            conn = get_connection()

            # Check if table exists
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'product_inventory'
                    );
                """)
                table_exists = cursor.fetchone()[0]

                if not table_exists:
                    print("Creating product_inventory table...")

                    # Create the table
                    cursor.execute("""
                        CREATE TABLE product_inventory (
                          product_id INTEGER PRIMARY KEY,
                          product_name TEXT NOT NULL,
                          stock INTEGER NOT NULL,
                          region_code TEXT NOT NULL,
                          last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        );
                    """)
                    conn.commit()

                    # Create index
                    cursor.execute("""
                        CREATE INDEX idx_product_inventory_region
                        ON product_inventory (region_code);
                    """)
                    conn.commit()

                    # Add sample products including our test product
                    print("Adding sample products including test product ID 987...")
                    cursor.execute("""
                        INSERT INTO product_inventory (product_id, product_name, stock, region_code)
                        VALUES
                            (987, 'Limited Edition Widget', 50, 'region_central'),
                            (101, 'Regular Widget A', 500, 'region_north'),
                            (102, 'Regular Widget B', 750, 'region_south'),
                            (103, 'Premium Widget', 250, 'region_central');
                    """)
                    conn.commit()

                    # Verify the table was created
                    cursor.execute("SELECT COUNT(*) FROM product_inventory;")
                    row_count = cursor.fetchone()[0]

                    if row_count > 0:
                        table_exists = True
                        print(f"Successfully created product_inventory table with {row_count} products")
                    else:
                        print("Table created but no products were inserted")
                else:
                    # Check if our test product exists
                    cursor.execute("SELECT stock FROM product_inventory WHERE product_id = 987;")
                    result = cursor.fetchone()

                    if not result:
                        print("Test product does not exist. Adding it...")
                        cursor.execute("""
                            INSERT INTO product_inventory (product_id, product_name, stock, region_code)
                            VALUES (987, 'Limited Edition Widget', 50, 'region_central');
                        """)
                        conn.commit()
                    else:
                        # Reset the stock to 50
                        cursor.execute("UPDATE product_inventory SET stock = 50 WHERE product_id = 987;")
                        conn.commit()
                        print(f"Test product exists. Reset stock to 50 units.")

                    # Print current stock of test product
                    cursor.execute("SELECT stock FROM product_inventory WHERE product_id = 987;")
                    stock = cursor.fetchone()[0]
                    print(f"Current stock of product #987: {stock} units")

            # Distribute the table if using Citus
            try:
                cursor.execute("SELECT create_distributed_table('product_inventory', 'region_code');")
                conn.commit()
                print("Table distributed successfully")
            except Exception as e:
                print(f"Note: Could not distribute table (this is fine if not using Citus): {e}")
                conn.rollback()

            # If we get to this point, we're good
            return True

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

    # If we get here, we've failed all attempts
    print("Failed to ensure product table exists after multiple attempts")
    return False

# Function to reset the test product's stock
def reset_product_stock():
    """Reset the stock of the test product to 50 units"""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE product_inventory SET stock = 50 WHERE product_id = 987;")
            conn.commit()

            # Reset metrics
            with metrics_lock:
                custom_metrics["current_stock"] = 50
                custom_metrics["expected_remaining"] = 50
                custom_metrics["successful_purchases"] = 0
                custom_metrics["failed_purchases"] = 0
                custom_metrics["oversold_items"] = 0
                custom_metrics["total_attempts"] = 0

        print("Reset product #987 stock to 50 units and cleared metrics")
        return True
    except Exception as e:
        print(f"Error resetting product stock: {e}")
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# Execute a database query with proper instrumentation
def execute_query(query, params=None, query_type="view"):
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
                custom_metrics["max_concurrent"] = max(
                    custom_metrics["max_concurrent"],
                    custom_metrics["concurrent_queries"]
                )

        # Get a connection
        conn = get_connection()

        # Set isolation level if using transaction isolation
        if USE_TRANSACTION_ISOLATION:
            conn.set_session(isolation_level=ISOLATION_LEVEL)

        # Start timing
        start_time = time.time()

        # Execute query
        with conn.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch results for view operations
            if query_type == "view":
                results = cursor.fetchall()
                count = len(results)
            else:  # For purchase operations
                conn.commit()
                count = cursor.rowcount

        # End timing and calculate latency
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000

        # Add to recent response times
        with metrics_lock:
            custom_metrics["recent_response_times"].append(latency_ms)

            # Store latency in custom metrics
            if query_type == "view":
                custom_metrics["view_latencies"].append(latency_ms)
            elif query_type == "purchase":
                custom_metrics["purchase_latencies"].append(latency_ms)

        # Return the result count and latency
        return count, latency_ms, None  # None means no error

    except Exception as e:
        # Calculate latency even for failed operations
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000 if 'start_time' in locals() else 0

        # Record error
        with metrics_lock:
            custom_metrics["errors"].append(str(e))
            custom_metrics["recent_response_times"].append(latency_ms)
            custom_metrics["error_rate"] = len(custom_metrics["errors"]) / (
                custom_metrics["total_attempts"] + 1  # +1 because we haven't incremented total_attempts yet
            )

        # Return the error
        return 0, latency_ms, e

    finally:
        # Release resources
        if conn:
            try:
                conn.close()
            except:
                pass

        if CONCURRENT_MODE and acquired_semaphore:
            with metrics_lock:
                custom_metrics["concurrent_queries"] -= 1
            concurrent_semaphore.release()

# Function to attempt product purchase with different concurrency control mechanisms
def attempt_purchase(product_id, quantity=1):
    """Attempt to purchase a product if stock is available"""
    conn = None
    try:
        conn = get_connection()

        # Set isolation level if enabled
        if USE_TRANSACTION_ISOLATION:
            conn.set_session(isolation_level=ISOLATION_LEVEL)

        with conn.cursor() as cursor:
            # Start a transaction
            if USE_TRANSACTION_ISOLATION:
                # With proper isolation, we first check stock and then update in one transaction
                cursor.execute(
                    "SELECT stock FROM product_inventory WHERE product_id = %s FOR UPDATE;",
                    (product_id,)
                )
                current_stock = cursor.fetchone()[0]

                if current_stock >= quantity:
                    # Stock available, proceed with purchase
                    cursor.execute(
                        "UPDATE product_inventory SET stock = stock - %s, last_updated = NOW() WHERE product_id = %s;",
                        (quantity, product_id)
                    )
                    conn.commit()

                    # Update metrics
                    with metrics_lock:
                        custom_metrics["successful_purchases"] += 1
                        custom_metrics["current_stock"] = current_stock - quantity
                        custom_metrics["expected_remaining"] -= quantity

                    return True, current_stock - quantity, None
                else:
                    # Not enough stock
                    conn.rollback()

                    # Update metrics
                    with metrics_lock:
                        custom_metrics["failed_purchases"] += 1

                    return False, current_stock, "Insufficient stock"
            else:
                # Without proper isolation - vulnerable to race conditions
                # First check if stock is available
                cursor.execute(
                    "SELECT stock FROM product_inventory WHERE product_id = %s;",
                    (product_id,)
                )
                current_stock = cursor.fetchone()[0]

                if current_stock >= quantity:
                    # Apply a small delay to simulate thinking time and increase race condition chance
                    time.sleep(0.05)

                    # Stock available, proceed with purchase
                    cursor.execute(
                        "UPDATE product_inventory SET stock = stock - %s, last_updated = NOW() WHERE product_id = %s;",
                        (quantity, product_id)
                    )
                    conn.commit()

                    # Check if we went below zero (overselling)
                    cursor.execute(
                        "SELECT stock FROM product_inventory WHERE product_id = %s;",
                        (product_id,)
                    )
                    new_stock = cursor.fetchone()[0]

                    # Update metrics
                    with metrics_lock:
                        custom_metrics["successful_purchases"] += 1
                        custom_metrics["current_stock"] = new_stock
                        custom_metrics["expected_remaining"] -= quantity

                        # Check for overselling
                        if new_stock < 0:
                            custom_metrics["oversold_items"] += 1

                    return True, new_stock, None
                else:
                    # Not enough stock
                    conn.rollback()

                    # Update metrics
                    with metrics_lock:
                        custom_metrics["failed_purchases"] += 1

                    return False, current_stock, "Insufficient stock"

    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        return False, None, str(e)

    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

# Save metrics to a file
def save_metrics():
    """Save metrics to a JSON file"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Calculate durations and rates
    duration = time.time() - (custom_metrics.get("start_time") or time.time())
    if duration > 0:
        purchase_rate = custom_metrics["successful_purchases"] / duration
    else:
        purchase_rate = 0

    # Calculate success rates
    total_attempts = custom_metrics["successful_purchases"] + custom_metrics["failed_purchases"]
    if total_attempts > 0:
        success_rate = custom_metrics["successful_purchases"] / total_attempts
    else:
        success_rate = 0

    # Create metrics directory if it doesn't exist
    results_dir = "./benchmark_results"
    os.makedirs(results_dir, exist_ok=True)

    # Save to file
    with open(f"{results_dir}/ecommerce_metrics_{timestamp}.json", "w") as f:
        json.dump({
            "config": {
                "isolation_level": ISOLATION_LEVEL if USE_TRANSACTION_ISOLATION else "NONE",
                "concurrent_mode": CONCURRENT_MODE,
                "max_concurrent_queries": MAX_CONCURRENT_QUERIES,
                "user_count": USER_COUNT,
                "initial_stock": custom_metrics["start_stock"]
            },
            "results": {
                "duration_seconds": duration,
                "purchase_latency_ms": {
                    "avg": statistics.mean(custom_metrics["purchase_latencies"]) if custom_metrics["purchase_latencies"] else 0,
                    "min": min(custom_metrics["purchase_latencies"]) if custom_metrics["purchase_latencies"] else 0,
                    "max": max(custom_metrics["purchase_latencies"]) if custom_metrics["purchase_latencies"] else 0,
                    "p95": statistics.quantiles(custom_metrics["purchase_latencies"], n=20)[18] if len(custom_metrics["purchase_latencies"]) > 19 else 0
                },
                "view_latency_ms": {
                    "avg": statistics.mean(custom_metrics["view_latencies"]) if custom_metrics["view_latencies"] else 0,
                    "min": min(custom_metrics["view_latencies"]) if custom_metrics["view_latencies"] else 0,
                    "max": max(custom_metrics["view_latencies"]) if custom_metrics["view_latencies"] else 0,
                    "p95": statistics.quantiles(custom_metrics["view_latencies"], n=20)[18] if len(custom_metrics["view_latencies"]) > 19 else 0
                },
                "purchase_rate_per_second": purchase_rate,
                "final_stock": custom_metrics["current_stock"],
                "expected_stock": custom_metrics["expected_remaining"],
                "successful_purchases": custom_metrics["successful_purchases"],
                "failed_purchases": custom_metrics["failed_purchases"],
                "total_attempts": total_attempts,
                "oversold_items": custom_metrics["oversold_items"],
                "success_rate": success_rate,
                "stock_integrity_maintained": custom_metrics["current_stock"] >= 0 and custom_metrics["current_stock"] == custom_metrics["expected_remaining"],
                "max_concurrent_connections": custom_metrics["max_concurrent"],
                "timestamp": datetime.now().isoformat()
            }
        }, f, indent=2)

    print(f"Metrics saved to {results_dir}/ecommerce_metrics_{timestamp}.json")

# Locust user class
class EcommerceUser(User):
    wait_time = between(0.5, 2)

    def on_start(self):
        # Initialize database if this is the first user
        if not hasattr(self.environment, "db_initialized"):
            if ensure_product_table_exists():
                reset_product_stock()
                self.environment.db_initialized = True

                # Set start time
                with metrics_lock:
                    custom_metrics["start_time"] = time.time()
            else:
                raise Exception("Could not initialize database")

    @task(VIEW_WEIGHT)
    def view_product(self):
        """View product details including stock level"""
        try:
            # Simple view of a product
            query = "SELECT product_id, product_name, stock FROM product_inventory WHERE product_id = %s"
            count, latency, error = execute_query(query, (987,), "view")

            # Fire request event
            if error:
                self.environment.events.request.fire(
                    request_type="View",
                    name="View Product",
                    response_time=latency,
                    response_length=0,
                    exception=error
                )
            else:
                self.environment.events.request.fire(
                    request_type="View",
                    name="View Product",
                    response_time=latency,
                    response_length=count,
                    exception=None
                )
        except Exception as e:
            self.environment.events.request.fire(
                request_type="View",
                name="View Product",
                response_time=0,
                response_length=0,
                exception=e
            )

    @task(PURCHASE_WEIGHT)
    def purchase_product(self):
        """Attempt to purchase a product"""
        try:
            # Track the attempt
            with metrics_lock:
                custom_metrics["total_attempts"] += 1

            # Choose a random quantity between 1 and 3
            quantity = random.randint(1, 3)

            # Measure latency ourselves to include the whole operation
            start_time = time.time()

            # Attempt the purchase
            success, new_stock, error = attempt_purchase(987, quantity)

            # Calculate latency
            latency_ms = (time.time() - start_time) * 1000

            # Fire request event
            if error:
                self.environment.events.request.fire(
                    request_type="Purchase",
                    name="Purchase Product",
                    response_time=latency_ms,
                    response_length=0,
                    exception=error
                )
            else:
                self.environment.events.request.fire(
                    request_type="Purchase",
                    name="Purchase Product",
                    response_time=latency_ms,
                    response_length=quantity if success else 0,
                    exception=None if success else Exception("Purchase failed - out of stock")
                )

        except Exception as e:
            self.environment.events.request.fire(
                request_type="Purchase",
                name="Purchase Product",
                response_time=0,
                response_length=0,
                exception=e
            )

# Load shape class for constant load
class ConstantLoadShape(LoadTestShape):
    """Maintains a constant number of users throughout the test."""
    def __init__(self):
        # Default user count and spawn rate
        self.user_count = int(os.getenv("USER_COUNT", "100"))
        self.spawn_rate = int(os.getenv("SPAWN_RATE", "10"))
        self.test_duration = int(os.getenv("TEST_DURATION", "60"))  # 60 seconds by default
        print(f"Running constant load with {self.user_count} users, spawn rate of {self.spawn_rate}/s for {self.test_duration}s")

    def tick(self):
        # Determine elapsed time since test start
        start = custom_metrics.get("start_time") or time.time()
        elapsed = time.time() - start

        # Stop after test duration
        if elapsed >= self.test_duration:
            # Save metrics at the end
            save_metrics()
            print("Test duration reached. Stopping test.")
            return None

        return (self.user_count, self.spawn_rate)

# Hook into Locust events
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    """Initialize the test environment"""
    print("Initializing E-commerce Stock Benchmark")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when the test is starting"""
    print("E-commerce Stock Benchmark starting")
    # Reset metrics
    with metrics_lock:
        custom_metrics["start_time"] = time.time()

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when the test is stopping"""
    print("E-commerce Stock Benchmark stopping")

    # Get final stock value from database
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT stock FROM product_inventory WHERE product_id = 987;")
            final_stock = cursor.fetchone()[0]

            # Update metrics
            with metrics_lock:
                custom_metrics["current_stock"] = final_stock

        conn.close()
    except Exception as e:
        print(f"Error getting final stock: {e}")

    # Calculate and display summary
    total_purchases = custom_metrics["successful_purchases"]
    oversold = custom_metrics["oversold_items"]
    expected_remaining = 50 - total_purchases
    actual_remaining = custom_metrics["current_stock"]

    print("\n===== BENCHMARK RESULTS =====")
    print(f"Initial stock: 50 units")
    print(f"Successful purchases: {total_purchases}")
    print(f"Failed purchases: {custom_metrics['failed_purchases']}")
    print(f"Oversold items (stock went negative): {oversold}")
    print(f"Expected remaining stock: {expected_remaining}")
    print(f"Actual remaining stock: {actual_remaining}")
    print(f"Stock integrity {'MAINTAINED' if actual_remaining == expected_remaining and actual_remaining >= 0 else 'VIOLATED'}")
    print(f"Transaction isolation: {'ENABLED - ' + ISOLATION_LEVEL if USE_TRANSACTION_ISOLATION else 'DISABLED'}")
    print("============================")

    # Save metrics
    save_metrics()

# For running standalone
if __name__ == "__main__":
    # Ensure product table exists
    ensure_product_table_exists()
    reset_product_stock()

    print("Ready to run E-commerce Stock Benchmark with Locust")
    print("Use: locust -f ecommerce_stock_benchmark.py")
