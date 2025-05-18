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
PURCHASE_WEIGHT = 100  # 100% update (purchase) operations
VIEW_WEIGHT = 0        # no view operations in this scenario

# Use transaction isolation level
USE_TRANSACTION_ISOLATION = os.getenv("USE_TRANSACTION_ISOLATION", "true").lower() == "true"
ISOLATION_LEVEL = os.getenv("ISOLATION_LEVEL", "SERIALIZABLE")  # Options: READ COMMITTED, REPEATABLE READ, SERIALIZABLE

# Concurrent connection settings
USER_COUNT = int(os.getenv("USER_COUNT", "100"))
MAX_CONCURRENT_QUERIES = int(os.getenv("MAX_CONCURRENT_QUERIES", str(USER_COUNT)))
CONCURRENT_MODE = os.getenv("CONCURRENT_MODE", "true").lower() == "true"

# Semaphore to limit concurrent connections
concurrent_semaphore = threading.BoundedSemaphore(MAX_CONCURRENT_QUERIES)
metrics_lock = threading.Lock()

print(f"Running E-commerce Stock Benchmark: Simulating concurrent stock updates only")
print(f"Transaction isolation level: {'ENABLED - ' + ISOLATION_LEVEL if USE_TRANSACTION_ISOLATION else 'DISABLED'}")
print(f"Concurrent mode: {'ENABLED' if CONCURRENT_MODE else 'DISABLED'}")
print(f"Maximum concurrent connections: {MAX_CONCURRENT_QUERIES}")

# Store metrics for custom reporting
custom_metrics = {
    "purchase_latencies": [],
    "successful_purchases": 0,
    "failed_purchases": 0,
    "total_attempts": 0,
    "oversold_items": 0,
    "start_stock": 50,
    "current_stock": 50,
    "expected_remaining": 50,
    "errors": [],
    "start_time": time.time(),  # Initialize with current time instead of None
    "recent_response_times": deque(maxlen=100),
    "max_concurrent": 0,
}

# Function to get a DB connection
def get_connection():
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        raise

# Ensure table exists and is initialized
def ensure_product_table_exists():
    max_attempts = 3
    for attempt in range(max_attempts):
        conn = None
        try:
            print(f"Checking product_inventory table (attempt {attempt+1}/{max_attempts})...")
            conn = get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'product_inventory'
                    );
                """)
                if not cursor.fetchone()[0]:
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
                    cursor.execute("""
                        INSERT INTO product_inventory (product_id, product_name, stock, region_code)
                        VALUES
                            (987, 'Limited Edition Widget', 50, 'region_central');
                    """)
                    conn.commit()
                else:
                    cursor.execute("SELECT stock FROM product_inventory WHERE product_id = 987;")
                    if not cursor.fetchone():
                        cursor.execute("""
                            INSERT INTO product_inventory (product_id, product_name, stock, region_code)
                            VALUES (987, 'Limited Edition Widget', 50, 'region_central');
                        """)
                        conn.commit()
                    else:
                        cursor.execute("UPDATE product_inventory SET stock = 50 WHERE product_id = 987;")
                        conn.commit()
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT create_distributed_table('product_inventory','region_code');")
                    conn.commit()
            except Exception:
                conn.rollback()
            return True
        except Exception as e:
            print(f"Error in table setup (attempt {attempt+1}): {e}")
            if conn:
                conn.rollback()
            if attempt < max_attempts-1:
                time.sleep(2**attempt)
        finally:
            if conn:
                conn.close()
    print("Failed to ensure table exists after multiple attempts")
    return False

# Reset stock and metrics
def reset_product_stock():
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("UPDATE product_inventory SET stock = 50 WHERE product_id = 987;")
            conn.commit()
        with metrics_lock:
            custom_metrics.update({
                "current_stock":50,
                "expected_remaining":50,
                "successful_purchases":0,
                "failed_purchases":0,
                "oversold_items":0,
                "total_attempts":0,
                "purchase_latencies":[]
            })
        print("Reset product #987 stock to 50 and cleared metrics")
        return True
    except Exception as e:
        print(f"Error resetting stock: {e}")
        return False
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# Attempt purchase (UPDATE) with isolation
def attempt_purchase(product_id, quantity=1):
    conn = None
    try:
        conn = get_connection()
        if USE_TRANSACTION_ISOLATION:
            conn.set_session(isolation_level=ISOLATION_LEVEL)
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT stock FROM product_inventory WHERE product_id = %s FOR UPDATE;",
                (product_id,)
            )
            stock = cursor.fetchone()[0]
            if stock >= quantity:
                cursor.execute(
                    "UPDATE product_inventory SET stock = stock - %s, last_updated = NOW() WHERE product_id = %s;",
                    (quantity, product_id)
                )
                conn.commit()
                with metrics_lock:
                    custom_metrics["successful_purchases"] += 1
                    custom_metrics["current_stock"] = stock - quantity
                    custom_metrics["expected_remaining"] -= quantity
                return True, stock-quantity, None
            else:
                conn.rollback()
                with metrics_lock:
                    custom_metrics["failed_purchases"] += 1
                return False, stock, "Insufficient stock"
    except Exception as e:
        if conn:
            conn.rollback()
        return False, None, str(e)
    finally:
        if conn:
            conn.close()

# Save metrics to JSON including throughput & latency
def save_metrics():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    start_time = custom_metrics["start_time"] if custom_metrics["start_time"] is not None else time.time()
    duration = time.time() - start_time
    purchase_rate = (custom_metrics["successful_purchases"] / duration) if duration>0 else 0
    avg_lat = statistics.mean(custom_metrics["purchase_latencies"]) if custom_metrics["purchase_latencies"] else 0
    p95_lat = statistics.quantiles(custom_metrics["purchase_latencies"], n=20)[18] \
        if len(custom_metrics["purchase_latencies"])>19 else 0

    results = {
        "config": {
            "isolation_level": ISOLATION_LEVEL if USE_TRANSACTION_ISOLATION else "NONE",
            "concurrent_mode": CONCURRENT_MODE,
            "max_concurrent_queries": MAX_CONCURRENT_QUERIES,
            "user_count": USER_COUNT,
            "initial_stock": custom_metrics["start_stock"]
        },
        "results": {
            "duration_seconds": duration,
            "throughput_ops_per_sec": purchase_rate,
            "avg_purchase_latency_ms": avg_lat,
            "p95_purchase_latency_ms": p95_lat,
            "successful_purchases": custom_metrics["successful_purchases"],
            "failed_purchases": custom_metrics["failed_purchases"],
            "oversold_items": custom_metrics["oversold_items"],
            "final_stock": custom_metrics["current_stock"],
            "stock_integrity_maintained": custom_metrics["current_stock"] >= 0
        }
    }

    os.makedirs("./benchmark_results", exist_ok=True)
    path = f"./benchmark_results/ecommerce_metrics_{timestamp}.json"
    with open(path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Metrics saved to {path}")

# Locust user class
class EcommerceUser(User):
    wait_time = between(0.5, 2)

    def on_start(self):
        if not hasattr(self.environment, "db_initialized"):
            if ensure_product_table_exists() and reset_product_stock():
                self.environment.db_initialized = True
                custom_metrics["start_time"] = time.time()
            else:
                raise Exception("DB init failed")

    @task(PURCHASE_WEIGHT)
    def purchase_product(self):
        with metrics_lock:
            custom_metrics["total_attempts"] += 1
        qty = random.randint(1,3)
        start = time.time()
        success, _, error = attempt_purchase(987, qty)
        latency_ms = (time.time()-start)*1000

        # Store the latency in purchase_latencies list for statistics calculation
        with metrics_lock:
            custom_metrics["purchase_latencies"].append(latency_ms)

        if error:
            events.request.fire(request_type="Purchase",
                                name="Purchase Product",
                                response_time=latency_ms,
                                response_length=0,
                                exception=Exception(error))
        else:
            events.request.fire(request_type="Purchase",
                                name="Purchase Product",
                                response_time=latency_ms,
                                response_length=qty if success else 0,
                                exception=None)

# Constant load shape
class ConstantLoadShape(LoadTestShape):
    def __init__(self):
        self.user_count = USER_COUNT
        self.spawn_rate = int(os.getenv("SPAWN_RATE","10"))
        self.test_duration = int(os.getenv("TEST_DURATION","60"))
        print(f"Constant load: {self.user_count} users @ {self.spawn_rate}/s for {self.test_duration}s")

    def tick(self):
        start_time = custom_metrics["start_time"] if custom_metrics["start_time"] is not None else time.time()
        elapsed = time.time() - start_time
        if elapsed >= self.test_duration:
            save_metrics()
            print("Test complete.")
            return None
        return (self.user_count, self.spawn_rate)

# Locust event hooks
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    print("Initializing E-commerce Stock Benchmark")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("Benchmark starting")
    custom_metrics["start_time"] = time.time()

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("Benchmark stopping")
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT stock FROM product_inventory WHERE product_id = 987;")
            custom_metrics["current_stock"] = cursor.fetchone()[0]
        conn.close()
    except:
        pass
    total = custom_metrics["successful_purchases"]
    print(f"Successful purchases: {total}")
    save_metrics()

# Standalone
if __name__ == "__main__":
    ensure_product_table_exists()
    reset_product_stock()
    print("Ready: locust -f ecommerce_stock_benchmark.py")
