from locust import User, task, between, events, tag
import os
import psycopg2
import time
import random

# Database configuration via environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "citus")
DB_PASSWORD = os.getenv("DB_PASSWORD", "citus")
DB_NAME = os.getenv("DB_NAME", "citus")

# Test product for stock update
PRODUCT_ID = 1
# Read/write ratio config (can be overridden with environment variables)
READ_RATIO = int(os.getenv("READ_RATIO", "80"))
WRITE_RATIO = int(os.getenv("WRITE_RATIO", "20"))
# Total weight should be 100
assert READ_RATIO + WRITE_RATIO == 100, "READ_RATIO + WRITE_RATIO must equal 100"

# Global variable to track test start time
test_start_time = None

def get_connection():
    """Create a new PostgreSQL connection."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    # Set search path for each connection
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO public, citus;")
        conn.commit()
    return conn

def ensure_tables_exist(conn):
    """Verify all required tables exist, and create them if they don't."""
    # Create a separate connection for table creation to avoid transaction conflicts
    # This ensures we won't have issues with SET commands inside transactions
    create_conn = None
    try:
        # Check if tables exist using the provided connection
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'products'
                );
            """)
            has_products = cur.fetchone()[0]

            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'orders'
                );
            """)
            has_orders = cur.fetchone()[0]

            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'order_items'
                );
            """)
            has_order_items = cur.fetchone()[0]

        # If any tables are missing, create them with a fresh connection
        if not (has_products and has_orders and has_order_items):
            print("One or more required tables are missing. Creating tables...")

            # Create a new connection specifically for table creation
            # This avoids transaction conflicts with SET commands
            create_conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            create_conn.autocommit = True  # Important: Set autocommit before any operations

            with create_conn.cursor() as cur:
                # Set search path outside transaction
                cur.execute("SET search_path TO public, citus;")

                # Create products table if needed
                if not has_products:
                    print("Creating products table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS products (
                            product_id SERIAL PRIMARY KEY,
                            product_name VARCHAR(100) NOT NULL,
                            price DECIMAL(10, 2) NOT NULL,
                            stock INT NOT NULL,
                            description TEXT
                        );
                    """)

                # Create orders table if needed
                if not has_orders:
                    print("Creating orders table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS orders (
                            order_id SERIAL PRIMARY KEY,
                            customer_id INT NOT NULL,
                            order_date TIMESTAMP NOT NULL,
                            status VARCHAR(20) NOT NULL
                        );
                    """)

                # Create order_items table if needed
                if not has_order_items:
                    print("Creating order_items table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS order_items (
                            order_id INT NOT NULL,
                            product_id INT NOT NULL,
                            quantity INT NOT NULL,
                            PRIMARY KEY (order_id, product_id)
                        );
                    """)

                # Add some sample data if products table is empty
                cur.execute("SELECT COUNT(*) FROM products;")
                if cur.fetchone()[0] == 0:
                    print("Adding minimal test data...")
                    # Insert test product
                    cur.execute("""
                        INSERT INTO products (product_id, product_name, price, stock, description)
                        VALUES (%s, %s, %s, %s, %s);
                    """, (PRODUCT_ID, "Test Product", 99.99, 10000, "Test product for load testing"))

                print("Tables are now ready.")

            # Verify tables exist in the original connection
            with conn.cursor() as cur:
                # Need to commit any pending transaction to see the new tables
                conn.commit()

                # Check tables again
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'products');")
                has_products = cur.fetchone()[0]
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'orders');")
                has_orders = cur.fetchone()[0]
                cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'order_items');")
                has_order_items = cur.fetchone()[0]

                return has_products and has_orders and has_order_items

        return True

    except Exception as e:
        print(f"Failed to create missing tables: {e}")
        return False
    finally:
        # Clean up the dedicated connection if it was created
        if create_conn:
            create_conn.close()
class EcommerceUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        # Each simulated user has its own connection
        try:
            self.conn = get_connection()
            self.conn.autocommit = False

            # Verify tables exist for this connection
            if not ensure_tables_exist(self.conn):
                print("Required tables don't exist and couldn't be created. Aborting test.")
                if hasattr(self, "conn") and not self.conn.closed:
                    self.conn.close()
                self.environment.runner.quit()
                return

        except Exception as e:
            print(f"Error acquiring database connection: {e}")
            # Force the user to stop if we can't get a connection
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.close()
            self.environment.runner.quit()

    def on_stop(self):
        if hasattr(self, "conn"):
            try:
                # Close the connection instead of returning it to a pool
                if not self.conn.closed:
                    self.conn.close()
                delattr(self, "conn")
            except Exception as e:
                print(f"Error closing connection: {e}")

    def log_success(self, name, response_time, response_length=0):
        """Log a successful request to Locust statistics"""
        self.environment.events.request.fire(
            request_type="DB",
            name=name,
            response_time=response_time,
            response_length=response_length,
            exception=None,
            context={},
        )

    def log_failure(self, name, response_time, exception):
        """Log a failed request to Locust statistics"""
        self.environment.events.request.fire(
            request_type="DB",
            name=name,
            response_time=response_time,
            response_length=0,
            exception=exception,
            context={},
        )

    # Methods to safely handle transactions
    def safe_execute(self, cursor, query, params=None):
        """Execute a query safely with error handling"""
        try:
            cursor.execute(query, params)
            return True
        except psycopg2.Error as e:
            # Properly handle the error, rollback if needed
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            raise e

    @tag('write')
    @task(WRITE_RATIO)
    def scenario_ecommerce_stock_update(self):
        """
        Scenario: High-concurrency stock decrement.
        Each task attempts to decrement stock by 1 if available.
        """
        start = time.time()
        cur = None
        try:
            # If connection closed, create a new one
            if not hasattr(self, "conn") or self.conn.closed:
                self.conn = get_connection()
                self.conn.autocommit = False

            cur = self.conn.cursor()
            # Attempt to decrement stock = stock - 1 WHERE stock > 0
            self.safe_execute(
                cur,
                """
                UPDATE products
                SET stock = stock - 1
                WHERE product_id = %s AND stock > 0;
                """,
                (PRODUCT_ID,)
            )
            if cur.rowcount == 0:
                # No rows updated → out of stock
                raise Exception("Out of stock")
            self.conn.commit()
            elapsed = int((time.time() - start) * 1000)
            self.log_success("ecommerce_stock_update", elapsed)
        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_stock_update", elapsed, e)
        finally:
            if cur:
                cur.close()

    @tag('read')
    @task(READ_RATIO)
    def scenario_ecommerce_product_info(self):
        """
        Scenario: High-concurrency product info reads.
        Each task reads product details.
        """
        start = time.time()
        cur = None
        try:
            cur = self.conn.cursor()
            # Verify connection is valid
            if self.conn.closed:
                self.conn = get_connection()
                self.conn.autocommit = False
                cur = self.conn.cursor()

            # Get product details
            self.safe_execute(
                cur,
                """
                SELECT product_id, product_name, price, stock, description
                FROM products
                WHERE product_id = %s;
                """,
                (PRODUCT_ID,)
            )
            result = cur.fetchone()
            if not result:
                raise Exception("Product not found")

            elapsed = int((time.time() - start) * 1000)
            self.log_success("ecommerce_product_info", elapsed, 1)
        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_product_info", elapsed, e)
        finally:
            if cur:
                cur.close()

    @tag('read')
    @task(READ_RATIO // 2)  # Lower frequency than simple reads
    def scenario_ecommerce_search_products(self):
        """
        Scenario: Search products with filtering.
        Simulates a more complex query with filtering and ordering.
        """
        start = time.time()
        cur = None
        try:
            # Verify connection is valid
            if self.conn.closed:
                self.conn = get_connection()
                self.conn.autocommit = False

            # Random price range for search
            min_price = random.randint(10, 50)
            max_price = min_price + random.randint(20, 100)

            cur = self.conn.cursor()
            # Simplified query without joins to avoid repartitioning issues
            self.safe_execute(
                cur,
                """
                SELECT p.product_id, p.product_name, p.price, p.stock
                FROM products p
                WHERE p.price BETWEEN %s AND %s
                  AND p.stock > 0
                ORDER BY p.price ASC
                LIMIT 10;
                """,
                (min_price, max_price)
            )
            results = cur.fetchall()

            elapsed = int((time.time() - start) * 1000)
            self.log_success("ecommerce_search_products", elapsed, len(results) if results else 0)
        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_search_products", elapsed, e)
        finally:
            if cur:
                cur.close()

    @tag('write')
    @task(WRITE_RATIO // 2)  # Lower frequency than stock updates
    def scenario_ecommerce_create_order(self):
        """
        Scenario: Create a new order with multiple items.
        Tests transaction performance with multiple operations.
        """
        start = time.time()
        cur = None
        try:
            # Verify connection is valid
            if self.conn.closed:
                self.conn = get_connection()
                self.conn.autocommit = False

            cur = self.conn.cursor()

            # Generate a random customer ID
            customer_id = random.randint(1, 1000)

            # Create order header
            self.safe_execute(
                cur,
                """
                INSERT INTO orders (customer_id, order_date, status)
                VALUES (%s, CURRENT_TIMESTAMP, 'pending')
                RETURNING order_id;
                """,
                (customer_id,)
            )
            order_id = cur.fetchone()[0]

            # Create 1-3 order items
            num_items = random.randint(1, 3)
            # Track products to avoid duplicates within this order
            used_products = set()

            for _ in range(num_items):
                # Find a unique product ID that hasn't been used in this order yet
                while True:
                    # Random product (not the test product to avoid affecting stock test)
                    product_id = random.randint(2, 100)
                    if product_id not in used_products:
                        used_products.add(product_id)
                        break

                quantity = random.randint(1, 3)

                # Add order item
                self.safe_execute(
                    cur,
                    """
                    INSERT INTO order_items (order_id, product_id, quantity)
                    VALUES (%s, %s, %s);
                    """,
                    (order_id, product_id, quantity)
                )

                # Update stock
                self.safe_execute(
                    cur,
                    """
                    UPDATE products
                    SET stock = stock - %s
                    WHERE product_id = %s AND stock >= %s;
                    """,
                    (quantity, product_id, quantity)
                )

                if cur.rowcount == 0:
                    # Not enough stock
                    raise Exception(f"Not enough stock for product {product_id}")

            # Commit the transaction
            self.conn.commit()

            elapsed = int((time.time() - start) * 1000)
            self.log_success("ecommerce_create_order", elapsed)
        except Exception as e:
            if self.conn and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_create_order", elapsed, e)
        finally:
            if cur:
                cur.close()

# Adds an event listener for test start
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global test_start_time
    test_start_time = time.time()  # Record the start time

    print(f"Test is starting with read/write ratio of {READ_RATIO}/{WRITE_RATIO}")
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}")

    # Create tables and initialize test data
    conn = None
    try:
        conn = get_connection()
        conn.autocommit = True

        with conn.cursor() as cur:
            # Set longer statement timeout
            cur.execute("SET statement_timeout = 120000;") # 120 seconds for init

            # Wait for database to be ready
            max_retries = 10
            retry_delay = 5
            for attempt in range(max_retries):
                try:
                    cur.execute("SELECT 1;")
                    print("Database connection established successfully.")
                    break
                except psycopg2.Error as e:
                    if attempt < max_retries - 1:
                        print(f"Database not ready, retrying in {retry_delay} seconds... ({e})")
                        time.sleep(retry_delay)
                    else:
                        raise Exception(f"Could not connect to database after {max_retries} attempts: {e}")

            # Make sure public schema exists and is empty
            try:
                cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
                cur.execute("CREATE SCHEMA public;")
                print("Recreated public schema")

                # Grant permissions to ensure access
                cur.execute("GRANT ALL ON SCHEMA public TO current_user;")
                cur.execute("GRANT ALL ON SCHEMA public TO public;")
            except psycopg2.Error as e:
                print(f"Warning when recreating public schema: {e}")
                # Try to continue anyway

            # Create tables with proper error handling - FORCE CREATE without IF NOT EXISTS
            print("Creating tables...")
            try:
                # Drop tables if they exist to ensure clean state
                cur.execute("DROP TABLE IF EXISTS order_items;")
                cur.execute("DROP TABLE IF EXISTS orders;")
                cur.execute("DROP TABLE IF EXISTS products;")

                # Create products table
                cur.execute("""
                    CREATE TABLE products (
                        product_id SERIAL PRIMARY KEY,
                        product_name VARCHAR(100) NOT NULL,
                        price DECIMAL(10, 2) NOT NULL,
                        stock INT NOT NULL,
                        description TEXT
                    );
                """)

                # Create orders table
                cur.execute("""
                    CREATE TABLE orders (
                        order_id SERIAL PRIMARY KEY,
                        customer_id INT NOT NULL,
                        order_date TIMESTAMP NOT NULL,
                        status VARCHAR(20) NOT NULL
                    );
                """)

                # Create order_items table
                cur.execute("""
                    CREATE TABLE order_items (
                        order_id INT NOT NULL,
                        product_id INT NOT NULL,
                        quantity INT NOT NULL,
                        PRIMARY KEY (order_id, product_id)
                    );
                """)

                print("Tables created successfully")

                # Verify tables were created
                if not ensure_tables_exist(conn):
                    raise Exception("Tables were not properly created")

            except psycopg2.Error as e:
                print(f"Error creating tables: {e}")
                raise Exception(f"Failed to create database tables: {e}")

            # Try to distribute tables for Citus
            print("Attempting to distribute tables...")

            try:
                # Check if this is a Citus database by checking for the function
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM pg_proc WHERE proname = 'create_distributed_table'
                    );
                """)
                has_citus = cur.fetchone()[0]

                if has_citus:
                    try:
                        cur.execute("SELECT create_distributed_table('products', 'product_id');")
                        print("Products table distributed successfully.")
                    except psycopg2.Error as e:
                        print(f"Warning: Could not distribute products table: {e}")

                    try:
                        cur.execute("SELECT create_distributed_table('orders', 'order_id');")
                        print("Orders table distributed successfully.")
                    except psycopg2.Error as e:
                        print(f"Warning: Could not distribute orders table: {e}")

                    try:
                        cur.execute("SELECT create_distributed_table('order_items', 'order_id');")
                        print("Order_items table distributed successfully.")
                    except psycopg2.Error as e:
                        print(f"Warning: Could not distribute order_items table: {e}")

                    # Enable repartition joins if using Citus
                    cur.execute("SET citus.enable_repartition_joins = ON;")
                else:
                    print("Not a Citus database, skipping distribution.")
            except psycopg2.Error as e:
                print(f"Warning during Citus setup: {e}")

            # Insert test data
            print("Inserting test data...")
            try:
                # Insert 100 products with random stock
                for i in range(1, 101):
                    product_name = f"Product {i}"
                    price = round(random.uniform(10, 500), 2)
                    stock = random.randint(20, 1000)
                    description = f"This is a description for product {i}"

                    cur.execute(
                        """
                        INSERT INTO products (product_id, product_name, price, stock, description)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        (i, product_name, price, stock, description)
                    )
                print("Test data inserted.")

                # Make sure our test product has enough stock
                cur.execute("UPDATE products SET stock = 10000 WHERE product_id = %s;", (PRODUCT_ID,))
                print(f"Reset stock for test product (ID: {PRODUCT_ID}) to 10000.")

                # Verify product count
                cur.execute("SELECT COUNT(*) FROM products;")
                count = cur.fetchone()[0]
                print(f"Confirmed {count} products in database.")

                if count == 0:
                    raise Exception("No products were inserted - database setup failed!")

            except psycopg2.Error as e:
                print(f"Error during data setup: {e}")
                raise Exception(f"Failed to initialize test data: {e}")

        # Close connection when done
        if conn and not conn.closed:
            conn.close()
        print("Database initialization complete!")

    except Exception as e:
        print(f"CRITICAL ERROR during database initialization: {e}")
        print("The test cannot run without proper database setup. Aborting.")
        if conn and not conn.closed:
            conn.close()
        # Force the test to quit if database setup fails
        environment.runner.quit()

# Adds an event listener for test stop
@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("Test is ending")
    # No pool to close

    # Print final stats summary
    print_stats(environment)

# Add stats listener for periodic reporting
@events.init.add_listener
def on_locust_init(environment, **kwargs):
    # Register periodic stats printer
    if environment.stats:
        environment.stats.csv_writer = None  # Default is None but being explicit

        # Register the stats printing task to run every 10 seconds
        environment.runner.register_message("stats_printer", lambda: print_stats(environment))

        # Schedule regular stats printing
        @environment.runner.greenlet.spawn
        def stats_printer():
            while True:
                environment.runner.send_message("stats_printer")
                time.sleep(10)  # Print stats every 10 seconds

def print_stats(environment):
    """Print statistics including avg latency, p95 latency and throughput"""
    stats = environment.stats

    if not stats or not stats.entries:
        return

    # Get current timestamp for the report
    current_time = time.time()

    # Header
    print("\n----- Performance Statistics -----")
    print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))}")
    print("{:<30} {:<12} {:<12} {:<15} {:<10}".format(
        "Endpoint", "Avg (ms)", "P95 (ms)", "Throughput (req/s)", "Failures"
    ))
    print("-" * 80)

    # Sort entries by endpoint name
    entries = sorted(stats.entries.items(), key=lambda x: x[0])

    # Calculate overall stats
    total_reqs = 0
    total_failures = 0

    # Use the global test_start_time instead of environment.runner.start_time
    global test_start_time
    test_duration = current_time - (test_start_time or current_time)
    if test_duration <= 0:
        test_duration = 0.001  # Avoid division by zero

    # Print stats for each entry
    for key, entry in entries:
        name = key[1] if len(key) > 1 else key[0]
        avg_response_time = entry.avg_response_time
        p95_response_time = entry.get_response_time_percentile(0.95)
        request_count = entry.num_requests
        failure_count = entry.num_failures

        # Calculate throughput (requests per second)
        throughput = request_count / test_duration

        print("{:<30} {:<12.2f} {:<12.2f} {:<15.2f} {:<10}".format(
            name, avg_response_time, p95_response_time, throughput, failure_count
        ))

        total_reqs += request_count
        total_failures += failure_count

    # Print total/summary
    print("-" * 80)
    overall_throughput = total_reqs / test_duration
    print("{:<30} {:<12} {:<12} {:<15.2f} {:<10}".format(
        "TOTAL", "", "", overall_throughput, total_failures
    ))
    print(f"Total requests: {total_reqs}")
    print(f"Test duration: {test_duration:.2f} seconds")
    print(f"Overall success rate: {100 - (total_failures/max(total_reqs, 1))*100:.2f}%")
    print("-" * 80)
