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

def get_connection():
    """Create a new PostgreSQL connection (full-buffered)."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    # Default cursor is fully buffered—no server-side cursors are used.
    with conn.cursor() as cursor:
        cursor.execute("SET search_path TO public, citus;")
        conn.commit()
    return conn

class EcommerceUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        # Each simulated user has its own connection
        self.conn = get_connection()
        self.conn.autocommit = False

    def on_stop(self):
        if hasattr(self, "conn"):
            self.conn.close()

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

    @tag('write')
    @task(WRITE_RATIO)
    def scenario_ecommerce_stock_update(self):
        """
        Scenario: High-concurrency stock decrement.
        Each task attempts to decrement stock by 1 if available.
        """
        start = time.time()
        try:
            cur = self.conn.cursor()
            # Attempt to decrement stock = stock - 1 WHERE stock > 0
            cur.execute(
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
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_stock_update", elapsed, e)

    @tag('read')
    @task(READ_RATIO)
    def scenario_ecommerce_product_info(self):
        """
        Scenario: High-concurrency product info reads.
        Each task reads product details.
        """
        start = time.time()
        try:
            cur = self.conn.cursor()
            # Get product details
            cur.execute(
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

            # Simpler query to avoid join issues
            # Note: We're skipping the product_relationships query that required repartitioning

            elapsed = int((time.time() - start) * 1000)
            self.log_success("ecommerce_product_info", elapsed, 1)
        except Exception as e:
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_product_info", elapsed, e)

    @tag('read')
    @task(READ_RATIO // 2)  # Lower frequency than simple reads
    def scenario_ecommerce_search_products(self):
        """
        Scenario: Search products with filtering.
        Simulates a more complex query with filtering and ordering.
        """
        start = time.time()
        try:
            # Random price range for search
            min_price = random.randint(10, 50)
            max_price = min_price + random.randint(20, 100)

            cur = self.conn.cursor()
            # Simplified query without joins to avoid repartitioning issues
            cur.execute(
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
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_search_products", elapsed, e)

    @tag('write')
    @task(WRITE_RATIO // 2)  # Lower frequency than stock updates
    def scenario_ecommerce_create_order(self):
        """
        Scenario: Create a new order with multiple items.
        Tests transaction performance with multiple operations.
        """
        start = time.time()
        try:
            cur = self.conn.cursor()

            # Start a transaction
            # Generate a random customer ID
            customer_id = random.randint(1, 1000)

            # Create order header
            cur.execute(
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
            for _ in range(num_items):
                # Random product (not the test product to avoid affecting stock test)
                product_id = random.randint(2, 100)
                quantity = random.randint(1, 3)

                # Add order item
                cur.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity)
                    VALUES (%s, %s, %s);
                    """,
                    (order_id, product_id, quantity)
                )

                # Update stock
                cur.execute(
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
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("ecommerce_create_order", elapsed, e)

# Adds an event listener for test start
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print(f"Test is starting with read/write ratio of {READ_RATIO}/{WRITE_RATIO}")
    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}")

    # Create tables and initialize test data
    try:
        conn = get_connection()
        conn.autocommit = True

        with conn.cursor() as cur:
            # Set longer statement timeout
            cur.execute("SET statement_timeout = 30000;") # 30 seconds

            # Check if products table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'products'
                );
            """)

            if not cur.fetchone()[0]:
                print("Creating database schema...")

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

                # Try to distribute tables for Citus
                print("Attempting to distribute tables...")
                try:
                    cur.execute("SELECT create_distributed_table('products', 'product_id');")
                    print("Products table distributed successfully.")
                except psycopg2.Error as e:
                    print(f"Warning: Could not distribute products table: {str(e)}")
                    print("Continuing with local table.")

                try:
                    cur.execute("SELECT create_distributed_table('orders', 'order_id');")
                    print("Orders table distributed successfully.")
                except psycopg2.Error as e:
                    print(f"Warning: Could not distribute orders table: {str(e)}")
                    print("Continuing with local table.")

                try:
                    cur.execute("SELECT create_distributed_table('order_items', 'order_id');")
                    print("Order_items table distributed successfully.")
                except psycopg2.Error as e:
                    print(f"Warning: Could not distribute order_items table: {str(e)}")
                    print("Continuing with local table.")

                print("Inserting test data...")

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

                # Make sure our test product has enough stock
                cur.execute("UPDATE products SET stock = 10000 WHERE product_id = %s;", (PRODUCT_ID,))

                print("Test data setup complete.")
            else:
                # Just reset test product stock if tables exist
                print("Database schema already exists.")

                # Ensure test product has enough stock for the test
                cur.execute("UPDATE products SET stock = 10000 WHERE product_id = %s;", (PRODUCT_ID,))
                print(f"Reset stock for test product (ID: {PRODUCT_ID}) to 10000.")

                # Enable repartition joins if needed
                try:
                    cur.execute("SET citus.enable_repartition_joins = ON;")
                except psycopg2.Error as e:
                    print(f"Warning: Could not enable repartition joins: {str(e)}")

        conn.close()
        print("Database initialization complete!")

    except Exception as e:
        print(f"Error initializing database: {e}")
        print("Continuing with tests, but expect errors if database is not properly set up.")
        if 'conn' in locals():
            conn.close()

# Adds an event listener for test stop
@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    print("Test is ending")
