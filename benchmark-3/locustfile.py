from locust import User, task, between, events
import os
import psycopg2
import time
import uuid
import random
import logging

# Database configuration via environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "citus")
DB_PASSWORD = os.getenv("DB_PASSWORD", "citus")
DB_NAME = os.getenv("DB_NAME", "citus")

# Account ID range for the test
# We'll use a small set of accounts to create contention
MIN_ACCOUNT_ID = 1
MAX_ACCOUNT_ID = 20

# Global variable to track test start time
test_start_time = None
# Global flag to indicate if DB setup is complete
db_setup_complete = False

# Setup database tables before the test starts
def setup_database():
    """Create the necessary tables and initialize test data for financial transactions."""
    logging.info("Setting up database tables for financial transactions benchmark...")

    try:
        conn = get_connection()
        conn.autocommit = True  # We want these DDL statements to be committed immediately

        with conn.cursor() as cur:
            # Enable uuid-ossp extension if not already enabled
            cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

            # Create tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    account_name VARCHAR(255) NOT NULL,
                    account_type VARCHAR(50) NOT NULL,
                    balance NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    transaction_id VARCHAR(36) PRIMARY KEY,
                    source_account_id INT REFERENCES accounts(account_id),
                    destination_account_id INT REFERENCES accounts(account_id),
                    amount NUMERIC(15, 2) NOT NULL,
                    transaction_time TIMESTAMP NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    CONSTRAINT positive_amount CHECK (amount > 0)
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS transaction_logs (
                    log_id SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(36) REFERENCES transactions(transaction_id),
                    source_account_id INT NOT NULL,
                    source_before_balance NUMERIC(15, 2) NOT NULL,
                    source_after_balance NUMERIC(15, 2) NOT NULL,
                    destination_account_id INT NOT NULL,
                    destination_before_balance NUMERIC(15, 2) NOT NULL,
                    destination_after_balance NUMERIC(15, 2) NOT NULL,
                    log_time TIMESTAMP NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS account_transactions (
                    transaction_id VARCHAR(36) PRIMARY KEY,
                    account_id INT REFERENCES accounts(account_id),
                    transaction_type VARCHAR(20) NOT NULL, -- 'deposit' or 'withdrawal'
                    amount NUMERIC(15, 2) NOT NULL,
                    before_balance NUMERIC(15, 2) NOT NULL,
                    after_balance NUMERIC(15, 2) NOT NULL,
                    transaction_time TIMESTAMP NOT NULL,
                    CONSTRAINT positive_amount CHECK (amount > 0)
                );
            """)

            # Initialize test data:
            # Create 20 accounts with initial balance of 1000
            for i in range(1, MAX_ACCOUNT_ID + 1):
                account_type = ""
                if i % 3 == 0:
                    account_type = "savings"
                elif i % 3 == 1:
                    account_type = "checking"
                else:
                    account_type = "money_market"

                cur.execute("""
                    INSERT INTO accounts (account_id, user_id, account_name, account_type, balance)
                    VALUES (%s, uuid_generate_v4(), %s, %s, 1000.00)
                    ON CONFLICT (account_id) DO UPDATE SET balance = 1000.00;
                """, (i, f'Test Account {i}', account_type))

            # Make sure the sequence is set correctly
            cur.execute("""
                SELECT setval('accounts_account_id_seq', (SELECT MAX(account_id) FROM accounts));
            """)

            # Verify setup
            cur.execute("SELECT COUNT(*) FROM accounts;")
            result = cur.fetchone()
            if result and result[0] == MAX_ACCOUNT_ID:
                logging.info(f"Successfully created {MAX_ACCOUNT_ID} test accounts with $1000.00 each.")
            else:
                logging.error(f"Failed to initialize all test accounts. Found {result[0] if result else 0} accounts.")

        conn.close()
        logging.info("Financial transactions database setup completed successfully.")
        return True
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        return False

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

def verify_tables_exist(conn):
    """Verify that all required tables exist in the database."""
    try:
        with conn.cursor() as cur:
            # Check if accounts table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = 'accounts'
                );
            """)
            has_accounts = cur.fetchone()[0]

            if not has_accounts:
                print("ERROR: accounts table does not exist!")
                return False

            # Verify accounts table has data
            cur.execute("SELECT COUNT(*) FROM public.accounts;")
            accounts_count = cur.fetchone()[0]
            print(f"Found {accounts_count} accounts in database.")

            if accounts_count < 1:
                print("WARNING: No accounts found in the accounts table!")
                return False

        return True
    except Exception as e:
        print(f"Error verifying tables: {e}")
        return False

class FinancialTransactionUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        # Each simulated user has its own connection
        try:
            # Wait for database setup to complete
            global db_setup_complete
            if not db_setup_complete:
                print("Waiting for database setup to complete...")
                time.sleep(2)  # Add a delay for safety

            self.conn = get_connection()
            self.conn.autocommit = False

            # Verify tables exist before proceeding
            if not verify_tables_exist(self.conn):
                print("ERROR: Required tables don't exist or contain no data.")
                if hasattr(self, "conn") and not self.conn.closed:
                    self.conn.close()
                self.environment.runner.quit()
                return

            # Generate a unique user ID for each simulated user
            self.user_id = str(uuid.uuid4())
            print(f"User {self.user_id[:8]} successfully initialized")

        except Exception as e:
            logging.error(f"Error during user startup: {e}")
            # Force the user to stop if we can't get a connection
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.close()
            self.environment.runner.quit()

    def on_stop(self):
        if hasattr(self, "conn") and not self.conn.closed:
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

    @task(1)
    def scenario_account_transfer(self):
        """
        Scenario 3: Financial Transactions (Concurrent Account Updates)
        Simulating a banking environment with simultaneous transfers between accounts.

        Operation Pattern: Multi-step transactional updates on shared balance records.
        System Stress: Risk of inconsistent state (e.g., overdrafts or duplicate debits) under weak isolation.
        Evaluation Metrics: Latency, transaction success rate, data integrity (no overdraft or mismatch), and rollback frequency.
        """
        start = time.time()

        # Randomly select source and destination accounts
        # Making sure they are different to create a real transfer scenario
        source_id = random.randint(MIN_ACCOUNT_ID, MAX_ACCOUNT_ID)
        dest_id = random.randint(MIN_ACCOUNT_ID, MAX_ACCOUNT_ID)
        while dest_id == source_id:
            dest_id = random.randint(MIN_ACCOUNT_ID, MAX_ACCOUNT_ID)

        # Transfer a random amount between 1 and 100
        amount = random.randint(1, 100)

        transaction_id = str(uuid.uuid4())

        try:
            # Make sure we're not in a transaction already
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()

            # Begin a fresh transaction with explicit isolation level
            self.conn.autocommit = False
            cur = self.conn.cursor()

            # Set isolation level as the first statement in the transaction
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

            # Check if source account has enough funds - lock the row with SELECT FOR UPDATE
            # IMPORTANT: Use explicit public schema
            cur.execute(
                """
                SELECT balance
                FROM public.accounts
                WHERE account_id = %s
                FOR UPDATE;
                """,
                (source_id,)
            )

            result = cur.fetchone()
            if not result or result[0] < amount:
                # Insufficient funds
                raise Exception(f"Insufficient funds in account {source_id}")

            source_balance = result[0]

            # Debit the source account
            cur.execute(
                """
                UPDATE public.accounts
                SET balance = balance - %s
                WHERE account_id = %s
                  AND balance >= %s
                RETURNING balance;
                """,
                (amount, source_id, amount)
            )

            if cur.rowcount == 0:
                # No rows updated → debit failed
                raise Exception(f"Failed to debit account {source_id}")

            new_source_balance = cur.fetchone()[0]

            # Credit the destination account
            cur.execute(
                """
                UPDATE public.accounts
                SET balance = balance + %s
                WHERE account_id = %s
                RETURNING balance;
                """,
                (amount, dest_id)
            )

            if cur.rowcount == 0:
                # No rows updated → credit failed
                raise Exception(f"Failed to credit account {dest_id}")

            new_dest_balance = cur.fetchone()[0]

            # Record the transaction
            cur.execute(
                """
                INSERT INTO public.transactions (
                    transaction_id,
                    source_account_id,
                    destination_account_id,
                    amount,
                    transaction_time,
                    status
                ) VALUES (%s, %s, %s, %s, NOW(), 'completed');
                """,
                (transaction_id, source_id, dest_id, amount)
            )

            # Record before and after balances for auditing
            cur.execute(
                """
                INSERT INTO public.transaction_logs (
                    transaction_id,
                    source_account_id,
                    source_before_balance,
                    source_after_balance,
                    destination_account_id,
                    destination_before_balance,
                    destination_after_balance,
                    log_time
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW());
                """,
                (
                    transaction_id,
                    source_id,
                    source_balance,
                    new_source_balance,
                    dest_id,
                    new_dest_balance - amount,
                    new_dest_balance,
                )
            )

            # Commit the transaction
            self.conn.commit()

            elapsed = int((time.time() - start) * 1000)
            self.log_success("account_transfer", elapsed)
        except Exception as e:
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("account_transfer", elapsed, e)

    @task(2)
    def scenario_check_balance(self):
        """
        A read-only transaction to check account balance.
        This is a common operation in banking systems and creates read load.
        """
        start = time.time()

        # Randomly select an account
        account_id = random.randint(MIN_ACCOUNT_ID, MAX_ACCOUNT_ID)

        try:
            # Make sure we're not in a transaction already
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()

            # Begin a fresh transaction with explicit isolation level
            self.conn.autocommit = False
            cur = self.conn.cursor()

            # Set isolation level as the first statement in the transaction
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

            # Check account balance
            # IMPORTANT: Use explicit public schema
            cur.execute(
                """
                SELECT balance
                FROM public.accounts
                WHERE account_id = %s;
                """,
                (account_id,)
            )

            result = cur.fetchone()
            if not result:
                raise Exception(f"Account {account_id} not found")

            self.conn.commit()

            elapsed = int((time.time() - start) * 1000)
            self.log_success("check_balance", elapsed)
        except Exception as e:
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("check_balance", elapsed, e)

    @task(1)
    def scenario_deposit_withdraw(self):
        """
        Simple deposit or withdrawal transaction.
        This creates additional write load on individual accounts.
        """
        start = time.time()

        # Randomly select an account
        account_id = random.randint(MIN_ACCOUNT_ID, MAX_ACCOUNT_ID)

        # Randomly choose between deposit and withdrawal
        is_deposit = random.choice([True, False])

        # Amount between 1 and 100
        amount = random.randint(1, 100)
        transaction_id = str(uuid.uuid4())

        try:
            # Make sure we're not in a transaction already
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()

            # Begin a fresh transaction
            self.conn.autocommit = False
            cur = self.conn.cursor()

            # Set appropriate isolation level as the first statement in the transaction
            if not is_deposit:
                # For withdrawal, need higher isolation level
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

                # Check if account has enough funds
                # IMPORTANT: Use explicit public schema
                cur.execute(
                    """
                    SELECT balance
                    FROM public.accounts
                    WHERE account_id = %s
                    FOR UPDATE;
                    """,
                    (account_id,)
                )

                result = cur.fetchone()
                if not result or result[0] < amount:
                    # Insufficient funds
                    raise Exception(f"Insufficient funds in account {account_id}")

                before_balance = result[0]

                # Perform withdrawal
                cur.execute(
                    """
                    UPDATE public.accounts
                    SET balance = balance - %s
                    WHERE account_id = %s
                      AND balance >= %s
                    RETURNING balance;
                    """,
                    (amount, account_id, amount)
                )

                if cur.rowcount == 0:
                    # No rows updated → withdrawal failed
                    raise Exception(f"Failed to withdraw from account {account_id}")

                after_balance = cur.fetchone()[0]
                transaction_type = 'withdrawal'
            else:
                # For deposit, use read committed isolation
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

                # Get current balance for logging
                # IMPORTANT: Use explicit public schema
                cur.execute(
                    """
                    SELECT balance
                    FROM public.accounts
                    WHERE account_id = %s
                    FOR UPDATE;
                    """,
                    (account_id,)
                )

                result = cur.fetchone()
                if not result:
                    raise Exception(f"Account {account_id} not found")

                before_balance = result[0]

                # Perform deposit
                cur.execute(
                    """
                    UPDATE public.accounts
                    SET balance = balance + %s
                    WHERE account_id = %s
                    RETURNING balance;
                    """,
                    (amount, account_id)
                )

                if cur.rowcount == 0:
                    # No rows updated → deposit failed
                    raise Exception(f"Failed to deposit to account {account_id}")

                after_balance = cur.fetchone()[0]
                transaction_type = 'deposit'

            # Record the transaction
            cur.execute(
                """
                INSERT INTO public.account_transactions (
                    transaction_id,
                    account_id,
                    transaction_type,
                    amount,
                    before_balance,
                    after_balance,
                    transaction_time
                ) VALUES (%s, %s, %s, %s, %s, %s, NOW());
                """,
                (transaction_id, account_id, transaction_type, amount, before_balance, after_balance)
            )

            # Commit the transaction
            self.conn.commit()

            elapsed = int((time.time() - start) * 1000)
            self.log_success(f"account_{transaction_type}", elapsed)
        except Exception as e:
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("account_deposit_withdraw", elapsed, e)

# Add event listeners for test lifecycle management
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global test_start_time, db_setup_complete
    test_start_time = time.time()  # Record the start time
    db_setup_complete = False  # Initialize as not complete

    print(f"Connecting to PostgreSQL at {DB_HOST}:{DB_PORT}")

    # Create tables and initialize test data
    conn = None
    try:
        # Create a dedicated connection for setup with explicit settings
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        conn.autocommit = True  # Important: Use autocommit for DDL

        with conn.cursor() as cur:
            # Set longer statement timeout
            cur.execute("SET statement_timeout = 120000;")  # 120 seconds for init
            # Set search path explicitly
            cur.execute("SET search_path TO public, citus;")

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

            # First drop schema and recreate to ensure clean state
            print("Recreating public schema...")
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public;")

            # Grant permissions
            cur.execute("GRANT ALL ON SCHEMA public TO current_user;")
            cur.execute("GRANT ALL ON SCHEMA public TO public;")

            # Enable uuid-ossp extension
            cur.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")

            print("Creating financial transaction tables from scratch...")

            # Create accounts table
            cur.execute("""
                CREATE TABLE public.accounts (
                    account_id SERIAL PRIMARY KEY,
                    user_id VARCHAR(36) NOT NULL,
                    account_name VARCHAR(255) NOT NULL,
                    account_type VARCHAR(50) NOT NULL,
                    balance NUMERIC(15, 2) NOT NULL DEFAULT 0.00,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)
            print("Created accounts table")

            # Create transactions table
            cur.execute("""
                CREATE TABLE public.transactions (
                    transaction_id VARCHAR(36) PRIMARY KEY,
                    source_account_id INT REFERENCES public.accounts(account_id),
                    destination_account_id INT REFERENCES public.accounts(account_id),
                    amount NUMERIC(15, 2) NOT NULL,
                    transaction_time TIMESTAMP NOT NULL,
                    status VARCHAR(20) DEFAULT 'pending',
                    CONSTRAINT positive_amount CHECK (amount > 0)
                );
            """)
            print("Created transactions table")

            # Create transaction_logs table
            cur.execute("""
                CREATE TABLE public.transaction_logs (
                    log_id SERIAL PRIMARY KEY,
                    transaction_id VARCHAR(36) REFERENCES public.transactions(transaction_id),
                    source_account_id INT NOT NULL,
                    source_before_balance NUMERIC(15, 2) NOT NULL,
                    source_after_balance NUMERIC(15, 2) NOT NULL,
                    destination_account_id INT NOT NULL,
                    destination_before_balance NUMERIC(15, 2) NOT NULL,
                    destination_after_balance NUMERIC(15, 2) NOT NULL,
                    log_time TIMESTAMP NOT NULL
                );
            """)
            print("Created transaction_logs table")

            # Create account_transactions table
            cur.execute("""
                CREATE TABLE public.account_transactions (
                    transaction_id VARCHAR(36) PRIMARY KEY,
                    account_id INT REFERENCES public.accounts(account_id),
                    transaction_type VARCHAR(20) NOT NULL,
                    amount NUMERIC(15, 2) NOT NULL,
                    before_balance NUMERIC(15, 2) NOT NULL,
                    after_balance NUMERIC(15, 2) NOT NULL,
                    transaction_time TIMESTAMP NOT NULL,
                    CONSTRAINT positive_amount CHECK (amount > 0)
                );
            """)
            print("Created account_transactions table")

            # Initialize test data: Create accounts with initial balance
            print(f"Creating {MAX_ACCOUNT_ID} test accounts...")
            for i in range(1, MAX_ACCOUNT_ID + 1):
                account_type = ""
                if i % 3 == 0:
                    account_type = "savings"
                elif i % 3 == 1:
                    account_type = "checking"
                else:
                    account_type = "money_market"

                cur.execute("""
                    INSERT INTO public.accounts (account_id, user_id, account_name, account_type, balance)
                    VALUES (%s, uuid_generate_v4(), %s, %s, 1000.00);
                """, (i, f'Test Account {i}', account_type))

            # Set the account_id sequence correctly
            cur.execute("SELECT setval('accounts_account_id_seq', (SELECT MAX(account_id) FROM public.accounts));")

            # Verify accounts were created
            cur.execute("SELECT COUNT(*) FROM public.accounts;")
            count = cur.fetchone()[0]
            print(f"Verified {count} accounts in database with $1000.00 each")

            # Verify tables and data
            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cur.fetchone()[0]
            print(f"Total tables in public schema: {table_count}")

            # List all tables in public schema for diagnostics
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [row[0] for row in cur.fetchall()]
            print(f"Tables in public schema: {', '.join(tables)}")

            # Add a delay to ensure all connections see the new schema
            print("Waiting for database changes to propagate...")
            time.sleep(2)

            # Double-check that tables exist and have data
            if not verify_tables_exist(conn):
                raise Exception("Database tables were created but couldn't be verified. Setup failed.")

        # Close connection when done
        if conn and not conn.closed:
            conn.close()
        print("Database initialization complete!")

        # Set DB setup complete flag for users
        db_setup_complete = True

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

# The SQL setup is now handled by the test_start event handler
"""
Setup Logic:
1. Tables Created:
   - accounts: Stores account information and balances
   - transactions: Records transfers between accounts
   - transaction_logs: Detailed audit trail of transfers with before/after balances
   - account_transactions: Records deposits and withdrawals to individual accounts

2. Test Data Initialization:
   - Creates 20 test accounts with $1000 initial balance each
   - Account types alternate between savings, checking, and money_market

3. When to Run:
   - The database is now automatically set up when the test starts
   - We recreate the entire schema to ensure a clean test environment
"""
