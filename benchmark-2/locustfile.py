from locust import User, task, between, events
import os
import psycopg2
import time
import uuid
import logging

# Database configuration via environment variables
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("DB_USER", "citus")
DB_PASSWORD = os.getenv("DB_PASSWORD", "citus")
DB_NAME = os.getenv("DB_NAME", "citus")

# Hardcode the event we're stress-testing
EVENT_ID = 1
MAX_SLOTS = 100  # Total available slots for the event

# Global variable to track test start time
test_start_time = None

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
        # Set explicit search path for this connection
        cursor.execute("SET search_path TO public, citus;")
        # Set read committed isolation level to improve visibility of changes
        cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        conn.commit()
    return conn

def setup_database():
    """Create the necessary tables and initialize test data."""
    logging.info("Setting up database tables for reservation system benchmark...")

    try:
        conn = get_connection()
        conn.autocommit = True  # We want these DDL statements to be committed immediately

        with conn.cursor() as cur:
            # Create tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    event_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    total_slots INT NOT NULL,
                    available_slots INT NOT NULL,
                    booked_slots INT NOT NULL DEFAULT 0,
                    event_date TIMESTAMP NOT NULL
                );
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS reservations (
                    reservation_id VARCHAR(36) PRIMARY KEY,
                    event_id INT REFERENCES events(event_id),
                    user_id VARCHAR(36) NOT NULL,
                    reservation_time TIMESTAMP NOT NULL,
                    status VARCHAR(20) DEFAULT 'confirmed'
                );
            """)

            # Initialize test data
            cur.execute("""
                INSERT INTO events (
                    event_id, name, description, total_slots, available_slots, event_date
                ) VALUES (
                    1, 'Test Event', 'Event for load testing', 100, 100, NOW() + INTERVAL '1 day'
                ) ON CONFLICT (event_id) DO UPDATE
                SET available_slots = 100, booked_slots = 0;
            """)

            # Verify setup
            cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s", (EVENT_ID,))
            result = cur.fetchone()
            if result and result[0] > 0:
                logging.info(f"Test event with ID {EVENT_ID} is ready with {MAX_SLOTS} available slots.")
            else:
                logging.error(f"Failed to find test event with ID {EVENT_ID}")

        conn.close()
        logging.info("Database setup completed successfully.")
        return True

    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        return False

def ensure_tables_exist(conn):
    """Verify all required tables exist, and create them if they don't."""
    try:
        # Check if tables exist using the provided connection
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'events'
                );
            """)
            has_events = cur.fetchone()[0]

            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'reservations'
                );
            """)
            has_reservations = cur.fetchone()[0]

        # If any tables are missing, create them
        if not (has_events and has_reservations):
            print("One or more required tables are missing. Creating tables...")

            # Create a new connection specifically for table creation with autocommit
            create_conn = get_connection()
            create_conn.autocommit = True

            with create_conn.cursor() as cur:
                # Create events table if needed
                if not has_events:
                    print("Creating events table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS events (
                            event_id SERIAL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            description TEXT,
                            total_slots INT NOT NULL,
                            available_slots INT NOT NULL,
                            booked_slots INT NOT NULL DEFAULT 0,
                            event_date TIMESTAMP NOT NULL
                        );
                    """)

                # Create reservations table if needed
                if not has_reservations:
                    print("Creating reservations table...")
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS reservations (
                            reservation_id VARCHAR(36) PRIMARY KEY,
                            event_id INT REFERENCES events(event_id),
                            user_id VARCHAR(36) NOT NULL,
                            reservation_time TIMESTAMP NOT NULL,
                            status VARCHAR(20) DEFAULT 'confirmed'
                        );
                    """)

                # Add test event if events table was just created
                if not has_events:
                    print("Adding test event data...")
                    cur.execute("""
                        INSERT INTO events (
                            event_id, name, description, total_slots, available_slots, event_date
                        ) VALUES (
                            %s, 'Test Event', 'Event for load testing', %s, %s, NOW() + INTERVAL '1 day'
                        );
                    """, (EVENT_ID, MAX_SLOTS, MAX_SLOTS))
                    print(f"Test event with ID {EVENT_ID} created with {MAX_SLOTS} slots")
                else:
                    # Check if test event exists and reset if needed
                    cur.execute("SELECT COUNT(*) FROM events WHERE event_id = %s;", (EVENT_ID,))
                    if cur.fetchone()[0] == 0:
                        # Test event doesn't exist, create it
                        cur.execute("""
                            INSERT INTO events (
                                event_id, name, description, total_slots, available_slots, event_date
                            ) VALUES (
                                %s, 'Test Event', 'Event for load testing', %s, %s, NOW() + INTERVAL '1 day'
                            );
                        """, (EVENT_ID, MAX_SLOTS, MAX_SLOTS))
                        print(f"Test event with ID {EVENT_ID} created with {MAX_SLOTS} slots")
                    else:
                        # Reset existing event slots
                        cur.execute("""
                            UPDATE events
                            SET available_slots = %s, booked_slots = 0
                            WHERE event_id = %s;
                        """, (MAX_SLOTS, EVENT_ID))
                        print(f"Reset slots for existing test event ID {EVENT_ID}")

            create_conn.close()
            print("Tables are now ready.")

        # After table creation (or if they already existed), verify in the original connection
        # First, make sure we're not in a transaction that might prevent seeing the new tables
        conn.commit()

        # IMPORTANT: After tables are created in a separate connection, we need to verify they're
        # visible in this connection. Some DBMS (particularly with certain isolation levels)
        # might not immediately see tables created in other connections.
        tables_exist = False
        retry_count = 0
        max_retries = 3

        while not tables_exist and retry_count < max_retries:
            try:
                with conn.cursor() as cur:
                    # Check for tables
                    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'events');")
                    has_events = cur.fetchone()[0]
                    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'reservations');")
                    has_reservations = cur.fetchone()[0]

                    if has_events and has_reservations:
                        # Verify the test event exists
                        cur.execute("SELECT count(*) FROM events WHERE event_id = %s;", (EVENT_ID,))
                        has_test_event = cur.fetchone()[0] > 0

                        if has_test_event:
                            tables_exist = True
                            break
                        else:
                            print(f"Test event ID {EVENT_ID} not found. Retrying verification...")
                    else:
                        missing = []
                        if not has_events: missing.append("events")
                        if not has_reservations: missing.append("reservations")
                        print(f"Tables still missing: {', '.join(missing)}. Retrying verification...")

                # If tables not verified, wait a bit and try again
                if not tables_exist:
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(1)  # Brief pause before retrying
            except Exception as e:
                print(f"Error during table verification (attempt {retry_count+1}): {e}")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(1)

        # Final verification result
        return tables_exist

    except Exception as e:
        print(f"Failed to create or verify tables: {e}")
        return False

class ReservationUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        # Each simulated user has its own connection
        try:
            self.conn = get_connection()
            self.conn.autocommit = False

            # Set isolation level explicitly for each connection
            with self.conn.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

            # Generate a unique user ID for each simulated user
            self.user_id = str(uuid.uuid4())

            # Diagnostic check - verify tables directly
            with self.conn.cursor() as cur:
                try:
                    cur.execute("SELECT 1 FROM public.events LIMIT 1;")
                    print(f"User {self.user_id[:8]}: Successfully verified events table exists")
                except Exception as e:
                    print(f"User {self.user_id[:8]}: Cannot access events table: {e}")
                    # Force setup of tables again
                    self.conn.rollback()  # Clear any transaction state
                    if not ensure_tables_exist(self.conn):
                        print(f"User {self.user_id[:8]}: Could not create required tables. Aborting.")
                        if not self.conn.closed:
                            self.conn.close()
                        self.environment.runner.quit()
                        return

        except Exception as e:
            print(f"Error during user startup: {e}")
            # Force the user to stop if we can't get a connection
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.close()
            self.environment.runner.quit()

    def on_stop(self):
        if hasattr(self, "conn"):
            try:
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

    @task(1)
    def scenario_reservation_booking(self):
        """
        Scenario 2: Reservation System (Concurrent Slot Booking)
        Each task attempts to book one slot for an event if available.

        Operation Pattern: Read-then-write logic with conditional checks and lock-based enforcement.
        System Stress: Concurrent transactions on constrained resources, with risk of double booking.
        """
        start = time.time()
        try:
            # First verify the connection is still valid
            if not hasattr(self, "conn") or self.conn.closed:
                print(f"User {self.user_id[:8]}: Reconnecting due to closed connection")
                self.conn = get_connection()
                self.conn.autocommit = False

            cur = self.conn.cursor()

            # Use public schema explicitly in all queries for clarity
            # First check if there are available slots
            # Use SELECT FOR UPDATE to lock the row and prevent concurrent modifications
            cur.execute(
                """
                SELECT available_slots
                FROM public.events
                WHERE event_id = %s
                FOR UPDATE;
                """,
                (EVENT_ID,)
            )

            result = cur.fetchone()
            if not result or result[0] <= 0:
                # No available slots
                raise Exception("No available slots")

            available_slots = result[0]

            # Now attempt to book a slot
            cur.execute(
                """
                UPDATE public.events
                SET available_slots = available_slots - 1,
                    booked_slots = booked_slots + 1
                WHERE event_id = %s
                  AND available_slots > 0
                RETURNING available_slots;
                """,
                (EVENT_ID,)
            )

            if cur.rowcount == 0:
                # No rows updated → booking failed
                raise Exception("Booking failed - no available slots")

            # Create a reservation record
            reservation_id = str(uuid.uuid4())

            cur.execute(
                """
                INSERT INTO public.reservations (
                    reservation_id,
                    event_id,
                    user_id,
                    reservation_time
                ) VALUES (%s, %s, %s, NOW());
                """,
                (reservation_id, EVENT_ID, self.user_id)
            )

            self.conn.commit()
            elapsed = int((time.time() - start) * 1000)
            self.log_success("reservation_booking", elapsed)
        except Exception as e:
            if hasattr(self, "conn") and not self.conn.closed:
                self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            self.log_failure("reservation_booking", elapsed, e)

# Add event listeners for test lifecycle management
@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    global test_start_time
    test_start_time = time.time()  # Record the start time

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
            # Use read committed for better visibility
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

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

            # *** CRITICAL FIX: Completely recreate schema and tables ***

            # First drop schema and recreate to ensure clean state
            print("Recreating public schema...")
            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public;")

            # Grant permissions
            cur.execute("GRANT ALL ON SCHEMA public TO current_user;")
            cur.execute("GRANT ALL ON SCHEMA public TO public;")

            print("Creating reservation system tables from scratch...")

            # Create events table - explicitly don't use IF NOT EXISTS
            cur.execute("""
                CREATE TABLE public.events (
                    event_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    total_slots INT NOT NULL,
                    available_slots INT NOT NULL,
                    booked_slots INT NOT NULL DEFAULT 0,
                    event_date TIMESTAMP NOT NULL
                );
            """)
            print("Created events table")

            # Create reservations table - explicitly don't use IF NOT EXISTS
            cur.execute("""
                CREATE TABLE public.reservations (
                    reservation_id VARCHAR(36) PRIMARY KEY,
                    event_id INT REFERENCES public.events(event_id),
                    user_id VARCHAR(36) NOT NULL,
                    reservation_time TIMESTAMP NOT NULL,
                    status VARCHAR(20) DEFAULT 'confirmed'
                );
            """)
            print("Created reservations table")

            # Add test event
            cur.execute("""
                INSERT INTO public.events (
                    event_id, name, description, total_slots, available_slots, event_date
                ) VALUES (
                    %s, 'Test Event', 'Event for load testing', %s, %s, NOW() + INTERVAL '1 day'
                );
            """, (EVENT_ID, MAX_SLOTS, MAX_SLOTS))
            print(f"Created test event with ID {EVENT_ID} and {MAX_SLOTS} slots")

            # Verify tables and data
            cur.execute("SELECT COUNT(*) FROM public.events;")
            event_count = cur.fetchone()[0]
            print(f"Verified events table contains {event_count} records")

            cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
            table_count = cur.fetchone()[0]
            print(f"Total tables in public schema: {table_count}")

            # List all tables in public schema for diagnostics
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = [row[0] for row in cur.fetchall()]
            print(f"Tables in public schema: {', '.join(tables)}")

            # Verify schema permissions
            cur.execute("""
                SELECT grantee, privilege_type
                FROM information_schema.role_table_grants
                WHERE table_schema = 'public' AND table_name = 'events'
                ORDER BY grantee, privilege_type;
            """)
            permissions = [(row[0], row[1]) for row in cur.fetchall()]
            if permissions:
                print(f"Permissions on events table: {permissions}")
            else:
                print("Warning: No explicit permissions found on events table")
                # Apply explicit permissions
                cur.execute("GRANT ALL ON public.events TO current_user;")
                cur.execute("GRANT ALL ON public.events TO public;")
                cur.execute("GRANT ALL ON public.reservations TO current_user;")
                cur.execute("GRANT ALL ON public.reservations TO public;")
                print("Applied explicit table permissions")

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
