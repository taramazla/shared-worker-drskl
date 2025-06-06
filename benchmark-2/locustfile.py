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

# Setup database tables before the test starts
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

    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        raise

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

# Initialize the database when the file is loaded
setup_database()

class ReservationUser(User):
    wait_time = between(1, 2)

    def on_start(self):
        # Each simulated user has its own connection
        self.conn = get_connection()
        self.conn.autocommit = False

        # Generate a unique user ID for each simulated user
        self.user_id = str(uuid.uuid4())

    def on_stop(self):
        if hasattr(self, "conn"):
            self.conn.close()

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
            cur = self.conn.cursor()

            # First check if there are available slots
            # Use SELECT FOR UPDATE to lock the row and prevent concurrent modifications
            cur.execute(
                """
                SELECT available_slots
                FROM events
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
                UPDATE events
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
                INSERT INTO reservations (
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
            events.request_success.fire(
                request_type="DB",
                name="reservation_booking",
                response_time=elapsed,
                response_length=0,
            )
        except Exception as e:
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            events.request_failure.fire(
                request_type="DB",
                name="reservation_booking",
                response_time=elapsed,
                exception=e,
            )

# The SQL setup is now handled by setup_database() function
"""
Setup Logic:
1. Tables Created:
   - events: Stores event information and slot availability
   - reservations: Records individual bookings

2. Test Data Initialization:
   - Creates a test event with 100 available slots
   - If the event already exists, resets its available slots to 100

3. When to Run:
   - The setup_database() function runs automatically when this file is loaded
   - This ensures the database is ready before any load testing begins
"""
