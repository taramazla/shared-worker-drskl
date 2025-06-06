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

class FinancialTransactionUser(User):
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
            cur = self.conn.cursor()
            
            # Begin transaction with serializable isolation to prevent anomalies
            cur.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")
            
            # Check if source account has enough funds - lock the row with SELECT FOR UPDATE
            cur.execute(
                """
                SELECT balance 
                FROM accounts 
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
                UPDATE accounts 
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
                UPDATE accounts 
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
                INSERT INTO transactions (
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
                INSERT INTO transaction_logs (
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
            events.request_success.fire(
                request_type="DB",
                name="account_transfer",
                response_time=elapsed,
                response_length=0,
            )
        except Exception as e:
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            events.request_failure.fire(
                request_type="DB",
                name="account_transfer",
                response_time=elapsed,
                exception=e,
            )

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
            cur = self.conn.cursor()
            
            # Read-only transaction - doesn't need high isolation
            cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            
            # Check account balance
            cur.execute(
                """
                SELECT balance 
                FROM accounts 
                WHERE account_id = %s;
                """, 
                (account_id,)
            )
            
            result = cur.fetchone()
            if not result:
                raise Exception(f"Account {account_id} not found")
            
            self.conn.commit()
            
            elapsed = int((time.time() - start) * 1000)
            events.request_success.fire(
                request_type="DB",
                name="check_balance",
                response_time=elapsed,
                response_length=0,
            )
        except Exception as e:
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            events.request_failure.fire(
                request_type="DB",
                name="check_balance",
                response_time=elapsed,
                exception=e,
            )

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
            cur = self.conn.cursor()
            
            # Begin transaction
            if not is_deposit:
                # For withdrawal, need to check balance first
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
                
                # Check if account has enough funds
                cur.execute(
                    """
                    SELECT balance 
                    FROM accounts 
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
                    UPDATE accounts 
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
                # For deposit, no need to check balance
                cur.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
                
                # Get current balance for logging
                cur.execute(
                    """
                    SELECT balance 
                    FROM accounts 
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
                    UPDATE accounts 
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
                INSERT INTO account_transactions (
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
            events.request_success.fire(
                request_type="DB",
                name=f"account_{transaction_type}",
                response_time=elapsed,
                response_length=0,
            )
        except Exception as e:
            self.conn.rollback()
            elapsed = int((time.time() - start) * 1000)
            events.request_failure.fire(
                request_type="DB",
                name="account_deposit_withdraw",
                response_time=elapsed,
                exception=e,
            )

# The SQL setup is now handled by setup_database() function
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
   - The setup_database() function runs automatically when this file is loaded
   - This ensures the database is ready before any load testing begins
"""
