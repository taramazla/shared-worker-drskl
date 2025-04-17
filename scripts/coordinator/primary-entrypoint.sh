#!/bin/bash
set -e

echo "Starting primary coordinator initialization..."

# Check if PostgreSQL is already initialized
if [ ! -s "/var/lib/postgresql/data/PG_VERSION" ]; then
    echo "📥 Initializing PostgreSQL data directory..."

    # Set environment variables
    POSTGRES_USER=${POSTGRES_USER:-postgres}
    POSTGRES_DB=${POSTGRES_DB:-postgres}
    POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-postgres}

    # Clear out the data directory to avoid permission issues
    rm -rf /var/lib/postgresql/data/*

    # Initialize the database with initdb directly
    gosu postgres initdb -D /var/lib/postgresql/data

    # Start PostgreSQL temporarily to create users and databases
    gosu postgres pg_ctl -D /var/lib/postgresql/data -o "-c listen_addresses=''" -w start

    # Create user and database
    gosu postgres psql -v ON_ERROR_STOP=1 --username postgres <<-EOSQL
        CREATE USER "$POSTGRES_USER" WITH SUPERUSER PASSWORD '$POSTGRES_PASSWORD';
        CREATE DATABASE "$POSTGRES_DB" WITH OWNER "$POSTGRES_USER";
        GRANT ALL PRIVILEGES ON DATABASE "$POSTGRES_DB" TO "$POSTGRES_USER";
EOSQL

    # Stop PostgreSQL after initialization
    gosu postgres pg_ctl -D /var/lib/postgresql/data -m fast -w stop

    # Ensure archive directory exists
    mkdir -p /var/lib/postgresql/data/archive

    # Configure PostgreSQL
    echo "📝 Configuring PostgreSQL..."
    cat >> /var/lib/postgresql/data/postgresql.conf << EOF

# Active-active configuration
listen_addresses = '*'
synchronous_commit = on
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
EOF

    # Configure pg_hba.conf with explicit replication permissions
    echo "📝 Configuring pg_hba.conf for replication..."
    cat > /var/lib/postgresql/data/pg_hba.conf << EOF
# TYPE  DATABASE        USER            ADDRESS                 METHOD

# "local" is for Unix domain socket connections only
local   all             all                                     trust
# IPv4 local connections:
host    all             all             127.0.0.1/32            trust
# IPv6 local connections:
host    all             all             ::1/128                 trust
# Allow replication connections from anywhere, for example from standby
local   replication     all                                     trust
host    replication     all             0.0.0.0/0               trust
host    replication     all             ::0/0                   trust
# Allow all connections from Docker network
host    all             all             0.0.0.0/0               trust
EOF

    # Fix permissions on data directory
    echo "Setting ownership of data directory to postgres..."
    chown -R postgres:postgres /var/lib/postgresql/data
    chmod 0700 /var/lib/postgresql/data

    echo "✅ PostgreSQL initialization completed."
else
    echo "📝 PostgreSQL data directory already exists."

    # Ensure replication is allowed in pg_hba.conf even if we didn't initialize
    echo "📝 Updating pg_hba.conf to allow replication..."

    # Check if replication entries already exist
    if ! grep -q "host    replication     all             0.0.0.0/0" /var/lib/postgresql/data/pg_hba.conf; then
        echo "host    replication     all             0.0.0.0/0               trust" >> /var/lib/postgresql/data/pg_hba.conf
    fi

    echo "📝 Reloading PostgreSQL configuration..."
fi

echo "✅ Starting PostgreSQL..."
exec gosu postgres postgres
