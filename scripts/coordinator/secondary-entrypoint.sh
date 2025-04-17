#!/bin/bash
set -e

# Remove existing data directory contents
echo "📥 Clearing old data from secondary coordinator..."
rm -rf /var/lib/postgresql/data/*

# Wait for the primary to be available
echo "⏳ Waiting for primary coordinator to be ready..."
until PGPASSWORD=citus pg_isready -h citus_coordinator_primary -U citus; do
    echo "Primary coordinator not ready, waiting..."
    sleep 5
done

echo "Primary coordinator is ready, continuing setup..."

# Test the replication connection before attempting pg_basebackup
echo "🔎 Testing replication connection to primary..."
PGPASSWORD=citus psql -h citus_coordinator_primary -U citus -c "SHOW max_wal_senders;" || {
    echo "❌ Failed to connect to primary to check replication settings. Please check primary pg_hba.conf."
    sleep 10
    exit 1
}

# Clone the primary data
echo "🔄 Cloning primary data using pg_basebackup..."
PGPASSWORD=citus gosu postgres pg_basebackup -h citus_coordinator_primary -U citus -D /var/lib/postgresql/data -P -X stream -v

# Configure PostgreSQL to work as an active node
echo "📝 Configuring PostgreSQL for active-active setup..."
cat > /var/lib/postgresql/data/postgresql.auto.conf << EOF
# Add configuration for active-active setup
synchronous_commit = on
listen_addresses = '*'
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
echo "🔧 Fixing permissions on data directory..."
chown -R postgres:postgres /var/lib/postgresql/data
chmod 0700 /var/lib/postgresql/data

echo "✅ Starting PostgreSQL as active secondary coordinator..."
exec gosu postgres postgres
