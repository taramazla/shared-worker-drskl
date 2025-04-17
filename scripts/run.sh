#!/bin/bash

# Set executable permissions on all scripts
echo "Setting execute permissions on shell scripts..."
chmod +x *.sh
echo "Permissions updated."

# Start the containers
echo "Starting Docker containers..."
docker-compose down -v
docker-compose up -d

# Monitor logs
echo "Monitoring logs (Ctrl+C to stop)..."
docker-compose logs -f
