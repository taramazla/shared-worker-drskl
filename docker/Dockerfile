# Use the official Citus image as the base image
FROM citusdata/citus:latest

# Update package lists and install PostGIS
RUN apt-get update && \
    apt-get install -y postgis postgresql-13-postgis-3 && \
    rm -rf /var/lib/apt/lists/*