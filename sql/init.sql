-- Connect to the Citus database
\c citus;

-- Create a distributed table
CREATE TABLE vehicle_locations (
  id bigserial,
  vehicle_id int NOT NULL,
  location geometry(Point, 4326) NOT NULL,
  recorded_at timestamptz NOT NULL,
  region_code text NOT NULL  -- Make sure it's NOT NULL if it's the distribution column
);

-- Create necessary indexes
CREATE INDEX idx_vehicle_locations_location ON vehicle_locations USING GIST (location);
CREATE INDEX idx_vehicle_locations_region_code ON vehicle_locations (region_code);

-- Distribute the table by region_code
SELECT create_distributed_table('vehicle_locations', 'region_code');

-- Insert 1,000,000 dummy rows into vehicle_locations
INSERT INTO vehicle_locations (vehicle_id, location, recorded_at, region_code)
SELECT
  -- Generate a random vehicle_id between 1 and 10,000
  (floor(random() * 10000) + 1)::int AS vehicle_id,

  -- Generate a random point near New York City, for example.
  -- Adjust the longitude and latitude ranges as needed.
  ST_SetSRID(
    ST_MakePoint(
      -74.0 + random() * 0.5,  -- longitude between -74.0 and -73.5
      40.7 + random() * 0.5    -- latitude between 40.7 and 41.2
    ),
    4326
  ) AS location,

  -- Generate a random timestamp within the last 30 days
  NOW() - (random() * interval '30 days') AS recorded_at,

  -- Randomly assign one of three region codes. Adjust or add more as needed.
  CASE
    WHEN random() < 0.33 THEN 'region_north'
    WHEN random() < 0.66 THEN 'region_south'
    ELSE 'region_central'
  END AS region_code
FROM generate_series(1, 1000000) s(i);

SELECT count(*) FROM vehicle_locations;

SELECT * FROM pg_dist_shard;

SELECT * FROM master_get_active_worker_nodes();

EXPLAIN ANALYZE
SELECT id, vehicle_id, recorded_at, region_code
FROM vehicle_locations
WHERE ST_DWithin(
        location::geography,
        ST_SetSRID(ST_MakePoint(-73.9857, 40.7484), 4326)::geography,
        5000
      );

EXPLAIN ANALYZE
SELECT *
FROM vehicle_locations
WHERE ST_Within(
    location,
    ST_MakeEnvelope(-74.0, 40.7, -73.9, 40.8, 4326)
);