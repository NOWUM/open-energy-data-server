#!/bin/bash

# Define database connection details
export PGPASSWORD=""
ADMIN_DB="postgres" 
HOST=""
PORT=""
USER="opendata"

TEST_DB="source_testdb"
DEST_TEST_DB="dest_testdb"

# Connect to the server and create src database
psql -h $HOST -p $PORT -U $USER -d $ADMIN_DB <<EOF
CREATE DATABASE "$TEST_DB";
\connect "$TEST_DB"
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EOF

# Connect to the server and create dest database
psql -h $HOST -p $PORT -U $USER -d $ADMIN_DB <<EOF
CREATE DATABASE "$DEST_TEST_DB";
\connect "$DEST_TEST_DB"
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EOF

# Create schemas and tables
psql -h $HOST -p $PORT -U $USER -d $TEST_DB <<EOF
-- Schema for standard tables
CREATE SCHEMA standard_tables;

-- Schema for hypertables
CREATE SCHEMA hypertables;

-- Schema for mixed types in hypertables with PostGIS
CREATE SCHEMA hypertables_postgis;

-- Set up a sample table in 'standard_tables'
CREATE TABLE standard_tables.sample_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

-- Set up a sample hypertable in 'hypertables'
CREATE TABLE hypertables.temp_data (
    time TIMESTAMPTZ NOT NULL,
    temperature DOUBLE PRECISION
);
SELECT create_hypertable('hypertables.temp_data', 'time');

-- Set up a normal table in 'hypertables_postgis'
CREATE TABLE hypertables_postgis.regular_data (
    id SERIAL PRIMARY KEY,
    description TEXT
);

-- Set up a hypertable in 'hypertables_postgis'
CREATE TABLE hypertables_postgis.more_temp_data (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);
SELECT create_hypertable('hypertables_postgis.more_temp_data', 'time');

-- Set up a hypertable with spatial data in 'hypertables_postgis'
CREATE TABLE hypertables_postgis.spatial_data (
    time TIMESTAMPTZ NOT NULL,
    location GEOGRAPHY(Point, 4326),
    data DOUBLE PRECISION
);
SELECT create_hypertable('hypertables_postgis.spatial_data', 'time');
EOF

# Populate tables with dummy data
psql -h $HOST -p $PORT -U $USER -d $TEST_DB <<EOF
-- Insert data into 'standard_tables'
INSERT INTO standard_tables.sample_table (name) VALUES
('John'), ('Jane'), ('Dave'), ('Sarah'), ('Alice'),
('Bob'), ('Carol'), ('Ted'), ('Nina'), ('Mike');

-- Insert data into 'hypertables'
INSERT INTO hypertables.temp_data (time, temperature) VALUES
(NOW(), 23.5), (NOW() - interval '1 hour', 22.0), (NOW() - interval '2 hour', 21.5),
(NOW() - interval '3 hour', 20.0), (NOW() - interval '4 hour', 19.5),
(NOW() - interval '5 hour', 19.0), (NOW() - interval '6 hour', 18.5),
(NOW() - interval '7 hour', 18.0), (NOW() - interval '8 hour', 17.5),
(NOW() - interval '9 hour', 17.0);

-- Insert data into 'hypertables_postgis.regular_data'
INSERT INTO hypertables_postgis.regular_data (description) VALUES
('Desc 1'), ('Desc 2'), ('Desc 3'), ('Desc 4'), ('Desc 5'),
('Desc 6'), ('Desc 7'), ('Desc 8'), ('Desc 9'), ('Desc 10');

-- Insert data into 'hypertables_postgis.more_temp_data'
INSERT INTO hypertables_postgis.more_temp_data (time, value) VALUES
(NOW(), 100), (NOW() - interval '1 hour', 95), (NOW() - interval '2 hour', 90),
(NOW() - interval '3 hour', 85), (NOW() - interval '4 hour', 80),
(NOW() - interval '5 hour', 75), (NOW() - interval '6 hour', 70),
(NOW() - interval '7 hour', 65), (NOW() - interval '8 hour', 60),
(NOW() - interval '9 hour', 55);

-- Insert data into 'hypertables_postgis.spatial_data' with random points in Germany
INSERT INTO hypertables_postgis.spatial_data (time, location, data) VALUES
(NOW(), ST_MakePoint(6.961899, 50.936935), 15.5),  -- Cologne
(NOW() - interval '1 hour', ST_MakePoint(9.993682, 53.551086), 14.0),  -- Hamburg
(NOW() - interval '2 hour', ST_MakePoint(13.4050, 52.5200), 13.5),  -- Berlin
(NOW() - interval '3 hour', ST_MakePoint(11.581981, 48.135125), 12.0),  -- Munich
(NOW() - interval '4 hour', ST_MakePoint(8.682127, 50.110924), 11.5),  -- Frankfurt
(NOW() - interval '5 hour', ST_MakePoint(9.182932, 48.775846), 11.0),  -- Stuttgart
(NOW() - interval '6 hour', ST_MakePoint(8.403653, 49.006889), 10.5),  -- Karlsruhe
(NOW() - interval '7 hour', ST_MakePoint(10.013654, 53.565278), 10.0),  -- Bremen
(NOW() - interval '8 hour', ST_MakePoint(7.465298, 51.513587), 9.5),  -- Dortmund
(NOW() - interval '9 hour', ST_MakePoint(6.805920, 51.221347), 9.0);  -- Dusseldorf
EOF

echo "Dummy database setup and population complete"
