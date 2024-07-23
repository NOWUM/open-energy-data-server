# Password for the Postgresql user
export PGPASSWORD=""

# The schema to migrate, passed as an argument
TARGET_SCHEMA="$1"

# Database from which to copy
SOURCE_DB=""
SOURCE_HOST=""
SOURCE_PORT=""
SOURCE_USER="opendata"

# Database to which to copy
DEST_DB=""
DEST_HOST=""
DEST_PORT=""
DEST_USER="opendata"


# Extract tables
tables=( $(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT tablename FROM pg_tables WHERE schemaname = '$TARGET_SCHEMA';") )

# Extract hypertables and time columns
hypertables=( $(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT hypertable_name FROM timescaledb_information.dimensions WHERE hypertable_schema = '$TARGET_SCHEMA';") )
time_col=( $(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t -c "SELECT column_name FROM timescaledb_information.dimensions WHERE hypertable_schema = '$TARGET_SCHEMA';") )

#Create Target schema in target DB
psql -h $DEST_HOST -p $DEST_PORT -U $DEST_USER -d $DEST_DB <<EOF
CREATE SCHEMA IF NOT EXISTS $TARGET_SCHEMA;
EOF

# Migrate each normal table
for table in "${tables[@]}"; do
  # Step 1: Dump Normal table
  pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t $TARGET_SCHEMA.$table > ${table}.sql

  # Step 2: Apply Normal table dump
  psql -h $DEST_HOST -p $DEST_PORT -U $DEST_USER -d $DEST_DB -f ${table}.sql
done


# Migrate each hypertable
for idx in "${!hypertables[@]}"; do
  hypertable="${hypertables[$idx]}"
  column="${time_col[$idx]}"
  echo "Migrating hypertable: $hypertable"

  # Step 1: Dump and apply schema
  pg_dump -h $SOURCE_HOST -p $SOURCE_PORT -U $SOURCE_USER -d $SOURCE_DB -t $TARGET_SCHEMA.$hypertable --schema-only > "${hypertable}_schema.sql"
  psql -h $DEST_HOST -p $DEST_PORT -U $DEST_USER -d $DEST_DB -f "${hypertable}_schema.sql"

  # Step 2: Create hypertable
  psql -h $DEST_HOST -p $DEST_PORT -U $DEST_USER -d $DEST_DB <<EOF
DROP TRIGGER IF EXISTS ts_insert_blocker ON $TARGET_SCHEMA.$hypertable;
SELECT create_hypertable('$TARGET_SCHEMA.$hypertable', '$column');
EOF

  # Step 3: Copy data
  psql -U $SOURCE_USER -h $SOURCE_HOST -p $SOURCE_PORT -d $SOURCE_DB  -c "COPY (SELECT * FROM $TARGET_SCHEMA.$hypertable) TO STDOUT WITH CSV HEADER;" > ${hypertable}_data.csv
  psql -h $DEST_HOST -p $DEST_PORT -U $DEST_USER -d $DEST_DB -c "COPY $TARGET_SCHEMA.$hypertable FROM STDIN WITH CSV HEADER;" < ${hypertable}_data.csv
done

