# Database connection details
export PGPASSWORD=""

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


# Dump the entire source database in plain-text SQL format
echo "Dumping the source database..."
pg_dump -h $HOST -p $PORT -U $USER -F p -b -v --file="source_db_backup.sql" $SOURCE_DB_NAME

# Restore the dump into the destination database
echo "Restoring dump into the destination database..."
psql -h $HOST -p $PORT -U $USER -d $DEST_DB_NAME -f "source_db_backup.sql"

echo "Migration completed successfully."
