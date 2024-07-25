# Backup & Restore OEDS
The OEDS uses the timescaledb and postgis extensions for PostgreSQL.
These extensions need to be considered when migrating the database.

## Prerequisites
- Make sure that the target server exists and is runnning.
- Make sure that the target server has the timescaledb and postgis extensions installed.

If using the docker-compose setup, the target server should already have timescaledb installed.

Postgis is automatically installed by the init.sql script.

Postgis can be manually installed using the following statement:
```sql
CREATE EXTENSION postgis;
```
## Test environment
If you want to test the migration, dummy databses can be created using this script [create_dummy_databases.sh](create_dummy_databases.sh).

## Backup & restore whole server
Backing up and restoring the whole database is straightforward.

The script in [schema_migrate.sh](schema_migrate.sh) can be used to backup and restore the database.

## Backup & restore specific schemas (datasets) of the OEDS
Backing up and restoring specific schemas is a bit more complicated, because timescaledb stores its data in chunks.

The script in [schema_migrate.sh](schema_migrate.sh) can be used to backup and restore specific schemas.
