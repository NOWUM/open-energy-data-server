# Welcome to OEDS's Documentation!

Welcome to the official documentation for OEDS! This guide will help you get started, walk you through examples, and provide detailed instructions on backup, restore, and migration processes.

## Getting Started

* [Getting Started](getting_started.md) - Set up and deploy the OEDS.
* [Main README](README.md)


OEDS Architecture

![OEDS Architecture](media/oeds-architecture.png)

OEDS Workflow

![OEDS Workflow](media/oeds-workflow.png)

## Mini Walkthrough

* [Minimal Example Walkthrough](minimal_walkthrough/minimal_example_walkthrough.md) - Step-by-step walkthrough of a minimal example using Open Power System Data (OPSD) national energy generation dataset.
    * [opsd_national_generation_capacity_crawler.py](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/opsd_national_generation_capacity_crawler.py) - Python crawler for national generation capacity.
    * [postgrest_stored_procedure.sql](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/postgrest_stored_procedure.sql) - Stored procedure for PostgREST.
    * [python_postgrest_visualise.py](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/python_postgrest_visualise.py) - Python script for PostgREST visualization.
    * [walkthrough_util.py](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/walkthrough_util.py) - Utility functions for the walkthrough.

## Usage Examples

* [Application Examples](examples/application_examples.md) - Application examples.
* [Client Export Examples](examples/client_export_examples.md) - Export examples for sql clients.
* [HTTP Export Examples](examples/http_export_examples.md) - Examples for exporting via HTTP.
* [SQL Helpers](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/examples/metadata_sql_helpers.sql) - SQL helpers for metadata operations.

## Backup and Migration

* [Backup, Restore, and Migration](backup_restore_migration/backup_restore_migrate.md) - Comprehensive guide for backup, restore, and migration.
    * [dummy_db.sh](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/backup_restore_migration/dummy_db.sh) - Script for dummy database setup.
    * [db_migrate.sh](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/backup_restore_migration/db_migrate.sh) - Script for whole database migration.
    * [schema_migrate.sh](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/backup_restore_migration/schema_migrate.sh) - Script for individual schema migration.
