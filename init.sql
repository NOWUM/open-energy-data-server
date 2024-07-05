-- SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
CREATE EXTENSION postgis;
CREATE SCHEMA axxteq;
CREATE SCHEMA e2watch;
CREATE SCHEMA eex;
CREATE SCHEMA enet;
CREATE SCHEMA entsoe;
CREATE SCHEMA entsog;
CREATE SCHEMA eview;
CREATE SCHEMA frequency;
CREATE SCHEMA gie;
CREATE SCHEMA iwugebaeudetypen;
CREATE SCHEMA jao;
CREATE SCHEMA ladesaeulenregister;
CREATE SCHEMA londondatastore;
CREATE SCHEMA mastr;
CREATE SCHEMA netztransparenz;
CREATE SCHEMA ninja;
CREATE SCHEMA nrw_kwp_waermedichte;
CREATE SCHEMA opsd;
CREATE SCHEMA refit;
CREATE SCHEMA postgrest;
CREATE SCHEMA scigrid;
CREATE SCHEMA smard;
CREATE SCHEMA windmodel;
CREATE SCHEMA weather;
CREATE ROLE readonly WITH LOGIN PASSWORD 'readonly' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
GRANT pg_read_all_data TO readonly;
ALTER ROLE readonly SET search_path TO public;
ALTER ROLE opendata SET search_path TO public;
CREATE TABLE public.metadata (
    schema_name TEXT PRIMARY KEY,
    crawl_date DATE,
    data_date DATE,
    data_source TEXT,
    licence TEXT,
    description TEXT,
    contact TEXT,
    tables INTEGER,
    concave_hull_geometry GEOMETRY,
    temporal_start TIMESTAMP,
    temporal_end TIMESTAMP,
    size BIGINT
);
create or replace function postgrest.pre_config()
returns void as $$
  select
    set_config('pgrst.db_schemas', string_agg(nspname, ','), true)
  from pg_namespace
  where nspname not like '%timescaledb%'
	and nspname not like '%information_schema%'
	and nspname not like '%pg%';
$$ language sql;

NOTIFY pgrst, 'reload config';
NOTIFY pgrst, 'reload schema';