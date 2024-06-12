-- SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
--
-- SPDX-License-Identifier: AGPL-3.0-or-later
CREATE EXTENSION postgis;
CREATE SCHEMA axxteq;
CREATE SCHEMA weather;
CREATE SCHEMA eex;
CREATE SCHEMA enet;
CREATE SCHEMA entsoe;
CREATE SCHEMA entsog;
CREATE SCHEMA frequency;
CREATE SCHEMA jao;
CREATE SCHEMA mastr;
CREATE SCHEMA nuts;
CREATE SCHEMA ninja;
CREATE SCHEMA scigrid;
CREATE SCHEMA windmodel;
CREATE SCHEMA opsd;
CREATE SCHEMA e2watch;
CREATE SCHEMA eview;
CREATE SCHEMA smard;
CREATE SCHEMA ladesaeulenregister;
CREATE SCHEMA gie;
CREATE SCHEMA iwugebaeudetypen;
CREATE SCHEMA nrw_kwp_waermedichte;
CREATE ROLE readonly WITH LOGIN PASSWORD 'readonly' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
GRANT pg_read_all_data TO readonly;
ALTER ROLE readonly SET search_path TO public;
ALTER ROLE opendata SET search_path TO public;
CREATE TABLE public.metadata (
    schema_name TEXT PRIMARY KEY,
    crawl_age DATE,
    data_age DATE,
    data_source TEXT,
    licence TEXT,
    description TEXT,
    contact TEXT,
    tables INTEGER,
    bbox_min_lat NUMERIC,
    bbox_max_lat NUMERIC,
    bbox_min_lon NUMERIC,
    bbox_max_lon NUMERIC,
    temporal_start TIMESTAMP,
    temporal_end TIMESTAMP,
    size BIGINT
);