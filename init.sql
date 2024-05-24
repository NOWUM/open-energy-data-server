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
CREATE ROLE readonly WITH LOGIN PASSWORD 'readonly' NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION VALID UNTIL 'infinity';
GRANT pg_read_all_data TO readonly;
ALTER ROLE readonly SET search_path TO public;
ALTER ROLE opendata SET search_path TO public;
