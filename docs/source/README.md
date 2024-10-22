<!--
SPDX-FileCopyrightText: Florian Maurer, Christian Rieke

SPDX-License-Identifier: AGPL-3.0-or-later
-->

# Open Energy Data Server

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10607894.svg)](https://doi.org/10.5281/zenodo.10607894)

This is a repository that contains python web-crawler scripts to download various available data, which is useful for simulation or analysis of Energy Systems.

The main target is to create an institute-wide available database that can be set up once and then be used by multiple researchers.

Allowing native access through PostgreSQL allows any easy integration of different software which can access data from a SQL database.

![Basic outline of the architecture and included services](media/oeds-architecture.png)


## TimeScaleDB

The used database technology for the database server is [TimescaleDB](https://timescale.com/) which is an extension for PostgreSQL (just like PostGIS but for timeseries databases).

### What is a time-series database?
Normal SQL tables can get quite slow if millions of entries are stored in them.

Luckily, timeseries data has the property of always having a separation at the time column.
This can be used for sharding of the database table.

Popular systems like InfluxDB are using this to improve queries with data aggregation or long-time history analysis.
Unfortunately, such databases do not allow storing data without a time column.
For example metadata or lists of existing power plants.

To be able to use both, TimeScaleDB seemed to be the best candidate.
The Grafana integration works also very well and clients can work with it, just like with every PostgreSQL server, without having a new query language to learn (like Flux for example).

### Replication
TimescaleDB allows having replication across multiple servers for load balancing and improvements for reading (and sometimes writing) timeseries data.
This works by using [Distributed Hypertables](https://docs.timescale.com/timescaledb/latest/how-to-guides/distributed-hypertables).

On a high level this can be imagined that for a query spanning a year, each of the three nodes calculates and aggregates the query result for 4 months - resulting in a higher performance.
This only works for timeseries tables and is not compatible with non-timeseries data.
Therefore to increase replication of other tables (like the Marktstammdatenregister), one still needs to have manual replication or use something like [Patroni](https://patroni.readthedocs.io/en/latest/).

## PostGIS
The database server also includes the [PostGIS](https://postgis.net/) extension which allows for spatial queries and storage of geospatial data.
PostGIS is installed once per database and can be used by every schema afterwards.

### What is a geospatial database?
Geospatial databases are optimized for storing and querying geospatial data.
They can store points, lines, polygons, and other geospatial data types and can perform spatial queries like finding all points within a certain distance of a given point.
Coordinate transformations and other geospatial operations are also possible with PostGIS.

## Contributing

Do you know of other interesting open-access databases which are worth mentioning here?
Maybe some are too volatile, large or unknown and are therefore not useful to store in the [OEP](https://openenergy-platform.org/).

Just send a PR and add a new file in the crawler folder with the main method signature as

```
def main(db_uri):
    pass
```

If your tables should be stored in a new database, you have to add your database to the [init.sql](./init.sql) script too.


## Citation

You can cite the `open-energy-data-server` through the Conference proceedings:

> Maurer, F., Sejdija, J., & Sander, V. (2024, February 2). Decentralized energy data storages through an Open Energy Database Server. 1st NFDI4Energy Conference (NFDI4Energy), Hanover, Germany. https://doi.org/10.5281/zenodo.10607895

## Using the ECMWF crawler

If you want to use the ECMWF crawler you need to create an account at [copernicus](https://cds.climate.copernicus.eu) to get an API key which allows you to query the API of copernicus. Follow the [instructions](https://cds.climate.copernicus.eu/api-how-to) of copernicus for that.
