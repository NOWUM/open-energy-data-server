# Open Energy Data Server

This is a repository which contains python web-crawler scripts to download various available data, which is useful for simulation or analysis of Energy Systems.

The main target is to create an institute-wide available database which can be set up once and then be used by multiple researchers.

Allowing native access through postgreSQL allows any easy integration of different software which can access data from a SQL database.

## Getting started

To set up your institutes new open-data server, you can [install docker](https://docs.docker.com/engine/install/) or podman.
And start the `docker-compose.yml` with `docker compose up -d`.

Then you have a running TimescaleDB server listening on postgresql default port `5432`.

You can install all python dependencies:

`pip install -r requirements.txt`

An finally run the main crawling script `python crawl_all.py` to download all available sources into the database.

## Using the ECMWF crawler

If you want to use the ECMWF crawler it is recommended to set up and use a conda environment for installing the necessary dependencies.
Start by [installing conda](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).
When using Windows, you need to start the Anaconda Prompt to run conda commands or add conda to PowerShell:
- `powershell -ExecutionPolicy ByPass -NoExit -Command "& 'C:\Users\USER\anaconda3\shell\condabin\conda-hook.ps1' ; conda activate 'C:\Users\USER\anaconda3' "`
- `conda init powershell`
- `conda config --set auto_activate_base false`
Afterward, start a terminal and navigate to the open-energy-data-server directory to run the command `conda env create -f environment.yml` to install the necessary dependencies.
Now activate the environment by executing `conda activate open-energy-data-server` in the terminal.
With the environment activated, you can now run the ECMWF crawler by running `python ./ecmwf_crawler.py` from within the open-energy-data-server directory.

## TimeScaleDB

The used database technology for the database server ist [TimescaleDB](https://timescale.com/) which is an extension for PostgreSQL (just like PostGIS but for timeseries databases).

### What is a time-series database?

Normal sql table can get quite slow if millions of entries are stored in them.

Luckily, timeseries data has the property of always having a seperation at the time column.
This can be used for sharding of the database table.

Popular systems like InfluxDB are using this to improve queries with data aggregation or long time history analysis.
Unfortunatley, such databases do not allow to store data without a time column.
For example metadata or lists of existing power plants.

To be able to use both, TimeScaleDB seemed to be the best candidate.
The Grafana integration works also very well and clients can work with it, just like with every PostgreSQL server, without having a new query language to learn (like Flux for example).

### Replication

TimescaleDB allows to have replication across multiple servers for load balancing and improvments for reading (and sometimes writing) timeseries data.
This works by using [Distributed Hypertables](https://docs.timescale.com/timescaledb/latest/how-to-guides/distributed-hypertables).

On a high level this can be imagined that for a query spanning a year, each of three nodes calculates and aggregates the query result for 4 month - resulting in a higher performance.
This only works for timeseries tables and is not compatible with non-timeseries data.
Therefore to increase replication of other tables (like the Marktstammdatenregister), one still needs to have manual replication or use something like [Patroni](https://patroni.readthedocs.io/en/latest/).


## Contributing

Do you know of other interesting open-access databases which are worth mentioning here?

Maybe some which are to volatile, large or unknown and are therefore not useful to store them in the [OEP](https://openenergy-platform.org/)?

Just send a PR and add a new file in the crawler folder with the main method signature as

```
def main(db_uri):
    pass
```

If your tables should be stored in a new database, you have to add your database to the [init.sql](./init.sql) script too.
