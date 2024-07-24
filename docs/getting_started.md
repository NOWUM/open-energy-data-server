# Getting Started
In this document you will find the necessary steps to set up the server and execute the crawlers.

## Requirements

To set up the server itself:
- [Docker](https://docs.docker.com/engine/install/) or [Podman](https://podman.io/).

To execute crawlers and minimal walkthrough:
- [Python](https://www.python.org/downloads/release/python-390/ ) (tested with version 3.9.0).

## Setting up the server

Open the terminal at the root of the project and run the following command to start all services in the [docker compose file](/compose.yml):

```bash
docker-compose up -d
```

This will start:
- The Postgresql database, found at port [6432](http://localhost:6432 ).
- The PgAdmin database administration software, found at port [8080](http://localhost:8080/).
- The PostgREST server, found at port [3001](http://localhost:3001/).
- The Grafana monitoring tool, found at port [3006](http://localhost:3006/).

To verify a successful start, you can check the logs of the services by running:

```bash
docker ps
```

If everything went well, you should see four services running at the previously mentioned ports.


You can stop the services by running:

```bash
docker-compose down
```

## Crawlers
Once again using a terminal at the root of the project, install the required python packages by running:

```bash
pip install -r requirements.txt
```
Next, you can run all crawlers by running:

WARNING: This will take a long time and consume a lot of resources. It is recommended to run over the weekend or indivudually.

```bash
python crawl_all.py
```

or as a module:

```bash
python -m crawl_all
```

This will execute all crawlers and populate the database with data and metadata.

To run a single crawler, navigate to the /crawler directory and run:

```bash
python <crawler_name>.py
```

or as a module:

```bash
python -m <crawler_name>
```

## Usage

### PgAdmin
PgAdmin is provisioned using this [configuration](/data/provisioning/pgadmin/servers.json)

If there are no servers shown upon startup, you can manually add the server using the same credentials as the provisioning, by right clicking the servers list in the "Object Explorer" of the PgAdmin tool and selecting "Register" -> "Server".

By default, the database is empty except for admnistration tables. You can add your own data or use the provided crawlers to populate the database.

### PostgREST
PostgREST can be used to query the database using a RESTful API. The API is available at [http://localhost:3001/](http://localhost:3001/).

Any table in any schema can be read from, but tables outisde the "public" schema require setting of additional header parameters to function.

For examples see: [http export examples](/docs/http_export_examples.md) or [Example Projects](https://github.com/NOWUM/OEDS-Examples).

### Grafana
Grafana is provisioned using this [configuration](/data/provisioning/grafana/provisioning/datasources/datasource.yml) and this [dashboard](/data/provisioning/grafana/provisioning/dashboards/dashboard.yml).

If there are no datasources and dashboards shown upon startup, you can manually add the datasource and dashboard using the same configuration as the provisioning.

The provided dashboards will be empty until the related crawlers are executed.

## More resources

A complete walkthrough with a new crawler, PostgREST usage and data exporting can be found [here](/docs/minimal%20walkthrough/).

Further documentation & examples exists regarding [http export](/docs/http_export_examples.md) and [client export](/docs/client_export_examples.md).

Example projects can be found in the [OEDS-Examples github](https://github.com/NOWUM/OEDS-Examples).
