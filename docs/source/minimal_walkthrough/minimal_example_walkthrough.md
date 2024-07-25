# Minimal example
Using the opsd national energy generation dataset as an example, this document contains a minimal example of how to:
- Add dataset using a crawler.
- Access the dataset using PostGREST.
- Visualise the data using Javascript, Grafana or Python.

## Add dataset using a python crawler
First, fetch the data. Then extract it, transform it if necessary and load it into the database.

Example crawler in:
[opsd_national_generation_capacity_crawler.py](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/opsd_national_generation_capacity_crawler.py)

## Access the dataset using PostGREST
If possible, access the data using a simple query over a PostGREST table endpoint using the "Accept-Content" header for schema selection.

[PostGREST API documentation Schemas](https://postgrest.org/en/v12/)

Alternatively create a stored procedure and access it via the rpc endpoint.

[PostGREST API documentation Functions](https://postgrest.org/en/v12/references/api/functions.html)

Create a new SQL type and a query which returns it, then create a stored procedure which returns the query result.

Example stored function SQL in:
[postgrest_stored_procedure.sql](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/postgrest_stored_procedure.sql)

## Visualise the data using Javascript, Grafana or Python
Use the PostGREST API to access the data and visualise it using Javascript, Grafana or Python.

Built in grafana visualisation is available on port 3008, the database connection is provisioned by default.

Javascript examples can be found in application_examples.md

Example Python script in:
[python_postgrest_visualise.py](https://github.com/NOWUM/open-energy-data-server/blob/main/docs/source/minimal_walkthrough/python_postgrest_visualise.py) 