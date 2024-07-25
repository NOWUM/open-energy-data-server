# Minimal example
Using the opsd national energy generation dataset as an example, this document contains a minimal example of how to:
- Add dataset using a crawler.
- Access the dataset using PostGREST.
- Visualise the data using Javascript, Grafana or Python.

## Add dataset using a python crawler
First, fetch the data. Then extract it, transform it if necessary and load it into the database.

Example crawler in:
".\opsd_national_generation_capacity_crawler.py".

## Access the dataset using PostGREST
If possible, access the data using a simple query over a PostGREST table endpoint using the "Accept-Content" header for schema selection.

https://postgrest.org/en/v12/references/api/schemas.html

Alternatively create a stored procedure and access it via the rpc endpoint.

https://postgrest.org/en/v12/references/api/functions.html

Create a new SQL type and a query which returns it, then create a stored procedure which returns the query result.

Example stored function SQL in:
".\postgrest_stored_procedure.sql"

## Visualise the data using Javascript, Grafana or Python
Use the PostGREST API to access the data and visualise it using Javascript, Grafana or Python.

Built in grafana visualisation is available at port 3008, the database connection is provided.

Javascript examples can be found in application_examples.md

Example Python script in:
".\python_postgrest_visualise.py"
