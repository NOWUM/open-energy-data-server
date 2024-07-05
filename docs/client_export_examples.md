# Exporting Data from the Open Energy Data Server using an Application
Dataset:
Londondatastore.
Average data from london smartmeter measurements.

## PSQL command line

```
echo >londondatastore.csv
psql postgresql://opendata:opendata@localhost:6432  
\copy (SELECT * FROM londondatastore.consumption LIMIT 20) TO './londondatastore.csv' WITH CSV HEADER; 
```

## SQL using Python

```python
import pandas as pd
from sqlalchemy import create_engine

//Remote connection
//engine = create_engine('postgresql://timescale.nowum.fh-aachen.de:5432/opendata?search_path=londondatastore')
//Remote default local configuration
engine = create_engine('postgresql://localhost:6432/opendata?search_path=londondatastore')
query = "SELECT ""DateTime"" AS hourly_timestamp,  AVG(""power"") AS average_power FROM consumption WHERE DateTime >= '2012-01-01' AND DateTime <= '2013-01-01' LIMIT 10"

with engine.connect() as conn:
    df = pd.read_sql(query, conn, parse_dates="DateTime", index_col="DateTime")
df = df.resample("1h").mean()
df.to_csv("londondatastore_pgrst.csv")
```

## Grafana

Using the Graph from this dashboard: 
http://localhost:3006/d/edn5t9gi0wyrke/refit-load-profile?orgId=1
https://monitor.nowum.fh-aachen.de/d/edn5t9gi0wyrke/refit-load-profile?orgId=1

One can inspect to see the data (or the query) of the graph easily as shown below:

Default configuration is locally available at:
http://localhost:3006/

Username: opendata
Password: opendata

Pgadmin is provisioned using information from data/provisioning/grafana
For database connection /datasources/datasource.yml 
For example dashboards /datasources/dashboardproviders & /datasources/dashboards


![Grafana Export](./media/grafana_export.png)


## PgAdmin4

PgAdmin is quite powerful and therefore not publically available, though you can use your instance on the open-energy-data-server for all of your institute for quick analysis.
It also lets you explain a query or create Indexes or Alter tables interactively.
pgAdmin can also draw ER-Diagrams from a schema or table in your database

Default configuration is locally available at:
http://localhost:8080/

Username: admin@admin.admin
Password: admin

Pgadmin is provisioned using information from data/provisioning/pgadmin/servers.json

![pgAdmin Export](./media/pgadmin_export.png)
