# Exporting Data from the Open Energy Data Server using HTTP via the PostgREST API
Dataset:
Londondatastore.
Average data from london smartmeter measurements.

## SQL using Python

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://timescale.nowum.fh-aachen.de:5432/opendata?search_path=londondatastore')
query = "SELECT ""DateTime"" AS hourly_timestamp,  AVG(""power"") AS average_power FROM consumption WHERE DateTime >= '2012-01-01' AND DateTime <= '2013-01-01' LIMIT 10"

with engine.connect() as conn:
    df = pd.read_sql(query, conn, parse_dates="DateTime", index_col="DateTime")
df = df.resample("1h").mean()
df.to_csv("londondatastore_pgrst.csv")
```

## HTTP using Python requests

```python
import requests
import pandas as pd

url = "https://monitor.nowum.fh-aachen.de/oeds/consumption"
headers = {
    "Accept": "application/json",
    "Accept-Profile": "londondatastore"
}
params = {
    "limit": 10,
    "DateTime": "gte.'2012-01-01'",
    "DateTime": "lte.'2013-01-01'",
    "select": "DateTime,power.sum()",
}

response = requests.get(url, headers=headers, params=params)
df = pd.DataFrame.from_records(response.json(), index="DateTime")
df.index = pd.to_datetime(df.index)
df = df.resample("1h").mean()
df.to_csv("londondatastore_pgrst.csv")
```