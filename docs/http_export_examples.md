# Exporting Data from the Open Energy Data Server using HTTP via the PostgREST API
Dataset:
Londondatastore.
Average data from london smartmeter measurements.

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