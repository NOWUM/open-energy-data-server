# SPDX-FileCopyrightText: Vassily Aliseyko, Florian Maurer, Christian Rieke

# SPDX-License-Identifier: AGPL-3.0-or-later

from io import StringIO
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine
from config_example import db_uri

log = logging.getLogger("opsd")
log.setLevel(logging.INFO)
logging.basicConfig()

national_generation_capacity_url = (
    "https://data.open-power-system-data.org/national_generation_capacity/2020-10-01/national_generation_capacity_stacked.csv"
)

def national_generation_capacity(engine):
    log.info("Fetching data from %s", national_generation_capacity_url)
    response = requests.get(national_generation_capacity_url)
    response.raise_for_status()  

    log.info("Loading data into DataFrame")
    data = pd.read_csv(StringIO(response.text))

    log.info("Writing data to the database")
    data.to_sql("national_generation_capacity", engine, if_exists="replace",)
    log.info("Data written successfully")

def main(db_uri):
    engine = create_engine(db_uri)
    
    national_generation_capacity(engine)

if __name__ == "__main__":
    
    main(db_uri("opsd"))
