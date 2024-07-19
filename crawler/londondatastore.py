# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Consumption data of various households in london gathered from 2011 to 2014.
A sub-set of 1,100 customers (Dynamic Time of Use or dToU) were given specific times when their electricity tariff would be higher or lower price than normal
High (67.20p/kWh), Low (3.99p/kWh) or normal (11.76p/kWh).
The rest of the sample (around 4,500) were on a flat rate of 14.228p/kWh.
https://data.london.gov.uk/blog/electricity-consumption-in-a-sample-of-london-households/
"""

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from .config import db_uri

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

LONDON_FULL_URL = "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/3527bf39-d93e-4071-8451-df2ade1ea4f2/LCL-FullData.zip"
LONDON_PARTITIONED_URL = "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/04feba67-f1a3-4563-98d0-f3071e3d56d1/Partitioned%20LCL%20Data.zip"


def main(db_uri):
    engine = create_engine(db_uri)
    log.info("Download london smartmeter energy dataset")
    response = requests.get(LONDON_PARTITIONED_URL)
    log.info("Write dataset to database")
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        # should be single file only if full_data
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                df = pd.read_csv(
                    thefile, parse_dates=["DateTime"], index_col="DateTime"
                )

                df.columns = [col.strip() for col in df.columns]
                df.rename(
                    columns={"KWH/hh (per half hour)": "power", "stdorToU": "tariff"},
                    inplace=True,
                )
                with engine.begin() as conn:
                    df.to_sql("consumption", conn, if_exists="append")
    log.info("Finished writing london smartmeter energy dataset to Database")

    try:
        query = text(
            "select public.create_hypertable('londondatastore.consumption', 'Time', if_not_exists => TRUE, migrate_data => TRUE)"
        )
        with engine.begin() as conn:
            conn.execute(query)
        log.error("successfully created hypertable for londondatastore")
    except Exception:
        log.error("could not create hypertable for londondatastore")


if __name__ == "__main__":
    logging.basicConfig()
    main(db_uri("londondatastore"))
