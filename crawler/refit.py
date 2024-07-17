# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Data from REFIT paper
https://www.nature.com/articles/sdata2016122

REFIT (An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study) 

This dataset is typically used for NILM applications (non-intrusive load monitoring).
"""
import io
import logging

import pandas as pd
import py7zr
import requests
from sqlalchemy import create_engine, text

from common.config import db_uri
from common.base_crawler import create_schema_only, set_metadata_only

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "refit",
    "data_date": "2024-06-12",
    "data_source": "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z",
    "license": "CC BY 4.0",
    "description": "University of Strathclyde household energy usage. Time-stamped data on various household appliances' energy consumption, detailing usage patterns across different homes.",
    "contact": "",
    "temporal_start": "2013-10-09 13:06:17",
    "temporal_end": "2015-07-10 11:56:32",
    "concave_hull_geometry": None,
}


REFIT_URL = (
    "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z"
)


def main(schema_name):
    log.info("Download refit dataset")
    response = requests.get(REFIT_URL)
    log.info("Write refit to database")
    engine = create_engine(db_uri(schema_name))
    with py7zr.SevenZipFile(io.BytesIO(response.content), mode="r") as z:
        names = z.getnames()
        # files = z.readall()
        for name in names:
            file = z.read([name])[name]
            df = pd.read_csv(file, index_col="Time", parse_dates=["Time"])
            del df["Unix"]
            df["house"] = name
            log.info(f"writing {name}")

            with engine.begin() as conn:
                df.to_sql("refit", conn, if_exists="append")
    log.info("Finished writing REFIT to Database")

    try:
        query = text(
            "select public.create_hypertable('refit.refit', 'Time', if_not_exists => TRUE, migrate_data => TRUE)"
        )
        with engine.begin() as conn:
            conn.execute(query)
        log.error("successfully created hypertable for REFIT")
    except Exception:
        log.error("could not create hypertable for REFIT")


if __name__ == "__main__":
    logging.basicConfig()
    main("refit")
