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

from .config import db_uri

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

REFIT_URL = (
    "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z"
)


def main(db_uri):
    log.info("Download refit dataset")
    response = requests.get(REFIT_URL)
    log.info("Write refit to database")
    engine = create_engine(db_uri)
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
    main(db_uri("refit"))
