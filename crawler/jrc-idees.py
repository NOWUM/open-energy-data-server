# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
The Joint Research Centre's Integrated Database of the European Energy System (JRC-IDEES) compiles a rich set of information allowing for highly granular analyses of the dynamics of the European energy system, so as to better understand the past and create a robust basis for future policy assessments.
https://data.jrc.ec.europa.eu/dataset/82322924-506a-4c9a-8532-2bdd30d69bf5
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

JRC_IDEES_URL = "https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/JRC-IDEES/JRC-IDEES-2021_v1/JRC-IDEES-2021.zip"


def main(db_uri):
    engine = create_engine(db_uri)
    log.info("Download JRC-IDEES dataset")
    response = requests.get(JRC_IDEES_URL)
    log.info("Write JRC-IDEES dataset to database")
    table_names = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            try:
                with thezip.open(zipinfo) as thefile:
                    # zipinfo = name?
                    if 'xls' not in thefile.name:
                        continue
                    xl = pd.ExcelFile(thefile)
                    for sheet_name in xl.sheet_names:
                        if sheet_name in ["index", "cover"]:
                            continue
                        table_names.append(sheet_name)
                        df = xl.parse(sheet_name, index_col=[0, 1])
                        df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
                        df = df[~df.index.duplicated(keep='first')]
                        df = df.T
                        df.index = pd.to_datetime(df.index, format="%Y")
                        df.index.name = "year"
                        df["zone"] = thefile.name.split(".")[0].split("_")[-1]
                        with engine.begin() as conn:
                            df.to_sql(sheet_name, conn, if_exists="append")
            except Exception as e:
                log.error(f"Error: {e} - {zipinfo}")

            break
    log.info("Finished writing JRC-IDEES dataset to Database")

    try:
        for table in table_names:
            query = text(
                f"select public.create_hypertable('jrc_idees.{table}', 'year', if_not_exists => TRUE, migrate_data => TRUE)"
            )
            with engine.begin() as conn:
                conn.execute(query)
        log.error("successfully created hypertable for jrc_idees")
    except Exception:
        log.error("could not create hypertable for jrc_idees")


if __name__ == "__main__":
    logging.basicConfig()
    main(db_uri("jrc_idees"))
