# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import logging
import zipfile
import io

import pandas as pd
import requests
import geopandas
from sqlalchemy import create_engine, text
from .config import db_uri



log = logging.getLogger("iwu")
log.setLevel(logging.INFO)

class DataCrawler:
    def __init__(self, db_uri):
        self.engine = create_engine(db_uri)
        pass

    def pullData(self):
        url = "https://www.opengeodata.nrw.de/produkte/umwelt_klima/klima/kwp/KWP-NRW-Waermebedarf_EPSG25832_Geodatabase.zip"
        response = requests.get(url)
        if response.status_code == 200:
            z = zipfile.ZipFile(io.BytesIO(response.content))
            logging.log(logging.INFO, "Downloaded the KWP NRW ZIP file")

            if not os.path.exists(
                os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw"
            ):
                z.extractall(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw")
                logging.log(logging.INFO, "Extracted KWP NRW GDB")
            else:
                logging.log(logging.INFO, "KWP NRW GDB already exists")

            return True
        else:
            log.info("Failed to download the ZIP file")
            return False

    def save_to_database(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "DROP TABLE IF EXISTS public.waermedichte CASCADE; DROP SCHEMA IF EXISTS nrw_kwp_waermedichte CASCADE;"
                )
            )
        start_i = 0
        end_i = 1000
        while end_i < 12710309:
            data = geopandas.read_file(
                os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw\Waermebedarf_NRW.gdb",
                rows=slice(start_i, end_i, None),
            )

            start_i = end_i
            end_i += 1000
            if end_i > 12710308:
                end_i = 12710308
            with self.engine.begin() as conn:
                data.to_postgis("waermedichte", conn, if_exists="append")
        
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE SCHEMA IF NOT EXISTS nrw_kwp_waermedichte; ALTER TABLE public.waermedichte SET SCHEMA nrw_kwp_waermedichte;"
                )
            )

    def clean(self):
        file_list = os.listdir(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw\Waermebedarf_NRW.gdb")
        for file_name in file_list:
            file_path = os.path.join(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw\Waermebedarf_NRW.gdb", file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
        os.rmdir(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw\Waermebedarf_NRW.gdb")

        file_list = os.listdir(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw")
        for file_name in file_list:
            file_path = os.path.join(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw", file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)

        os.rmdir(os.path.join(os.path.dirname(__file__)) + "\data\kwp_nrw")


def main(db_uri):
    crawler = DataCrawler(db_uri)
    if crawler.pullData():
        crawler.save_to_database()
        crawler.clean()
        pass


if __name__ == "__main__":
    main(db_uri("nrw_kwp_waermedichte"))
