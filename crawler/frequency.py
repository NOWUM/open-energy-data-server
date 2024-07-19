# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import text

from common.base_crawler import BaseCrawler
from common.config import db_uri

log = logging.getLogger("frequency")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "frequency",
    "data_date": "2019-09-01",
    "data_source": "https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip",
    "license": "usage allowed",
    "description": """Electricity net frequency for germany. Time indexed.
No license given, usage is desirable but without any liability: https://www.50hertz.com/Transparenz/Kennzahlen
""",
    "contact": "",
    "temporal_start": "2011-01-01 00:00:00",
    "temporal_end": "2019-09-01 00:00:00",
    "concave_hull_geometry": None,
}

def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile, len(thezip.infolist())


class FrequencyCrawler(BaseCrawler):
    def __init__(self, schema_name):
        super().__init__(schema_name)


    def crawl_year_by_url(self, url):
        for name, thefile, count in download_extract_zip(url):
            log.info(name)
            if count == 1:  # only 2010
                df = pd.read_csv(
                    thefile,
                    sep=";",
                    decimal=",",
                    header=None,
                    names=["date_time", "frequency"],
                    # index_col='date',
                    # parse_dates=['date_time']
                )
                df.index = pd.to_datetime(df["date_time"], format="%d.%m.%Y %H:%M:%S")

                del df["date_time"]
            else:
                df = pd.read_csv(
                    thefile,
                    sep=",",
                    header=None,
                    parse_dates=[[0, 1]],
                )
                if len(df.columns) == 3:
                    del df[2]
                df.columns = ["date_time", "frequency"]
                df.set_index("date_time")
            try:
                with self.engine.begin() as conn:
                    df.to_sql("frequency", conn, if_exists="append")
            except Exception as e:
                log.error(f"Error: {e}")

    def crawl_frequency(self, first=2011, last=2020):
        for year in range(first, last + 1):
            log.info(year)
            url = f"https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip"
            self.crawl_year_by_url(url)

    def create_hypertable(self):
        try:
            with self.engine.begin() as conn:
                query = text("SELECT public.create_hypertable('frequency', 'date_time', if_not_exists => TRUE, migrate_data => TRUE);")
                conn.execute(query)
            log.info("created hypertable frequency")
        except Exception as e:
            log.error(f"could not create hypertable: {e}")


def main(db_uri):
    fc = FrequencyCrawler(db_uri)
    fc.crawl_frequency(first=2014)
    fc.create_hypertable()


if __name__ == "__main__":
    logging.basicConfig()
    if False:
        year = 2010
        thefile = "Netzfrequenz 2019/201901_Frequenz.csv"
        thefile = "Netzfrequenz 2011/201101_Frequenz.txt"
        thefile = "Netzfrequenz 2010/Frequenz2010.csv"
        # try parsing 2010 csv files
        conn = "sqlite://freq.db"
        fc = FrequencyCrawler(conn)
        fc.crawl_frequency(first=2014)

        import matplotlib.pyplot as plt

        sql = "select date_time, frequency from frequency where date_time>2019-01-01"
        df = pd.read_sql(sql, conn)
        plt.plot(df["date_time"], df["frequency"])

    conn_uri = db_uri("frequency")
    log.info(f"connect to {conn_uri}")
    fc = FrequencyCrawler("frequency")
    # fc.crawl_frequency(first=2014)
    year = 2015
    url = f"https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip"
    fc.crawl_year_by_url(url)

