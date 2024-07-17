# SPDX-FileCopyrightText: Florian Maurer, Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import re
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from sqlalchemy import  text
from common.base_crawler import BaseCrawler
from common.config import db_uri


log = logging.getLogger("eview")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "eview",
    "data_date": "2024-06-12",
    "data_source": "http://www.eview.de/solarstromdaten/login.php",
    "license": "© Eview",
    "description": "Eview solar energy. Plan specific time indexed performance.",
    "contact": "",
    "temporal_start": "2009-01-02 08:15:00",
    "temporal_end": "2024-06-10 21:45:00",
    "concave_hull_geometry": None,
}

default_start_date = date(2022, 11, 1)
# using http instead of https to be faster


class EViewCrawler(BaseCrawler):
    def __init__(self, schema_name):
        super().__init__(schema_name)

    def get_solar_units(self):
        # crawl available pv units
        data = requests.get("http://www.eview.de/solarstromdaten/login.php")
        return re.findall("login\.php\?p=;(\w{2});", data.text)

    def crawl_unit_date(self, unit, fetch_date):
        log.info(f"fetching {fetch_date} for {unit}")

        day = datetime.strftime(fetch_date, "%d.%m.%Y")
        url = f"http://www.eview.de/solarstromdaten/export.php?p=;{unit};z;dg1;f0;t{day}/1;km250"
        try:
            df = pd.read_csv(
                url,
                decimal=",",
                index_col=0,
                encoding="iso-8859-1",
                skiprows=4,
                parse_dates=True,
                dayfirst=True,
            )
            log.info(f"num records {df.size}")
        except Exception:
            log.info("not data")
            return
        if df.size < 2:
            log.info("not data")
            return
        ddf = df.unstack()
        ddf = ddf.reset_index()
        ddf.index = ddf["Datum und Uhrzeit"]
        ddf.index.name = "datetime"
        del ddf["Datum und Uhrzeit"]
        ddf.columns = ["plant", "value"]
        ddf["plant_id"] = unit
        with self.engine.begin() as conn:
            ddf.to_sql("eview", con=conn, if_exists="append")

    def crawl_unit(self, unit, begin_date):
        first_date = pd.to_datetime(begin_date) + timedelta(days=1)
        last_date = date.today() - timedelta(days=1)
        log.info(f"fetching from {first_date} until {last_date}")
        for fetch_date in pd.date_range(first_date, last_date):
            self.crawl_unit_date(unit, fetch_date)

    def select_latest(self, unit):
        day = datetime.strftime(default_start_date, "%Y-%m-%d")
        today = datetime.strftime(date.today(), "%Y-%m-%d")
        sql = f"select datetime from eview where plant_id='{unit}' and datetime > '{day}' and datetime < '{today}' order by datetime desc limit 1"
        try:
            with self.engine.begin() as conn:
                return pd.read_sql(sql, conn, parse_dates=["datetime"]).values[0][0]
        except Exception as e:
            log.error(e)
            return default_start_date

    def create_hypertable(self):
        try:
            query_create_hypertable =text("SELECT public.create_hypertable('eview', 'datetime', if_not_exists => TRUE, migrate_data => TRUE);")
            with self.engine.begin() as conn:
                conn.execute(query_create_hypertable)
            log.info("created hypertable eview")
        except Exception as e:
            log.error(f"could not create hypertable: {e}")


def main(schema_name):
    ec = EViewCrawler(schema_name)
    solar_plants = ec.get_solar_units()

    for plant in solar_plants:
        log.info(f"fetching {plant}")
        try:
            begin_date = ec.select_latest(plant)
            ec.crawl_unit(plant, begin_date)
        except Exception:
            log.exception(f"Error with {plant}")

    ec.create_hypertable()
    ec.set_metadata(metadata_info)

if __name__ == "__main__":
    logging.basicConfig()

    # db_conn = 'sqlite:///./data/eview.db'
    db_conn = db_uri("eview")
    log.info(f"connect to {db_conn}")
    ec = EViewCrawler(db_conn)
    plant = "FI"
    begin_date = ec.select_latest(plant)
    ec.crawl_unit(plant, begin_date)
    ec.create_hypertable()

#    main(db_uri)
