# SPDX-FileCopyrightText: Florian Maurer, Jonathan Sejdija
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the generation data of germany from the smard portal of the Bundesnetzagentur at smard.de.
It contains mostly data for Germany which is also availble in the ENTSO-E transparency platform but under a CC open license.
"""

import json
import logging
from datetime import timedelta

import pandas as pd
import requests
from sqlalchemy import text

from common.base_crawler import BaseCrawler

log = logging.getLogger("smard")
default_start_date = "2023-01-01 22:45:00"  # "2023-11-26 22:45:00"


metadata_info = {
    "schema_name": "smard",
    "data_date": "2024-06-12",
    "data_source": "https://www.smard.de/",
    "license": "CC-BY-4.0",
    "description": "Open access ENTSOE  Germany. Production of energy by good and timestamp",
    "contact": "",
    "temporal_start": "2023-01-01 23:00:00",
    "temporal_end": "2024-06-09 21:45:00",
    "concave_hull_geometry": None,
}


class SmardCrawler(BaseCrawler):
    def __init__(self, schema_name):
        super().__init__(schema_name)

    def create_table(self):
        try:
            query_create_hypertable = "SELECT public.create_hypertable('smard', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);"
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "CREATE TABLE IF NOT EXISTS smard( "
                        "timestamp timestamp without time zone NOT NULL, "
                        "commodity_id text, "
                        "commodity_name text, "
                        "mwh double precision, "
                        "PRIMARY KEY (timestamp , commodity_id));"
                    )
                )
                conn.execute(text(query_create_hypertable))
            log.info("created hypertable smard")
        except Exception as e:
            log.error(f"could not create hypertable: {e}")

    def get_data_per_commodity(self):
        keys = {
            # 411: 'Prognostizierter Stromverbrauch',
            410: "Realisierter Stromverbrauch",
            4066: "Biomasse",
            1226: "Wasserkraft",
            1225: "Wind Offshore",
            4067: "Wind Onshore",
            4068: "Photovoltaik",
            1228: "Sonstige Erneuerbare",
            1223: "Braunkohle",
            4071: "Erdgas",
            4070: "Pumpspeicher",
            1227: "Sonstige Konventionelle",
            4069: "Steinkohle",
            # 5097: 'Prognostizierte Erzeugung PV und Wind Day-Ahead'
        }

        for commodity_id, commodity_name in keys.items():
            start_date = self.select_latest(commodity_id) + timedelta(minutes=15)
            # start_date_tz to unix time
            start_date_unix = int(start_date.timestamp() * 1000)
            url = f"https://www.smard.de/app/chart_data/{commodity_id}/DE/{commodity_id}_DE_quarterhour_{start_date_unix}.json"
            log.info(url)
            response = requests.get(url)
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                log.error(f"Could not get data for commodity: {commodity_id} {e}")
                continue
            data = json.loads(response.text)
            timeseries = pd.DataFrame.from_dict(data["series"])
            if timeseries.empty:
                log.info(f"Received empty data for commodity: {commodity_id}")
                continue
            timeseries[0] = pd.to_datetime(timeseries[0], unit="ms", utc=True)
            timeseries.columns = ["timestamp", "mwh"]
            timeseries["commodity_id"] = commodity_id
            timeseries["commodity_name"] = commodity_name
            timeseries = timeseries.dropna(subset="mwh")

            yield timeseries

    def select_latest(
        self, commodity_id, delete=False, prev_latest=None
    ) -> pd.Timestamp:
        # day = default_start_date
        # today = date.today().strftime('%d.%m.%Y')
        # sql = f"select timestamp from smard where timestamp > '{day}' and timestamp < '{today}' order by timestamp desc limit 1"
        sql = f"select timestamp from smard where commodity_id='{commodity_id}' order by timestamp desc limit 1"
        try:
            with self.engine.begin() as conn:
                if delete:
                    print(prev_latest)
                    sql_delete = f"delete from smard where timestamp > '{prev_latest.replace(hour=22, minute=45, second=0)}' AND commodity_id='{commodity_id}'"
                    conn.execute(text(sql_delete))
                    log.info(f"Deleted data from {prev_latest} to now")
                latest = pd.read_sql(sql, conn, parse_dates=["timestamp"]).values[0][0]
            latest = pd.to_datetime(latest, unit="ns")
            log.info(f"The latest date in the database is {latest}")
            start_date_unix = int((latest + timedelta(minutes=15)).timestamp() * 1000)
            response = requests.get(
                f"https://www.smard.de/app/chart_data/{commodity_id}/DE/{commodity_id}_DE_quarterhour_{start_date_unix}.json"
            )
            try:
                response.raise_for_status()
            except requests.exceptions.HTTPError as e:
                log.error(
                    f"Could not get data for commodity, will retry: {commodity_id} {e}"
                )
                self.select_latest(
                    commodity_id,
                    delete=True,
                    prev_latest=latest - timedelta(days=1),
                )
            return latest
        except Exception as e:
            log.info(f"Using the default start date {e}")
            return pd.to_datetime(default_start_date)

    def feed(self):
        for data_for_commodity in self.get_data_per_commodity():
            if data_for_commodity.empty:
                continue
            df_for_commodity = data_for_commodity.set_index(
                ["timestamp", "commodity_id"]
            )
            # delete timezone duplicate
            # https://stackoverflow.com/a/34297689
            df_for_commodity = df_for_commodity[
                ~df_for_commodity.index.duplicated(keep="first")
            ]

            log.info(df_for_commodity)
            with self.engine.begin() as conn:
                df_for_commodity.to_sql("smard", con=conn, if_exists="append")


def main(schema_name):
    ec = SmardCrawler(schema_name)
    ec.create_table()
    ec.feed()
    ec.set_metadata(metadata_info)


if __name__ == "__main__":
    logging.basicConfig(filename="smard.log", encoding="utf-8", level=logging.INFO)
    # db_uri = 'sqlite:///./data/smard.db'
    main("smard")
