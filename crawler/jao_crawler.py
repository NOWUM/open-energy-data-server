# SPDX-FileCopyrightText: Simon Hesselmann, Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Downloads latest data from the Joint Allocation Office (JAO).

Currently, this only includes market results and their bids.
Data from the https://publicationtool.jao.eu/ is not yet included.

Good analysis of this data is included in https://boerman.dev/ and
https://data.boerman.dev/d/5CYxW2JVz/flows-scheduled-commercial-exchanges-day-ahead
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from jao import JaoAPIClient

# pip install git+https://github.com/maurerle/jao-py@improve_horizon_support
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.exc import OperationalError

from crawler.config import db_uri

log = logging.getLogger("jao")

MIN_WEEKLY_DATE = datetime(2023, 1, 1)

DELTAS = {
    "seasonal": relativedelta(years=1),
    "yearly": relativedelta(years=1),
    "monthly": relativedelta(month=1),
    "weekly": relativedelta(weeks=1, weekday=0),
    "daily": relativedelta(days=1),
    "intraday": relativedelta(days=1),
}


def string_to_timestamp(*dates):
    timestamps = []
    date_formats = [
        "%Y-%m-%d-%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    ]
    for date_string in dates:
        if isinstance(date_string, str):
            timestamp = None

            for date_format in date_formats:
                try:
                    timestamp = datetime.strptime(date_string, date_format)
                    timestamps.append(timestamp)
                    break
                except ValueError:
                    pass

            if timestamp is None:
                timestamps.append(None)
        else:
            timestamps.append(date_string)

    return timestamps if len(timestamps) > 1 else timestamps[0]


class DatabaseManager:
    def __init__(self, db_uri: str):
        self.engine = create_engine(db_uri)

    def execute(self, query: text):
        with self.engine.begin() as connection:
            result = connection.execute(query)
        return result

    def get_tables(self):
        metadata = MetaData()
        metadata.reflect(bind=self.engine)
        return metadata.tables.keys()

    def table_exists(self, table_name: str):
        return table_name in self.get_tables()

    def get_count(self, table_name: str):
        count_query = text(f"SELECT date FROM {table_name} limit 1")
        return self.execute(count_query).scalar()

    def get_min(self, table_name: str, column_name: str):
        min_date_query = text(f"SELECT MIN({column_name}) FROM {table_name}")
        return self.execute(min_date_query).scalar()

    def get_max(self, table_name, column_name):
        max_date_query = text(f"SELECT MAX({column_name}) FROM {table_name}")
        return self.execute(max_date_query).scalar()

    def create_hypertables(self):
        for table_name in self.get_tables():
            try:
                query_create_hypertable = f"SELECT public.create_hypertable('{table_name}', 'date', if_not_exists => TRUE, migrate_data => TRUE);"
                self.execute(text(query_create_hypertable))
                log.info(f"created hypertable {table_name}")
            except Exception as e:
                log.error(f"could not create hypertable: {e}")


class JaoClientWrapper:
    def __init__(self, api_key):
        self.client = JaoAPIClient(api_key)

    def get_bids(self, auction_id: str):
        try:
            return self.client.query_auction_bids_by_id(auction_id)
        except requests.exceptions.HTTPError as e:
            log.error(f"Error fetching bids for auction {auction_id}: {e}")
            return pd.DataFrame()

    def get_auctions(
        self, corridor: str, from_date: datetime, to_date: datetime, horizon="Monthly"
    ) -> pd.DataFrame:
        from_date, to_date = string_to_timestamp(from_date, to_date)
        try:
            return self.client.query_auction_stats(
                from_date, to_date, corridor, horizon
            )
        except requests.exceptions.HTTPError as e:
            if e.response.status_code != 400:
                log.error(
                    f"Error fetching auctions for corridor {corridor} from {from_date} to {to_date}: {e}"
                )
            return pd.DataFrame()

    def get_horizons(self):
        return self.client.query_auction_horizons()

    def get_corridors(self):
        return self.client.query_auction_corridors()


def calculate_min_max(db_manager, corridor, horizon="Yearly"):
    table_name = "auctions"
    try:
        query = text(
            f"SELECT MIN(date), MAX(date) FROM auctions where corridor='{corridor}' and horizon='{horizon}'"
        )
        scalar = db_manager.execute(query).first()
        min_date = scalar[0]
        max_date = scalar[1]
        return string_to_timestamp(min_date), string_to_timestamp(max_date)
    except Exception as e:
        log.error(f"error crawling {e}")
        log.info(
            f"The table '{table_name}' did not exist or was empty. Crawling whole interval"
        )
        return None, None


def crawl_single_horizon(
    jao_client,
    db_manager,
    from_date,
    to_date,
    corridor,
    horizon,
):
    table_name = f"bids_{horizon.lower()}"
    table_name = table_name.replace("-", "_").replace(" ", "_")

    try:
        auctions_data = jao_client.get_auctions(corridor, from_date, to_date, horizon)
    except Exception as e:
        log.error(f"Did not get Auctions for {corridor} - {horizon}: {e}")
        return
    if auctions_data.empty:
        return

    log.info(
        f"started crawling bids of {corridor} - {horizon} for {len(auctions_data)} auctions"
    )
    auctions_data["horizon"] = horizon

    try:
        with db_manager.engine.begin() as connection:
            auctions_data.to_sql(
                "auctions",
                connection,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=10_000,
            )
    except OperationalError:
        log.exception(
            f"database error writing {len(auctions_data)} entries - trying again"
        )
        import time

        time.sleep(5)
        with db_manager.engine.begin() as connection:
            auctions_data.to_sql(
                "auctions", connection, if_exists="append", index=False
            )

    for auction_id, auction_date in auctions_data.loc[:, ["id", "date"]].values:
        bids_data = jao_client.get_bids(auction_id)

        if bids_data.empty:
            continue

        bids_data["auctionId"] = auction_id
        bids_data["date"] = auction_date
        try:
            with db_manager.engine.begin() as connection:
                bids_data.to_sql(
                    table_name,
                    connection,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=10_000,
                )
        except OperationalError:
            log.error(f"database error writing {len(bids_data)} entries - trying again")
            import time

            time.sleep(5)
            with db_manager.engine.begin() as connection:
                bids_data.to_sql(
                    table_name, connection, if_exists="append", index=False
                )


def run_data_crawling(
    jao_client: JaoClientWrapper,
    from_date: datetime,
    to_date: datetime,
    db_manager: DatabaseManager,
):
    log.info(f"starting run_data_crawling from {from_date} to {to_date}")
    for horizon in jao_client.get_horizons():
        for corridor in jao_client.get_corridors():
            if "intraday" == horizon.lower():
                continue

            if horizon.lower() == "weekly":
                from_date = max(MIN_WEEKLY_DATE, from_date)

            first_date, last_date = calculate_min_max(db_manager, corridor, horizon)
            log.info(f"crawl {horizon}, {corridor} - {from_date} - {to_date}")
            if not first_date:
                first_date = to_date
            if from_date < first_date:
                log.info(f"crawling before {from_date} until {first_date}")
                crawl_single_horizon(
                    jao_client, db_manager, from_date, first_date, corridor, horizon
                )
            delta = DELTAS.get(horizon.lower(), timedelta(days=1))
            if last_date and to_date - delta > last_date:
                # must be at least one horizon ahead, otherwise we are crawling duplicates
                log.info(f"crawling before {last_date} until {to_date}")
                crawl_single_horizon(
                    jao_client, db_manager, last_date, to_date, corridor, horizon
                )
            log.info(f"finished crawling bids of {corridor} - {horizon}")
    log.info(f"finished run_data_crawling from {from_date} to {to_date}")


def main(connection_string, from_date_string="2023-01-01-00:00:00"):
    db_manager = DatabaseManager(connection_string)
    jao_client = JaoClientWrapper("1ba7533c-e5d1-4fc1-8c28-cf51d77c91f6")

    now = datetime.now()

    from_date = string_to_timestamp(from_date_string)
    # not sure if this is exclusive, so substract a ms to be safe
    to_date = datetime(now.year, now.month, 1) - timedelta(microseconds=1)

    run_data_crawling(jao_client, from_date, to_date, db_manager)
    db_manager.create_hypertables()


if __name__ == "__main__":
    logging.basicConfig(level="INFO")
    main(connection_string=db_uri("jao"), from_date_string="2019-01-01-00:00:00")
