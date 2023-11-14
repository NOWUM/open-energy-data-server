import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from jao import JaoAPIClient
from sqlalchemy import MetaData, create_engine, text

from crawler.config import db_uri

log = logging.getLogger("jao")
client = JaoAPIClient("1ba7533c-e5d1-4fc1-8c28-cf51d77c91f6")


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
        count_query = text(f"SELECT COUNT(*) FROM {table_name}")
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
                query_create_hypertable = f"SELECT create_hypertable('{table_name}', 'month', if_not_exists => TRUE, migrate_data => TRUE);"
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
            return self.client.query_auction_stats_months(
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


def run_data_crawling(
    jao_client: JaoClientWrapper,
    from_date: datetime,
    to_date: datetime,
    db_manager: DatabaseManager,
):
    for corridor in jao_client.get_corridors():
        for horizon in jao_client.get_horizons():
            table_name = f"bids_{horizon.lower()}"
            table_name = table_name.replace("-", "_").replace(" ", "_")

            auctions_data = jao_client.get_auctions(
                corridor, from_date, to_date, horizon
            )

            if auctions_data.empty:
                continue

            with db_manager.engine.begin() as connection:
                auctions_data.to_sql(
                    "auctions", connection, if_exists="append", index=False
                )

            for auction_id, auction_month in auctions_data.loc[
                :, ["id", "month"]
            ].values:
                bids_data = jao_client.get_bids(auction_id)

                if not bids_data.empty:
                    bids_data["auctionId"] = auction_id
                    bids_data["month"] = auction_month
                    with db_manager.engine.begin() as connection:
                        bids_data.to_sql(
                            table_name, connection, if_exists="append", index=False
                        )


def main(connection_string, from_date_string="2019-01-01-00:00:00"):
    db_manager = DatabaseManager(connection_string)
    jao_client = JaoClientWrapper("1ba7533c-e5d1-4fc1-8c28-cf51d77c91f6")

    now = datetime.now()

    from_date = string_to_timestamp(from_date_string)
    # not sure if this is exclusive, so substract a ms to be safe
    to_date = datetime(now.year, now.month, 1) - timedelta(microseconds=1)

    table_name = "auctions"

    if db_manager.table_exists(table_name) and db_manager.get_count(table_name) > 0:
        first_date = string_to_timestamp(db_manager.get_min(table_name, "month"))
        last_date = string_to_timestamp(db_manager.get_max(table_name, "month"))

        if from_date < first_date:
            run_data_crawling(jao_client, from_date, first_date, db_manager)
        if to_date > last_date:
            run_data_crawling(jao_client, last_date, to_date, db_manager)
    else:
        log.info(f"The table '{table_name}' did not exist or was empty.")
        run_data_crawling(jao_client, from_date, to_date, db_manager)

    db_manager.create_hypertables()


if __name__ == "__main__":
    logging.basicConfig()
    main(connection_string=db_uri("jao"), from_date_string="2019-01-01-00:00:00")
