# SPDX-FileCopyrightText: Marvin Lorber
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the data of the GIE transparency platform. (https://agsi.gie.eu/, https://alsi.gie.eu/)
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.

Licence Information from Websire:
Data usage
    It is mandatory to credit or mention to GIE (Gas Infrastructure Europe), AGSI or ALSI as data source when using or repackaging this data. 
Contact
    For data inquiries, please contact us via transparency@gie.eu

API and data documentation: https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf

This crawler uses the roiti-gie client: https://github.com/ROITI-Ltd/roiti-gie
"""

import logging
import pandas as pd
import asyncio
import os
import time

from gie import GiePandasClient
from sqlalchemy import create_engine, text
from crawler.config import db_uri
from datetime import date, datetime, timedelta

log = logging.getLogger("gie")
log.setLevel(logging.INFO)
# silence roiti logger
logging.getLogger("GiePandasClient").setLevel(logging.WARNING)

default_start_date = date(2012, 1, 1)
data_hierachy = ["country", "company", "location"]


async def async_main(db_uri):
    engine = create_engine(db_uri)
    API_KEY = os.getenv("GIE_API_KEY")
    if not API_KEY:
        raise Exception("GIE_API_KEY is not defined")
    try:
        pandas_client = GiePandasClient(api_key=API_KEY)
        first_date = select_latest(engine)
        last_date = date.today() - timedelta(days=1)
        log.info(f"fetching from {first_date} until {last_date}")
        api_call_count = 0
        for fetch_date in pd.date_range(first_date, last_date):
            log.info(f"Handling {fetch_date}")
            api_call_count += 1
            if api_call_count > 30:
                # The api limits clients to 60 Requests per second
                # So we have to make sure to stay below that
                time.sleep(1)
                api_call_count = 0
            await collect_Date(
                datetime.strftime(fetch_date, "%Y-%m-%d"), pandas_client, engine
            )
        await pandas_client.close_session()
    except Exception as e:
        log.error(e)
        await pandas_client.close_session()

    create_hypertable(engine)


async def collect_Date(date, pandas_client: GiePandasClient, engine):
    df_agsi_europe = await pandas_client.query_country_agsi_storage(date=date)
    df_alsi_europe = await pandas_client.query_country_alsi_storage(date=date)

    with engine.begin() as conn:
        recursiveWrite(df_agsi_europe, "agsi", conn, pandas_client, 0)
        recursiveWrite(df_alsi_europe, "alsi", conn, pandas_client, 0)


def select_latest(engine):
    day = datetime.strftime(default_start_date, "%Y-%m-%d")
    today = datetime.strftime(date.today(), "%Y-%m-%d")
    sql = f"SELECT gie.gasdaystart FROM gie_agsi_country AS gie ORDER BY gie.gasdaystart DESC LIMIT 1"
    try:
        with engine.begin() as conn:
            return pd.read_sql(sql, conn, parse_dates=["datetime"]).values[0][0]
    except Exception as e:
        log.error(
            f"Could not read start date - using default: {default_start_date} - {e}"
        )
        return default_start_date


def extract(df, client: GiePandasClient):
    result = [0] * len(df)
    for i in range(len(df)):
        result[i] = client._pandas_df_format(
            df.loc[i, "children"], client._FLOATING_COLS, client._DATE_COLS
        )
        result[i] = result[i].assign(parent=df.loc[i, "name"])
    return result


def recursiveWrite(
    df, data_identifier: str, conn, client: GiePandasClient, level: int = 0
):
    df_children = extract(df, client)

    for df_child in df_children:
        if level < 2:
            recursiveWrite(df_child, data_identifier, conn, client, level + 1)
            df_child.drop(columns="children", inplace=True)
        # rename columns to lowercase titles
        df_child.rename(mapper=str.lower, axis="columns", inplace=True)

        df_child.to_sql(
            f"gie_{data_identifier}_{data_hierachy[level]}",
            conn,
            if_exists="append",
        )


def create_hypertable(engine):
    try:
        with engine.begin() as conn:
            for tablename in [
                "gie_agsi_country",
                "gie_agsi_company",
                "gie_agsi_location",
                "gie_alsi_country",
                "gie_alsi_company",
                "gie_alsi_location",
            ]:
                query_create_hypertable = f"SELECT public.create_hypertable('{tablename}', 'gasDayStart', if_not_exists => TRUE, migrate_data => TRUE);"
                conn.execute(text(query_create_hypertable))
        log.info(f"created hypertables for gie")
    except Exception as e:
        log.error(f"could not create hypertable: {e}")


def main(db_uri_str):
    asyncio.run(async_main(db_uri(db_uri_str)))


if __name__ == "__main__":
    main("gie")
