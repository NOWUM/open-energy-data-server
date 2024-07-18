# SPDX-FileCopyrightText: Marvin Lorber
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the data of the GIE transparency platform. (https://agsi.gie.eu/, https://alsi.gie.eu/)
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.

license Information from Websire:
Data usage
    It is mandatory to credit or mention to GIE (Gas Infrastructure Europe), AGSI or ALSI as data source when using or repackaging this data.
Contact
    For data inquiries, please contact us via transparency@gie.eu

API and data documentation: https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf

This crawler uses the roiti-gie client: https://github.com/ROITI-Ltd/roiti-gie
"""

import asyncio
import logging
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
from common.base_crawler import BaseCrawler
from roiti.gie import GiePandasClient
from sqlalchemy import create_engine, text

from common.config import db_uri

log = logging.getLogger("gie")
log.setLevel(logging.INFO)
# silence roiti logger
logging.getLogger("GiePandasClient").setLevel(logging.WARNING)

metadata_info = {
    "schema_name": "gie",
    "data_date": "2024-06-12",
    "data_source": "https://agsi.gie.eu/",
    "license": "https://www.gie.eu/privacy-policy/",
    "description": "Gas Inventory Transparency. Time and country indexed capacity and consumption of gas.",
    "contact": "",
    "temporal_start": "2012-01-01 00:00:00",
    "temporal_end": "2024-06-11 01:13:10",
    "concave_hull_geometry": "0103000020E610000001000000210000003E945E0A632DA0BFBD7361F239DF4840C6FE82603AC8DF3FA50EB38AECC04940F9121E6D62ACE33FE811BB3E06D14940E3ACA46D4E2D3340457268AF013D4D4094FD7970426C3540E815CABCFEA64D40265305A392EA37405F07CE1951024E40EE5A423EE8293B405F07CE1951424E40E71D7380E4953A40CD312570FDE04940A7597AECF00C3A40AE433C18F6AA49404C5C5F087B3F3840F68CEB3A58304840B88D8845C24F3A40E67FD90FACCD474005A3923A01DD3940B81E85EB516844404EB67CAF742739402114747D11D9434027A0D2FFB38C374034EBC00CA8EA4240CAB38D163C5937403CDB9C6215E34240835575F39C4514409FC7C3CA7E964540F01F3C45E544CE3F80B267F448E2D73F00000000000000000000000000000000EDE67AF6A3D7C0BF6097913E27FEE13FA1A52A60F257124011BF8F852E6B4540B8E8735FE155EABFFA92B59B4BB74240AEFBEE3C294BEEBFFB7BAF7E1DB042409BE51EBDA8941BC0B83DC7A4707D4240FCD042AB6AEA1BC0396796D81C7F4240CB9650B2D0A721C09CAB13378CDC4240FAEDEBC039C321C0992A1895D4F942402575029A087B20C0EC2FBB270FBB45405E967726AB5C1AC065A6E2DB21E34740C49A2270E27218C00262B9B323244840220A2C345F6C19C0AF7B3A74A831484050FC1873D75214C03F355EBA49DC494052EDE71974FB12C03BE7D6E491E849403E945E0A632DA0BFBD7361F239DF4840",
}
default_start_date = date(2012, 1, 1)
data_hierachy = ["country", "company", "location"]


async def async_main(schema_name):
    engine = create_engine(db_uri(schema_name))
    base = BaseCrawler(schema_name)
    base.create_schema()
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
    base.set_metadata( metadata_info)


async def collect_Date(date, pandas_client: GiePandasClient, engine):
    df_agsi_europe = await pandas_client.query_country_agsi_storage(date=date)
    df_alsi_europe = await pandas_client.query_country_alsi_storage(date=date)

    with engine.begin() as conn:
        recursiveWrite(df_agsi_europe, "agsi", conn, pandas_client, 0)
        recursiveWrite(df_alsi_europe, "alsi", conn, pandas_client, 0)


def select_latest(engine):
    sql = "SELECT gie.gasdaystart FROM gie_agsi_country AS gie ORDER BY gie.gasdaystart DESC LIMIT 1"
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
        log.info("created hypertables for gie")
    except Exception as e:
        log.error(f"could not create hypertable: {e}")


def main(schema_name):
    asyncio.run(async_main(schema_name))


if __name__ == "__main__":
    main("gie")

