# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
This retrieves OPEC raw oil prices since 2003 from OPEC.
The request url can not be accessed from requests, we therefore need to use cloudscraper.
As it is "protected" by cloudflare.

However, the following works:
    curl 'https://www.opec.org/basket/basketDayArchives.xml' --compressed -H 'User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/2'

while wget does not work:
    wget --header='User-Agent: Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/2' --compression=auto 'https://www.opec.org/basket/basketDayArchives.xml'

The resulting dataframe is in USD/Barrel, so we convert it to €/kWh (thermal).

required imports
!pip install cloudscraper lxml yfinance
"""

import logging
from io import StringIO

import cloudscraper
import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text

from common.base_crawler import create_schema_only, set_metadata_only
from common.config import db_uri

log = logging.getLogger("opec")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "opec",
    "data_source": "https://www.opec.org/opec_web/en/data_graphs/40.htm",
    "license": "https://www.opec.org/opec_web/en/35.htm",
    "description": "OPEC crude oil price data in USD/barrel, calculated €/kWh prices from Yahoo",
    "contact": "",
    "temporal_start": "2003-01-02",
}

headers = {
    "Host": "www.opec.org",
    # "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:129.0) Gecko/20100101 Firefox/128.0",
}

DOWNLOAD_URL = "https://www.opec.org/basket/basketDayArchives.xml"


def main(schema_name):
    engine = create_engine(db_uri(schema_name), pool_pre_ping=True)
    create_schema_only(engine, schema_name)

    retries = 0
    max_retries = 5
    while retries < max_retries:
        scraper = cloudscraper.create_scraper()
        resp = scraper.get(DOWNLOAD_URL, headers=headers)

        if resp.status_code > 200:
            retries += 1
            log.info("Error downloading data, retrying (%i/%i)", retries, max_retries)
            import time

            time.sleep(2**retries)
        else:
            break

    if resp.status_code > 200:
        raise Exception("Could not download data")
    df = pd.read_xml(StringIO(resp.text), parse_dates=["data"]).set_index("data")

    start = df.index[0].strftime("%Y-%m-%d")
    end = df.index[-1].strftime("%Y-%m-%d")
    # Download USD/EUR exchange rate data
    usdeur = yf.download("USDEUR=X", start=start, end=end)
    df.index.rename("date", inplace=True)
    df.rename(columns={"val": "usd_per_barrel"}, inplace=True)
    # fill missing index values in opec_price which are missing in yahoo finance data
    df["usd_eur"] = usdeur["Close"]["USDEUR=X"].reindex(df.index).bfill().ffill()
    df["euro_per_barrel"] = df["usd_eur"] * df["usd_per_barrel"]
    # convert euro/barrel to euro/kWh (thermal)
    # 159L, 10kWh/L
    df["euro_per_kwh"] = df["euro_per_barrel"] / 159 / 10

    # usdeur["Close"].plot()
    # df["euro_per_kwh"].plot()
    try:
        with engine.begin() as conn:
            df.to_sql(name="opec", con=conn, if_exists="replace", index=True)
    except Exception:
        log.exception("error in opec")

    try:
        query = text(
            f"select public.create_hypertable('{schema_name}.opec', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
        )
        with engine.begin() as conn:
            conn.execute(query)
        log.error("successfully created hypertable for opec")
    except Exception:
        log.error("could not create hypertable for opec")
    set_metadata_only(engine, metadata_info)


if __name__ == "__main__":
    logging.basicConfig()
    main("opec")
