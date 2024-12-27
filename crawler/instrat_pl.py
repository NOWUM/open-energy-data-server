# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
This retrieves EU ETS prices from a polish website.

The EU Emission Trading System (ETS) sells emission allowances to
companies that emit greenhouse gases.

For this lots of 1000t CO2eq are sold with a given price in €/t CO2eq.
"""

import logging

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text

from common.base_crawler import create_schema_only, set_metadata_only
from common.config import db_uri

log = logging.getLogger("instrat_pl")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "eu_ets",
    "data_source": "https://energy.instrat.pl/en/prices/eu-ets/",
    "license": "CC-BY-4.0",
    "description": "EU-ETS, coal and gas prices from polish energy provider",
    "contact": "",
    "temporal_start": "2012-01-03",
}

# €/tCO2
EU_ETS_URL = "https://energy-instrat-api.azurewebsites.net/api/prices/co2?all=1"
# coal used for electricity generation PLN/GJ or PLN/t
COAL_URL = "https://energy-instrat-api.azurewebsites.net/api/coal/pscmi_1?all=1"
# the heat_url switches between Y-m-d and Y-d-m and is not practicable to parse
COAL_HEAT_URL = "https://energy-instrat-api.azurewebsites.net/api/coal/pscmi_2?all=1"
# gas used for electricity generation PLN/MWh
GAS_URL = (
    "https://energy-instrat-api.azurewebsites.net/api/prices/gas_price_rdn_daily?all=1"
)


def main(schema_name):
    engine = create_engine(db_uri(schema_name), pool_pre_ping=True)
    create_schema_only(engine, schema_name)

    ### EU ETS
    eu_ets_data_raw = pd.read_json(EU_ETS_URL).set_index("date")
    eu_ets_data_raw.index = eu_ets_data_raw.index.tz_localize(None)
    eu_ets_data = eu_ets_data_raw.resample("D").bfill()
    eu_ets_data.rename(columns={"price": "eur_per_tco2"}, inplace=True)

    ### COAL
    coal_data = pd.read_json(COAL_URL).set_index("date")
    coal_data.index = coal_data.index.tz_localize(None)

    start = coal_data.index[0].strftime("%Y-%m-%d")
    end = coal_data.index[-1].strftime("%Y-%m-%d")
    pln_eur = yf.download("PLNEUR=X", start=start, end=end)["Close"]["PLNEUR=X"]
    # coal_data["pscmi1_pln_per_gj"].plot()
    resample_pln_eur = pln_eur.resample("MS").bfill().ffill()
    resample_pln_eur = resample_pln_eur.reindex(coal_data.index).ffill()
    coal_data["steam_coal_eur_per_gj"] = (
        coal_data["pscmi1_pln_per_gj"] * resample_pln_eur
    )
    coal_data["steam_coal_eur_per_t"] = coal_data["pscmi1_pln_per_t"] * resample_pln_eur
    # 1 GJ = 1e9 Ws = 1e9/3600 Wh = 1e6/3600 kWh
    coal_data["price_eur_per_kwh"] = coal_data["steam_coal_eur_per_gj"] / (
        1e6 / 3600
    )  # GJ to kWh

    ### GAS data
    gas_data = pd.read_json(GAS_URL).set_index("date")
    # remove timezone from data
    gas_data.index = gas_data.index.tz_localize(None)

    start = gas_data.index[0].strftime("%Y-%m-%d")
    end = gas_data.index[-1].strftime("%Y-%m-%d")
    pln_eur = yf.download("PLNEUR=X", start=start, end=end)["Close"]["PLNEUR=X"]
    resample_pln_eur = pln_eur.reindex(gas_data.index).bfill().ffill()

    gas_data.rename(columns={"price": "price_pln_per_mwh"}, inplace=True)
    gas_data["price_eur_per_mwh"] = gas_data["price_pln_per_mwh"] * resample_pln_eur
    gas_data["price_eur_per_kwh"] = gas_data["price_eur_per_mwh"] / 1e3
    try:
        with engine.begin() as conn:
            eu_ets_data.to_sql(name="eu_ets", con=conn, if_exists="replace", index=True)
            coal_data.to_sql(
                name="coal_price", con=conn, if_exists="replace", index=True
            )
            gas_data.to_sql(name="gas_price", con=conn, if_exists="replace", index=True)
    except Exception:
        log.exception("error in eu_ets")
    try:
        for table in ["eu_ets", "coal_price", "gas_price"]:
            query = text(
                f"select public.create_hypertable('{schema_name}.{table}', 'date', if_not_exists => TRUE, migrate_data => TRUE)"
            )
            with engine.begin() as conn:
                conn.execute(query)
        log.error("successfully created hypertable for eu_ets")
    except Exception:
        log.error("could not create hypertable for eu_ets")
    set_metadata_only(engine, metadata_info)


if __name__ == "__main__":
    logging.basicConfig()
    main("instrat_pl")
