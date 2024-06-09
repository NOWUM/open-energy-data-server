# SPDX-FileCopyrightText: Steffen Carstensen
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
https://regelleistung.net/
"""

import logging
import sys
import warnings
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine

from .config import db_uri

log = logging.getLogger("regelleistung")
log.setLevel(logging.INFO)

EARLIEST_DATE_TO_WRITE = datetime.strptime("2020-01-01", "%Y-%m-%d").date()

TABLE_NAME_FCR_DEMANDS = "fcr_bedarfe"
URL_FCR_DEMANDS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/demands?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=FCR"
TABLE_NAME_FCR_RESULTS = "fcr_ergebnisse"
URL_FCR_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/resultsoverview?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=FCR"
TABLE_NAME_FCR_ANONYM_RESULTS = "fcr_anonyme_ergebnisse"
URL_FCR_ANONYM_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/anonymousresults?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=FCR"

TABLE_NAME_AFRR_DEMANDS = "afrr_bedarfe"
URL_AFRR_DEMANDS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/demands?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=aFRR"
TABLE_NAME_AFRR_RESULTS = "afrr_ergebnisse"
URL_AFRR_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/resultsoverview?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=aFRR"
TABLE_NAME_AFRR_ANONYM_RESULTS = "afrr_anonyme_ergebnisse"
URL_AFRR_ANONYM_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/anonymousresults?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=aFRR"

TABLE_NAME_MFRR_DEMANDS = "mfrr_bedarfe"
URL_MFRR_DEMANDS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/demands?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=mFRR"
TABLE_NAME_MFRR_RESULTS = "mfrr_ergebnisse"
URL_MFRR_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/resultsoverview?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=mFRR"
TABLE_NAME_MFRR_ANONYM_RESULTS = "mfrr_anonyme_ergebnisse"
URL_MFRR_ANONYM_RESULTS = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/anonymousresults?date={date_str}&exportFormat=xlsx&market=CAPACITY&productTypes=mFRR"


def get_date_from_sql(engine, table_name, sql):
    try:
        with engine.begin() as conn:
            df = pd.read_sql(sql, conn, parse_dates=["date_from"])
            date = (df["date_from"][0]).date()
            return date
    except sqlalchemy.exc.ProgrammingError as e:
        _, err_obj, _ = sys.exc_info()
        if "psycopg2.errors.UndefinedTable" in str(err_obj):
            log.info(f"There does not exist a table {table_name} yet.")
            return None
        else:
            log.error(e)
    except Exception as e:
        log.error(e)


def get_latest_date_if_table_exists(engine, table_name):
    sql = f"SELECT date_from FROM {table_name} ORDER BY date_from DESC LIMIT 1"
    return get_date_from_sql(engine, table_name, sql)


def get_earliest_date_if_table_exists(engine, table_name):
    sql = f"SELECT date_from FROM {table_name} ORDER BY date_from ASC LIMIT 1"
    return get_date_from_sql(engine, table_name, sql)


def database_friendly(string):
    return (
        string.lower()
        .replace("(eur/mw)/h", "eur_mwh")
        .replace("productname", "product")
        .replace("[", "")
        .replace("]", "")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        .replace("+", "")
        .replace(" ", "_")
        # Table fcr_ergebnisse
        .replace("fr_demand_mw", "france_demand_mw")
        .replace("dk_demand_mw", "denmark_demand_mw")
        .replace("nl_demand_mw", "netherlands_demand_mw")
        .replace("at_demand_mw", "austria_demand_mw")
        .replace("be_demand_mw", "belgium_demand_mw")
        .replace("de_demand_mw", "germany_demand_mw")
        .replace("ch_demand_mw", "switzerland_demand_mw")
        .replace("si_demand_mw", "slovenia_demand_mw")
        .replace("at_import_export_mw", "austria_deficit_surplus_mw")
        .replace("fr_import_export_mw", "france_deficit_surplus_mw")
        .replace("dk_import_export_mw", "denmark_deficit_surplus_mw")
        .replace("ch_import_export_mw", "switzerland_deficit_surplus_mw")
        .replace("si_import_export_mw", "slovenia_deficit_surplus_mw")
        .replace("be_import_export_mw", "belgium_deficit_surplus_mw")
        .replace("de_import_export_mw", "germany_deficit_surplus_mw")
        .replace("nl_import_export_mw", "netherlands_deficit_surplus_mw")
        .replace(
            "at_settlementcapacity_price_eur_mw",
            "austria_settlementcapacity_price_eur_mw",
        )
        .replace(
            "ch_settlementcapacity_price_eur_mw",
            "switzerland_settlementcapacity_price_eur_mw",
        )
        .replace(
            "de_settlementcapacity_price_eur_mw",
            "germany_settlementcapacity_price_eur_mw",
        )
        .replace(
            "si_settlementcapacity_price_eur_mw",
            "slovenia_settlementcapacity_price_eur_mw",
        )
        .replace(
            "be_settlementcapacity_price_eur_mw",
            "belgium_settlementcapacity_price_eur_mw",
        )
        .replace(
            "dk_settlementcapacity_price_eur_mw",
            "denmark_settlementcapacity_price_eur_mw",
        )
        .replace(
            "nl_settlementcapacity_price_eur_mw",
            "netherlands_settlementcapacity_price_eur_mw",
        )
        .replace(
            "fr_settlementcapacity_price_eur_mw",
            "france_settlementcapacity_price_eur_mw",
        )
    )


def get_df_for_date(url, date_to_get):
    date_str = date_to_get.strftime("%Y-%m-%d")
    url_with_date = url.format(date_str=date_str)
    warnings.filterwarnings(
        action="ignore",
        category=UserWarning,
        message="Workbook contains no default style, apply openpyxl's default",
    )
    df = pd.read_excel(url_with_date, sheet_name="001", na_values=["-", "n.a.", "n.e."])
    df.rename(mapper=lambda x: database_friendly(x), axis="columns", inplace=True)

    # adapt date_from and date_to column
    product_split_array = (df["product"].str.split("_")).to_numpy()
    hours_from = np.array([product_list[1] for product_list in product_split_array])
    hours_to = np.array([product_list[2] for product_list in product_split_array])
    timedelta_from = np.array([timedelta(hours=int(hour)) for hour in hours_from])
    timedelta_to = np.array([timedelta(hours=int(hour)) for hour in hours_to])
    df["date_from"] = df["date_from"] + pd.to_timedelta(timedelta_from, "d")
    df["date_to"] = df["date_to"] + pd.to_timedelta(timedelta_to, "d")

    # adapt mw column to mwh column
    hours_from = np.array([product_list[1] for product_list in product_split_array])
    hours_to = np.array([product_list[2] for product_list in product_split_array])
    hours_from_int = (hours_from).astype(np.int16)
    hours_to_int = (hours_to).astype(np.int16)
    hours_diff = hours_to_int - hours_from_int
    cols_to_adapt = [
        "total_min_capacity_price_eur_mw",
        "total_average_capacity_price_eur_mw",
        "total_marginal_capacity_price_eur_mw",
        "germany_min_capacity_price_eur_mw",
        "germany_average_capacity_price_eur_mw",
        "germany_marginal_capacity_price_eur_mw",
        "austria_min_capacity_price_eur_mw",
        "austria_average_capacity_price_eur_mw",
        "austria_marginal_capacity_price_eur_mw",
        "capacity_price_eur_mw",
    ]
    for col_to_adapt in cols_to_adapt:
        if col_to_adapt in df.columns:
            final_col_name = col_to_adapt + "h"
            df[final_col_name] = pd.to_numeric(df[col_to_adapt]) / hours_diff

    cols_to_drop = [
        "total_min_energy_price_eur_mwh",
        "total_average_energy_price_eur_mwh",
        "total_marginal_energy_price_eur_mwh",
        "germany_min_energy_price_eur_mwh",
        "germany_average_energy_price_eur_mwh",
        "germany_marginal_energy_price_eur_mwh",
        "austria_min_energy_price_eur_mwh",
        "austria_average_energy_price_eur_mwh",
        "austria_marginal_energy_price_eur_mwh",
        "total_min_capacity_price_eur_mw",
        "total_average_capacity_price_eur_mw",
        "total_marginal_capacity_price_eur_mw",
        "germany_min_capacity_price_eur_mw",
        "germany_average_capacity_price_eur_mw",
        "germany_marginal_capacity_price_eur_mw",
        "austria_min_capacity_price_eur_mw",
        "austria_average_capacity_price_eur_mw",
        "austria_marginal_capacity_price_eur_mw",
        "capacity_price_eur_mw",
    ]
    for col_to_drop in cols_to_drop:
        if col_to_drop in df.columns:
            df = df.drop(col_to_drop, axis=1)
    return df


def write_concat_table(engine, table_name, new_data):
    with engine.begin() as conn:
        # merge old data with new data
        prev = pd.read_sql_query(f"select * from {table_name}", conn)
        new_cols = set(new_data.columns).difference(set(prev.columns))
        removed_cols = set(prev.columns).difference(set(new_data.columns))
        log.info(f"New columns: {new_cols}")
        log.info(f"Removed columns: {removed_cols}")
        log.info(new_data["date_from"])
        complete_data = pd.concat([prev, new_data])
        complete_data.to_sql(table_name, conn, if_exists="replace", index=False)


def write_past_entries(
    engine,
    table_name,
    url,
    earliest_date,
    earliest_date_to_write=EARLIEST_DATE_TO_WRITE,
):
    data_for_date_exists = True
    wrote_data = False

    while data_for_date_exists and (earliest_date_to_write < earliest_date):
        try:
            earliest_date -= timedelta(days=1)
            df = get_df_for_date(url, earliest_date)
            with engine.begin() as conn:
                df.to_sql(table_name, conn, if_exists="append", index=False)
            wrote_data = True
        except sqlalchemy.exc.ProgrammingError as e:
            _, err_obj, _ = sys.exc_info()
            if "psycopg2.errors.UndefinedColumn" in str(err_obj):
                log.info(f"handling {repr(e)} by concat")
                write_concat_table(engine, table_name, df)
                log.info(f"replaced table {table_name}")
                wrote_data = True
            else:
                log.error(f"Encountered error {e}")
                data_for_date_exists = False
        except Exception as e:
            log.info(
                f"The earliest date for {table_name} is the date {earliest_date}. {e}"
            )
            data_for_date_exists = False

    if wrote_data:
        log.info(
            f"Finished writing {table_name} to Database with earliest date {earliest_date}"
        )
    elif not wrote_data and data_for_date_exists:
        log.info(
            f"The defined date for the earliest entry was already reached in {table_name}. If you want to have more data, simply adjust the earliest date to write parameter."
        )
    else:
        log.info(f"No past data was written for {table_name}")


def create_table_and_write_past_data(
    engine, url, table_name, earliest_date_to_write=EARLIEST_DATE_TO_WRITE
):
    log.info(f"Start creating table {table_name} and adding new data")
    earliest_date = date.today()
    write_past_entries(engine, table_name, url, earliest_date, earliest_date_to_write)


def add_additional_past_entries(
    engine, table_name, url, earliest_date_to_write=EARLIEST_DATE_TO_WRITE
):
    log.info(f"Start writing missing past entries in table {table_name} if any")
    earliest_date = get_earliest_date_if_table_exists(engine, table_name)
    write_past_entries(engine, table_name, url, earliest_date, earliest_date_to_write)


def write_new_data_from_latest_date_to_today(engine, url, table_name, latest_data_date):
    log.info(f"Start writing new data to {table_name}")

    today_date = datetime.today().date()

    if latest_data_date == (today_date - timedelta(days=1)):
        log.info(f"Table {table_name} has already the newest data.")
    else:
        latest_data_date = latest_data_date + timedelta(days=1)
        encountered_problem = False
        while latest_data_date < today_date and not encountered_problem:
            try:
                df = get_df_for_date(url, latest_data_date)
                with engine.begin() as conn:
                    df.to_sql(table_name, conn, if_exists="append", index=False)
                latest_data_date += timedelta(days=1)
            except sqlalchemy.exc.ProgrammingError as e:
                _, err_obj, _ = sys.exc_info()
                if "psycopg2.errors.UndefinedColumn" in str(err_obj):
                    log.info(f"handling {repr(e)} by concat")
                    write_concat_table(engine, table_name, df)
                    log.info(f"replaced table {table_name}")
                else:
                    log.error(f"Encountered error {e}")
                    encountered_problem = True
            except Exception:
                encountered_problem = True

        log.info(
            f"Finished writing new data to {table_name} with newest date being yesterday {(latest_data_date - timedelta(days=1))}"
        )


def write_data_in_table(
    engine,
    table_name,
    url,
    earliest_date_to_write=EARLIEST_DATE_TO_WRITE,
    write_additional_past_entries_if_any=True,
):
    latest_date = get_latest_date_if_table_exists(engine, table_name)
    if latest_date is not None:
        write_new_data_from_latest_date_to_today(engine, url, table_name, latest_date)
        if write_additional_past_entries_if_any:
            add_additional_past_entries(engine, table_name, url, earliest_date_to_write)
    else:
        create_table_and_write_past_data(
            engine, url, table_name, earliest_date_to_write
        )


def write_all_tables(engine):
    write_data_in_table(engine, TABLE_NAME_FCR_DEMANDS, URL_FCR_DEMANDS)
    write_data_in_table(engine, TABLE_NAME_FCR_RESULTS, URL_FCR_RESULTS)
    write_data_in_table(engine, TABLE_NAME_FCR_ANONYM_RESULTS, URL_FCR_ANONYM_RESULTS)

    write_data_in_table(engine, TABLE_NAME_AFRR_DEMANDS, URL_AFRR_DEMANDS)
    write_data_in_table(engine, TABLE_NAME_AFRR_RESULTS, URL_AFRR_RESULTS)
    write_data_in_table(engine, TABLE_NAME_AFRR_ANONYM_RESULTS, URL_AFRR_ANONYM_RESULTS)

    write_data_in_table(engine, TABLE_NAME_MFRR_DEMANDS, URL_MFRR_DEMANDS)
    write_data_in_table(engine, TABLE_NAME_MFRR_RESULTS, URL_MFRR_RESULTS)
    write_data_in_table(engine, TABLE_NAME_MFRR_ANONYM_RESULTS, URL_MFRR_ANONYM_RESULTS)


def main(db_uri):
    engine = create_engine(db_uri)
    write_all_tables(engine)


if __name__ == "__main__":
    logging.basicConfig()
    main(db_uri("regelleistung"))
