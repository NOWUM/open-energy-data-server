#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
This crawler downloads all the data of the ENTSO-E transparency platform.
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.
"""

import logging
import os
import time
from datetime import timedelta

import pandas as pd
from entsoe import EntsoePandasClient
from entsoe.exceptions import InvalidBusinessParameterError, NoMatchingDataError
from entsoe.mappings import NEIGHBOURS, PSRTYPE_MAPPINGS, Area
from requests.exceptions import HTTPError
from sqlalchemy import create_engine, text
from tqdm import tqdm

from config import db_uri

log = logging.getLogger("entsoe")
log.setLevel(logging.INFO)

from_n = []
to_n = []

for n1 in NEIGHBOURS:
    for n2 in NEIGHBOURS[n1]:
        from_n.append(n1)
        to_n.append(n2)
neighbours = pd.DataFrame({"from": from_n, "to": to_n})

all_countries = [e.name for e in Area]


def sanitize_series(seriesname):
    """
    replaces illegal values from a series name
    for insertion into database

    Parameters
    ----------
    seriesname : str
        name of the series

    Returns
    -------

    st : str

    """

    st = str.replace(str(seriesname), ")", "")
    st = str.replace(st, "(", "")
    st = str.replace(st, ",", "")
    st = str.replace(st, "'", "")
    st = st.strip()
    st = str.replace(st, " ", "_")
    if st == "0":
        st = "value"
    return st


def calculate_nett_generation(df):
    """
    Calculates the difference between columns ending with _actual_aggregated and _actual_consumption.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with columns ending with _actual_aggregated or _actual_consumption

    Returns
    -------
    dat: pd.DataFrame
    """

    dat = df.copy()
    for c in filter(lambda x: x.endswith("_actual_aggregated"), dat.columns):
        new = str.replace(c, "_actual_aggregated", "")
        dif = list(
            filter(
                lambda x: x.endswith("_actual_consumption") and x.startswith(new),
                dat.columns,
            )
        )
        if len(dif) > 0:
            # calc difference if both exists
            dat[new] = dat[c] - dat[dif[0]]
            del dat[c]  # delete handled series of our copy here
            del dat[dif[0]]
        else:
            # if no consumption exists, directly return aggregated
            dat[new] = dat[c]
            del dat[c]
    for c in filter(lambda x: x.endswith("_actual_consumption"), dat.columns):
        # if only consumption exists use the negative value
        new = str.replace(c, "_actual_consumption", "")
        dat[new] = -dat[c]
        del dat[c]
    return dat


class EntsoeCrawler:
    """
    class to allow easier crawling of ENTSO-E timeseries data
    """

    def __init__(self, database):
        self.engine = create_engine(database)

    def init_base_sql(self):
        """
        write static data to database once
        """
        psrtype = pd.DataFrame.from_dict(
            PSRTYPE_MAPPINGS, orient="index", columns=["prod_type"]
        )
        areas = pd.DataFrame(
            [[e.name, e.value, e.tz, e.meaning] for e in Area],
            columns=["name", "value", "tz", "meaning"],
        )
        with self.engine.begin() as conn:
            areas.columns = [x.lower() for x in areas.columns]
            psrtype.columns = [x.lower() for x in psrtype.columns]
            areas.to_sql("areas", conn, if_exists="replace")
            psrtype.to_sql("psrtype", conn, if_exists="replace")

    def fetch_and_write_entsoe_df_to_db(self, country, proc, start, end):
        """
        Crawl data from ENTSO-E transparency platform and write it to the database

        Parameters
        ----------
        country : str
            2-letter country code
        proc :
            procedure of entsoe-py client
        start : pd.Timestamp
            start time
        end : pd.Timestamp
            end time

        Returns
        -------

        """
        try:
            try:
                data = pd.DataFrame(proc(country, start=start, end=end))
            except NoMatchingDataError:
                raise
            except HTTPError as e:
                log.error(f"{e.response.status_code} - {e.response.reason}")
                if e.response.status_code == 400:
                    raise
                else:
                    log.info(f"retrying: {repr(e)}, {start}, {end}")
                    time.sleep(10)
                    data = pd.DataFrame(proc(country, start=start, end=end))

            except Exception as e:
                log.info(f"retrying: {repr(e)}, {start}, {end}")
                time.sleep(10)
                data = pd.DataFrame(proc(country, start=start, end=end))

            # replace spaces and invalid chars in column names
            data.columns = [sanitize_series(x).lower() for x in data.columns]
            data = data.fillna(0)

            # XXX could have used nett=True in entsoe-py client
            # calculate difference betweeen agg and consumption
            data = calculate_nett_generation(data)

            # add country column
            data["country"] = country
            try:
                with self.engine.begin() as conn:
                    data.to_sql(proc.__name__, conn, if_exists="append")
            except Exception as e:
                with self.engine.begin() as conn:
                    log.info(f"handling {repr(e)} by concat")
                    # merge old data with new data
                    prev = pd.read_sql_query(
                        f"select * from {proc.__name__}", conn, index_col="index"
                    )
                    dat = pd.concat([prev, data])
                    # convert type as pandas needs it
                    dat.index = pd.to_datetime(dat.index, utc=True)
                    dat.to_sql(proc.__name__, conn, if_exists="replace")
                    log.info(f"replaced table {proc.__name__}")
        except NoMatchingDataError:
            log.error(f"no data found for {proc.__name__}, {country}, {start}, {end}")
        except Exception as e:
            log.error(
                f"error downloading {proc.__name__}, {country}, {start}, {end}: {e}"
            )

    def get_latest_crawled_timestamp(self, start, delta, tablename, tz="Europe/Berlin"):
        """
        Find the best Start for the given procedurename by finding the last timestemp where data was collected for.
        Also calculates the best delta to update until today.

        Parameters
        ----------
        start : pd.Timestamp
        delta : pd.Timedelta
            to check if a delta has already been set
        tablename : str
            name of the table
        tz :  str
            (Default value = 'Europe/Berlin')

        Returns
        -------
        type
        start : pd.Timestamp
            best start
        delta : pd.Timedelta
            best delta

        """

        if start and delta:
            return start, delta
        else:
            try:
                with self.engine.begin() as conn:
                    query = text(f'select max("index") from {tablename}')
                    d = conn.execute(query).fetchone()[0]
                start = pd.to_datetime(d)
                try:
                    start = start.tz_localize("Europe/Berlin")
                except TypeError:
                    # if already localized
                    pass
            except Exception as e:
                start = pd.Timestamp("20150101", tz=tz)
                log.info(f"using default {start} timestamp ({e})")

            end = pd.Timestamp.now(tz=tz)
            delta = end - start
            return start, delta

    def download_entsoe(self, countries, proc, start, delta, times):
        """
        Downloads data with a procedure from a EntsoePandasClient
        and stores it in the configured database

        Parameters
        ----------
        countries : list[str]
            list of country codes
        proc :
            procedure of entsoe-py
        start : pd.Timestamp

        delta : pd.Timedelta

        times : int


        Returns
        -------

        """
        log.info(f"****** {proc.__name__} *******")

        if (times * delta).days < 2:
            log.info("nothing to do")
            return
        for i in range(times):
            start_ = start + i * delta
            end_ = start + (i + 1) * delta
            # daten für jedes Land runterladen
            pbar = tqdm(countries)
            for country in pbar:
                pbar.set_description(f"{country} {start_:%Y-%m-%d} to {end_:%Y-%m-%d}")

                self.fetch_and_write_entsoe_df_to_db(country, proc, start_, end_)

        # indexe anlegen für schnelles suchen
        try:
            with self.engine.begin() as conn:
                log.info(f"creating index country_idx_{proc.__name__}")
                query = text(
                    f'CREATE INDEX IF NOT EXISTS "country_idx_{proc.__name__}" ON "{proc.__name__}" ("country", "index");'
                )
                conn.execute(query)
                # query = text(f'CREATE INDEX IF NOT EXISTS "country_{proc.__name__}" ON "{proc.__name__}" ("country");')
                # conn.execute(query)
                log.info(f"created indexes country_idx_{proc.__name__}")
        except Exception as e:
            log.error(f"could not create index if needed: {e}")

        # falls es eine TimescaleDB ist, erzeuge eine Hypertable
        try:
            with self.engine.begin() as conn:
                query_create_hypertable = text(
                    f"SELECT public.create_hypertable('{proc.__name__}', 'index', if_not_exists => TRUE, migrate_data => TRUE);"
                )
                conn.execute(query_create_hypertable)
            log.info(f"created hypertable {proc.__name__}")
        except Exception as e:
            log.error(f"could not create hypertable: {e}")

    def pull_crossborders(self, start, delta, times, proc, allZones=True):
        """
        Pulls transmissions across borders from entsoe

        Parameters
        ----------
        start :
            param delta:
        times :
            param proc:
        allZones :
            Default value = True)
        delta :
            param proc:
        proc :


        Returns
        -------

        """
        start, delta = self.get_latest_crawled_timestamp(start, delta, proc.__name__)
        log.info(f"****** {proc.__name__} *******")

        if (times * delta).days < 2:
            log.info("nothing to do")
            return

        for i in range(times):
            data = pd.DataFrame()
            start_ = start + i * delta
            end_ = start + (i + 1) * delta
            log.info(start_)

            for n1 in tqdm(NEIGHBOURS):
                for n2 in NEIGHBOURS[n1]:
                    try:
                        if (len(n1) == 2 and len(n2) == 2) or allZones:
                            dataN = proc(n1, n2, start=start_, end=end_)
                            data[n1 + "-" + n2] = dataN
                    except (NoMatchingDataError, InvalidBusinessParameterError):
                        # log.info('no data found for ',n1,n2)
                        pass
                    except Exception as e:
                        log.error(f"Error crawling Crossboarders {e}")
                data = data.copy()

            data.columns = [x.lower() for x in data.columns]
            try:
                with self.engine.begin() as conn:
                    data.to_sql(proc.__name__, conn, if_exists="append")
            except Exception as e:
                log.error(f"error saving crossboarders {e}")
                with self.engine.begin() as conn:
                    prev = pd.read_sql_query(
                        f"select * from {proc.__name__}", conn, index_col="index"
                    )

                    ges = pd.concat([prev, data])
                    ges.index = pd.to_datetime(ges.index, utc=True)
                    ges.to_sql(proc.__name__, conn, if_exists="replace")
                log.info("fixed error by adding new columns to crossborders")

            try:
                with self.engine.begin() as conn:
                    query_create_hypertable = text(
                        f"SELECT public.create_hypertable('{proc.__name__}', 'index', if_not_exists => TRUE, migrate_data => TRUE);"
                    )
                    conn.execute(query_create_hypertable)
            except Exception as e:
                log.error(f"could not create hypertable: {e}")

    def save_power_system_data(self):
        """
        pulls static data from opsd and reads it into the database
        - used for mapping existing power plants from entsoe to a location on a map

        Parameters
        ----------

        Returns
        -------

        """
        df = pd.read_csv(
            "https://data.open-power-system-data.org/conventional_power_plants/latest/conventional_power_plants_EU.csv"
        )
        df = df.dropna(axis=0, subset=["lon", "lat", "eic_code"])
        df = df[
            [
                "eic_code",
                "name",
                "company",
                "country",
                "capacity",
                "energy_source",
                "lon",
                "lat",
            ]
        ]
        # delete those without location or eic_code

        with self.engine.begin() as conn:
            df.to_sql("powersystemdata", conn, if_exists="replace")
        return df

    def download_entsoe_plant_data(self, countries, client, start, delta, times):
        """
        Allows to download the generation per power plant from entsoe.
        Uses download_entsoe to write the data into the DB.

        Parameters
        ----------
        countries : list[str]
            list of 2-letter countrycodes
        client : entsoe.EntsoePandasClient
            DataFrameClient of entsoe-py package
        start : pd.Timestamp
            timestamp aware pd.Timestamp
        delta : pd.Timedelta
            Timedelta to fetch data for per bulk
        times : int
            number of bulks with size delta to fetch

        Returns
        -------

        """

        # new proxy function
        def query_per_plant(country, start, end):
            """
            wrapper function around query_generation_per_plant to convert multiindex

            Parameters
            ----------
            country : str
                country to fetch
            start : pd.DateTime
                param end:
            end :


            Returns
            -------

            """
            ppp = client.query_generation_per_plant(country, start=start, end=end)
            # convert multiindex into second column
            pp = ppp.melt(
                var_name=["name", "type"], value_name="value", ignore_index=False
            )
            return pp

        log.info(f"****** {query_per_plant.__name__} *******")
        start_, delta_ = self.get_latest_crawled_timestamp(
            start, delta, query_per_plant.__name__
        )
        self.download_entsoe(
            countries, query_per_plant, start_, delta=delta_, times=times
        )

        try:
            with self.engine.begin() as conn:
                query = text(
                    'CREATE INDEX IF NOT EXISTS "idx_name_query_per_plant" ON "query_per_plant" ("name", "index", "country");'
                )
                conn.execute(query)
        except Exception as e:
            log.error(f"could not create index: {e}")

        try:
            with self.engine.begin() as conn:
                query = "select distinct name, country,type from query_per_plant"
                names = pd.read_sql_query(query, conn)
                names.to_sql("plant_names", conn, if_exists="replace")
        except Exception as e:
            log.error(f"could not create plant_names: {e}")

    def countries_with_plant_data(
        self,
        client,
        countries=all_countries,
        st=pd.Timestamp("20180101", tz="Europe/Berlin"),
    ):
        """
        checks for all countries if the have available data at date.
        Returns list of countries with existing generation data per plant at given timestamp

        Parameters
        ----------
        client : entsoe.EntsoePandasClient
        countries : list[str], default all_countries

        Returns
        -------
        plant_countries : list[str]
            list of country_codes with existing data for generation per plant

        """
        plant_countries = []
        log.info("****** find countries with plant_data *******")
        for country in countries:
            try:
                _ = client.query_generation_per_plant(
                    country, start=st, end=st + timedelta(days=1)
                )
                plant_countries.append(country)
                log.info(f"found data for {country}")
            except Exception:
                continue
        return plant_countries

    def update_database(self, client, start=None, delta=None, countries=all_countries):
        """
        Runs everything which is needed to update the database and pull the data since the last successful pull.

        Parameters
        ----------
        client : entsoe.EntsoePandasClient
            entsoe-py client
        delta : pd.Timedelta
        countries : list[str], default all_countries
        start : pd.Timestamp

        Returns
        -------

        """
        proc_cap = client.query_installed_generation_capacity
        start_, delta_ = self.get_latest_crawled_timestamp(
            start, delta, proc_cap.__name__
        )

        if delta_.days > 365:
            self.download_entsoe(countries, proc_cap, start_, delta=delta_, times=1)

        # timeseries
        ts_procs = [
            client.query_day_ahead_prices,
            client.query_load,
            client.query_load_forecast,
            client.query_generation_forecast,
            client.query_wind_and_solar_forecast,
            client.query_generation,
        ]

        # Download load and generation
        # hier könnte man parallelisieren
        for proc in ts_procs:
            start_, delta_ = self.get_latest_crawled_timestamp(
                start, delta, proc.__name__
            )
            self.download_entsoe(countries, proc, start_, delta_, times=1)

        self.pull_crossborders(start, delta, 1, client.query_crossborder_flows)

        plant_countries = self.countries_with_plant_data(client)

        self.download_entsoe_plant_data(
            plant_countries[:], client, start, delta, times=1
        )

        log.info("****** finished updating ENTSO-E *******")

    def create_database(self, client, start, delta, countries=all_countries):
        """

        Parameters
        ----------
        client : entsoe.EntsoePandasClient
            param start:
        delta :
            param countries:  (Default value = [])
        start :
            datetime
        countries :
             (Default value = all_countries)

        Returns
        -------

        """
        self.init_base_sql()
        self.save_power_system_data()
        self.download_entsoe(
            countries,
            client.query_installed_generation_capacity_per_unit,
            start,
            delta=delta,
            times=1,
        )


def main(db_uri):
    api_key = os.getenv("ENTSOE_API_KEY", "ae2ed060-c25c-4eea-8ae4-007712f95375")
    client = EntsoePandasClient(api_key=api_key)
    crawler = EntsoeCrawler(database=db_uri)

    start = pd.Timestamp("20150101", tz="Europe/Berlin")
    delta = pd.Timestamp.now(tz="Europe/Berlin") - start
    crawler.create_database(client, start, delta)
    crawler.update_database(client, start, delta)


if __name__ == "__main__":
    logging.basicConfig()
    log.info("ENTSOE")
    """
    First register at ENTSO-E transparency portal by clicking login or this link:
    https://transparency.entsoe.eu/protected-url
    Generate Token as documented here:
    https://iop-transparency.entsoe.eu/content/static_content/download?path=/Static%20content/API-Token-Management.pdf
    """
    client = EntsoePandasClient(api_key="XXX")

    start = pd.Timestamp("20150101", tz="Europe/Berlin")
    delta = timedelta(days=30)
    end = start + delta

    times = 7 * 12  # bis 2022
    db = db_uri("entsoe")
    # db = 'sqlite:///data/entsoe.db'

    crawler = EntsoeCrawler(database=db)
    procs = [
        client.query_day_ahead_prices,
        client.query_net_position,
        client.query_load,
        client.query_load_forecast,
        client.query_generation_forecast,
        client.query_wind_and_solar_forecast,
        client.query_generation,
    ]
    times = 1
    # Download load and generation
    for proc in procs:
        # hier könnte man parallelisieren
        crawler.download_entsoe(all_countries, proc, start, delta, times)

    # Capacities
    procs = [
        client.query_installed_generation_capacity,
        client.query_installed_generation_capacity_per_unit,
    ]

    # crawler.pull_crossborders(start,delta,1,client.query_crossborder_flows)

    # per plant generation
    plant_countries = crawler.countries_with_plant_data(client, all_countries)

    # db = 'sqlite:///data/entsoe.db'
    crawler = EntsoeCrawler(database=db)

    # 2017-12-16 bis 2018-03-15 runterladen
    crawler.download_entsoe_plant_data(plant_countries[:], client, start, delta, times)

    # create indices if not existing
