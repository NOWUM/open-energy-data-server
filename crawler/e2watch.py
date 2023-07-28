import pandas as pd
import requests
import logging
import json
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from crawler.base_crawler import BasicDbCrawler
import os

log = logging.getLogger('e2watch')
default_start_date = '2023-01-01 00:00:00'


class E2WatchCrawler(BasicDbCrawler):

    def create_table(self):
        try:
            query_create_hypertable = "SELECT create_hypertable('e2watch', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);"
            with self.db_accessor() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS e2watch( "
                             "timestamp timestamp without time zone NOT NULL, "
                             "bilanzkreis_id text, "
                             "strom_kwh double precision, "
                             "wasser_m3 double precision, "
                             "waerme_kwh double precision, "
                             "temperatur double precision, "
                             "PRIMARY KEY (timestamp , bilanzkreis_id));")
                conn.execute(query_create_hypertable)
            log.info(f'created hypertable e2watch')
        except Exception as e:
            log.error(f'could not create hypertable: {e}')

        try:
            with self.db_accessor() as conn:
                conn.execute("CREATE TABLE IF NOT EXISTS buildings( "
                             "bilanzkreis_id text, "
                             "building_id text, "
                             "lat double precision, "
                             "lon double precision, "
                             "beschreibung text, "
                             "strasse text, "
                             "plz text, "
                             "stadt text, "
                             "PRIMARY KEY (bilanzkreis_id));")
            log.info(f'created table buildings')
        except Exception as e:
            log.error(f'could not create table: {e}')

    def get_all_buildings(self):
        sql = f"select * from buildings"
        with self.db_accessor() as connection:
            try:
                building_data = pd.read_sql(sql, connection, parse_dates=['timestamp'])
                if len(building_data) > 0:
                    log.info(f'Building data already exists in the database. No need to crawl it again.')
                    building_data = building_data.set_index(['bilanzkreis_id'])
                    return building_data
            except Exception as e:
                log.error(f'There does not exist a table buildings yet. The buildings will now be crawled. {e}')

        df = pd.read_csv(os.path.realpath(os.path.join(os.path.dirname(__file__), 'data', 'e2watch_building_data.csv')))
        df = df.set_index(['bilanzkreis_id'])
        return df

    def get_data_per_building(self, buildings: pd.DataFrame):
        energy = ['strom', 'wasser', 'waerme']
        end_date = date.today().strftime('%d.%m.%Y')

        for bilanzkreis_id in buildings.index.values:
            df_last = pd.DataFrame([])
            for measurement in energy:
                start_date = self.select_latest(bilanzkreis_id) + timedelta(hours=1)
                start_date_tz = start_date.tz_localize('UTC').tz_convert('Europe/Berlin')
                start_date_str = start_date_tz.strftime("%d.%m.%Y %H:%M:%S")
                url = f'https://stadt-aachen.e2watch.de/gebaeude/getMainChartData/{bilanzkreis_id}?medium={measurement}&from={start_date_str}&to={end_date}&type=stundenverbrauch'
                log.info(url)
                response = requests.get(url)
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    log.error(f'Could not get data for building: {bilanzkreis_id} {e}')
                    continue
                data = json.loads(response.text)
                timeseries = pd.DataFrame.from_dict(data['result']['series'][0]['data'])
                if timeseries.empty:
                    log.info(f'Received empty data for building: {bilanzkreis_id}')
                    continue
                timeseries[0] = pd.to_datetime(timeseries[0], unit='ms', utc=True)
                timeseries.columns = ['timestamp', measurement + '_kwh' if (
                            measurement == 'strom' or measurement == 'waerme') else measurement + '_m3']
                temperature = pd.DataFrame.from_dict(data['result']['series'][1]['data'])
                if temperature.empty:
                    log.info(f'Received empty temperature for building: {bilanzkreis_id}')
                    continue
                temperature[0] = pd.to_datetime(temperature[0], unit='ms', utc=True)
                temperature.columns = ['timestamp', 'temperatur']
                timeseries = pd.merge(timeseries, temperature, on=['timestamp'])

                if not df_last.empty:
                    df_last = pd.merge(timeseries, df_last, on=['timestamp', 'temperatur'])

                else:
                    df_last = timeseries

            if not df_last.empty:
                df_last.insert(0, 'bilanzkreis_id', bilanzkreis_id)
            yield df_last

    def select_latest(self, bilanzkreis_id) -> pd.Timestamp:
        # day = default_start_date
        # today = date.today().strftime('%d.%m.%Y')
        # sql = f"select timestamp from e2watch where timestamp > '{day}' and timestamp < '{today}' order by timestamp desc limit 1"
        sql = f"select timestamp from e2watch where bilanzkreis_id='{bilanzkreis_id}' order by timestamp desc limit 1"
        with self.db_accessor() as connection:
            try:
                latest = pd.read_sql(sql, connection, parse_dates=['timestamp']).values[0][0]
                latest = latest.astype(int)
                latest = pd.to_datetime(latest, unit='ns')
                log.info(f'The latest date in the database is {latest}')
                return latest
            except Exception as e:
                log.info(f'Using the default start date {e}')
                return pd.to_datetime(default_start_date)

    def feed(self, buildings: pd.DataFrame):
        sql = f"select * from buildings"
        with self.db_accessor() as connection:
            try:
                building_data = pd.read_sql(sql, connection, parse_dates=['timestamp'])
                if len(building_data) == 0:
                    log.info(f'creating new buildings table')
                    buildings.to_sql('buildings', con=connection, if_exists='append')
            except Exception as e:
                log.info(f'Probably no database connection: {e}')
        for data_for_building in self.get_data_per_building(buildings):
            if data_for_building.empty:
                continue
            with self.db_accessor() as connection:
                data_for_building = data_for_building.set_index(['timestamp', 'bilanzkreis_id'])
                # delete timezone duplicate
                # https://stackoverflow.com/a/34297689
                data_for_building = data_for_building[~data_for_building.index.duplicated(keep='first')]

                log.info(data_for_building)
                data_for_building.to_sql('e2watch', con=connection, if_exists='append')


def main(db_uri):
    ec = E2WatchCrawler(db_uri)
    ec.create_table()
    buildings = ec.get_all_buildings()
    ec.feed(buildings)


if __name__ == '__main__':
    logging.basicConfig(filename='e2watch.log', encoding='utf-8', level=logging.INFO)
    # db_uri = 'sqlite:///./data/eview.db'
    db_uri = f'postgresql://opendata:opendata@10.13.10.41:5432/e2watch'
    main(db_uri)
