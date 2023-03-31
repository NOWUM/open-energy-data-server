import pandas as pd
import requests
import logging
import json
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
from crawler.base_crawler import BasicDbCrawler

log = logging.getLogger('e2watch')
default_start_date = '01.01.2023'


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

        response = requests.get('https://stadt-aachen.e2watch.de/')
        html = response.content
        data = BeautifulSoup(html, 'html.parser')
        all_buildings = data.findAll("div", {"class": "entity-list-container"})
        building_id = []
        lat = []
        lon = []
        beschreibung = []
        for i in all_buildings:
            list_buildings = i.findAll('li')
            for x in list_buildings:
                building_id.append(x.find('a')['id'])
                lat.append(x.find('a')['lat'])
                lon.append(x.find('a')['lng'])
                beschreibung.append(x.find('a')['data-original-title'])

        df = pd.DataFrame([])
        df['building_id'] = building_id
        df['lat'] = lat
        df['lon'] = lon
        df['beschreibung'] = beschreibung
        tags = df['beschreibung'].str.split('br /> ', expand=True)
        tags[0] = tags[0].replace('(<b>)|(<\/b>)|<', '', regex=True).astype(str)
        plz = tags[1].str.split(', ', expand=True)
        df['beschreibung'] = tags[0]
        df['strasse'] = plz[0]
        df['plz'] = plz[1]
        df['stadt'] = plz[2]
        df['lat'] = df['lat'].astype(float)
        df['lon'] = df['lon'].astype(float)
        df = df.set_index(['building_id'])

        bid = []

        for building_id in df.index.values:
            log.info(f'Doing building {building_id}')
            response = requests.get(f'https://stadt-aachen.e2watch.de/details/objekt/{building_id}')
            html = response.content
            data = BeautifulSoup(html, 'html.parser')
            all_buildings = data.findAll("div", {"class": "container main-chart"})
            list_buildings = all_buildings[0].findAll('li')
            bid.append(list_buildings[0].find('a')['bid'])

        df['bilanzkreis_id'] = bid
        df['building_id'] = df.index
        df = df.set_index(['bilanzkreis_id'])
        return df

    def get_data_per_building(self, buildings: pd.DataFrame, start_date: str):
        energy = ['strom', 'wasser', 'waerme']
        end_date = date.today().strftime('%d.%m.%Y')

        for bilanzkreis_id in buildings.index.values:
            df_last = pd.DataFrame([])
            for measurement in energy:
                url = f'https://stadt-aachen.e2watch.de/gebaeude/getMainChartData/{bilanzkreis_id}?medium={measurement}&from={start_date}&to={end_date}&type=stundenverbrauch'
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

    def select_latest(self):
        # day = default_start_date
        # today = date.today().strftime('%d.%m.%Y')
        # sql = f"select timestamp from e2watch where timestamp > '{day}' and timestamp < '{today}' order by timestamp desc limit 1"
        sql = f"select timestamp from e2watch order by timestamp desc limit 1"
        with self.db_accessor() as connection:
            try:
                latest = pd.read_sql(sql, connection, parse_dates=['timestamp']).values[0][0]
                latest = latest.astype(datetime)
                latest = pd.to_datetime(latest, unit='ns')
                log.info(f'The latest date in the database is {latest.strftime("%d.%m.%Y %H:%M:%S")}')
                latest = latest + timedelta(hours=1)
                latest = latest.strftime("%d.%m.%Y %H:%M:%S")
                log.info(f'Next date to crawl is {latest}')
                return latest
            except Exception as e:
                log.info(f'Using the default start date {e}')
                return default_start_date

    def feed(self, buildings: pd.DataFrame, start_date: str):
        sql = f"select * from buildings"
        with self.db_accessor() as connection:
            try:
                building_data = pd.read_sql(sql, connection, parse_dates=['timestamp'])
                if len(building_data) == 0:
                    log.info(f'creating new buildings table')
                    buildings.to_sql('buildings', con=connection, if_exists='append')
            except Exception as e:
                log.info(f'Probably no database connection: {e}')
        for data_for_building in self.get_data_per_building(buildings, start_date):
            if data_for_building.empty:
                continue
            with self.db_accessor() as connection:
                data_for_building = data_for_building.set_index(['timestamp', 'bilanzkreis_id'])
                # check if timestamp < start_date
                if data_for_building.index.get_level_values('timestamp')[0] < pd.to_datetime(start_date, utc=True):
                    # drop that row
                    log.info(f'Dropping row {data_for_building.index.get_level_values("timestamp")[0]}')
                    data_for_building = data_for_building.drop(data_for_building.index[0])
                log.info(data_for_building)
                data_for_building.to_sql('e2watch', con=connection, if_exists='append')


def main(db_uri):
    ec = E2WatchCrawler(db_uri)
    ec.create_table()
    begin_date = ec.select_latest()
    buildings = ec.get_all_buildings()
    ec.feed(buildings, begin_date)


if __name__ == '__main__':
    logging.basicConfig(filename='e2watch.log', encoding='utf-8', level=logging.INFO)
    # db_uri = 'sqlite:///./data/eview.db'
    db_uri = f'postgresql://opendata:opendata@10.13.10.41:5432/e2watch'
    main(db_uri)
