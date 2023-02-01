import pandas as pd
import requests
import logging
import json
from bs4 import BeautifulSoup
from datetime import date
from crawler.base_crawler import BasicDbCrawler

log = logging.getLogger('e2watch')
log.setLevel(logging.INFO)
default_start_date = '01.01.2019'


class E2WatchCrawler(BasicDbCrawler):

    def get_all_buildings(self):
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
        return df

    def get_data_per_building(self, buildings: pd.DataFrame, start_date:str):
        energy = ['strom', 'wasser', 'waerme']
        end_date = date.today().strftime('%d.%m.%Y')

        for building_id in buildings.index.values:
            df_last = pd.DataFrame([])
            for measurement in energy:
                url = f'https://stadt-aachen.e2watch.de/gebaeude/getMainChartData/{building_id}?medium={measurement}&from={start_date}&to={end_date}&type=stundenverbrauch'
                log.info(url)
                response = requests.get(url)
                data = json.loads(response.text)
                timeseries = pd.DataFrame.from_dict(data['result']['series'][0]['data'])
                if timeseries.empty:
                    log.info(f'Received empty data for building: {building_id}')
                    continue
                timeseries[0] = pd.to_datetime(timeseries[0], unit='ms')
                timeseries.columns = ['timestamp', measurement + '_kWh' if (measurement == 'strom' or measurement == 'waerme') else measurement + '_m3']
                temperature = pd.DataFrame.from_dict(data['result']['series'][1]['data'])
                if temperature.empty:
                    log.info(f'Received empty temperature for building: {building_id}')
                    continue
                temperature[0] = pd.to_datetime(temperature[0], unit='ms')
                temperature.columns = ['timestamp', 'temperatur']
                timeseries = pd.merge(timeseries, temperature, on=['timestamp'])

                if not df_last.empty:
                    df_last = pd.merge(timeseries, df_last, on=['timestamp', 'temperatur'])

                else:
                    df_last = timeseries
            log.info(df_last)

            if not df_last.empty:
                df_last.insert(0, 'building_id', building_id)
            yield df_last

    def select_latest(self):
        day = default_start_date
        today = date.today().strftime('%d.%m.%Y')
        sql = f"select timestamp from e2watch where timestamp > '{day}' and timestamp < '{today}' order by timestamp desc limit 1"
        with self.db_accessor() as connection:
            try:
                return pd.read_sql(sql,connection, parse_dates=['timestamp']).values[0][0]
            except Exception as e:
                log.error(e)
                return default_start_date


    def feed(self, buildings: pd.DataFrame, start_date:str):
        with self.db_accessor() as connection:
            buildings.to_sql('buildings', con=connection, if_exists='append')
            li = []
        for data_for_building in self.get_data_per_building(buildings, start_date):
            if data_for_building.empty:
                continue
            with self.db_accessor() as connection:
                data_for_building.to_sql('e2watch', con=connection, if_exists='append')


    def create_hypertable(self):
        try:
            query_create_hypertable = "SELECT create_hypertable('e2watch', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);"
            with self.db_accessor() as conn:
                conn.execute(query_create_hypertable)
            log.info(f'created hypertable e2watch')
        except Exception as e:
            log.error(f'could not create hypertable: {e}')

def main(db_uri):
    ec = E2WatchCrawler(db_uri)
    begin_date = ec.select_latest()
    buildings = ec.get_all_buildings()
    ec.feed(buildings, begin_date)
    ec.create_hypertable()

if __name__ == '__main__':
    logging.basicConfig()
    #db_uri = 'sqlite:///./data/eview.db'
    db_uri = f'postgresql://opendata:opendata@10.13.10.41:5432/e2watch'
    log.info(f'connect to {db_uri}')
    ec = E2WatchCrawler(db_uri)
    begin_date = default_start_date
    buildings = ec.get_all_buildings()
    ec.feed(buildings, begin_date)

