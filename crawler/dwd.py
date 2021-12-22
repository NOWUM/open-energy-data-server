import time
import requests
import bz2
import pygrib
import pandas as pd
import numpy as np
import logging
import os
import sqlite3
from tqdm import tqdm

log = logging.getLogger('openDWD_cosmo')
log.setLevel(logging.INFO)


class OpenDWDCrawler:

    def __init__(self, engine, folder='./grb_files', create_database=True):
        log.info('initialize dwd cosmo crawler')

        # base url and parameters to get the weather data from open dwd cosmo model
        self.base_url = 'https://opendata.dwd.de/climate_environment/REA/COSMO_REA6/hourly/2D/'
        self.codecs = dict(temp_air='T_2M/T_2M.2D.',
                           ghi='ASOB_S/ASOB_S.2D.',
                           dni='ASWDIFD_S/ASWDIFD_S.2D.',
                           dhi='ASWDIR_S/ASWDIR_S.2D.',
                           wind_meridional='V_10M/V_10M.2D.',
                           wind_zonal='U_10M/U_10M.2D.',
                           rain_con='RAIN_CON/RAIN_CON.2D.',
                           rain_gsp='RAIN_GSP/RAIN_GSP.2D.',
                           cloud_cover='CLCT/CLCT.2D.')

        log.info(f'collect: {self.codecs.keys()}')

        self.engine = engine
        if create_database and type(self.engine) != sqlite3.Connection:
            self.engine.execute("CREATE TABLE IF NOT EXISTS public.cosmo( "\
                                "time timestamp without time zone NOT NULL, "\
                                "nuts text, "\
                                "temp_air double precision, "\
                                "ghi double precision, " \
                                "dni double precision, "\
                                "dhi double precision, "\
                                "wind_meridional double precision, "\
                                "wind_zonal double precision, "\
                                "rain_con double precision,"\
                                "rain_gsp double precision, "\
                                "cloud_cover double precision, "\
                                "PRIMARY KEY (time , nuts));")

        query_create_hypertable = "SELECT create_hypertable('cosmo', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
        with self.engine.connect() as connection:
            with connection.begin():
                connection.execute(query_create_hypertable)

        self.weather_file_name = 'dwd.grb'
        self.folder = folder

        if not os.path.exists(self.folder):
            os.makedirs(self.folder)

        self.nuts_matrix = np.load(r'./nuts_matrix.npy', allow_pickle=True)

        log.info('crawler initialized')

    def __del__(self):
        for key in self.codecs.keys():
            try:
                file_name = f'{self.folder}/{key}_{self.weather_file_name}'
                if os.path.isfile(file_name):
                    os.remove(file_name)
            except Exception:
                log.error(f'error cleaning up file {file_name}')
        self.engine.dispose()

    def save_data_in_file(self, typ='temp_air', year='1995', month='01'):
        # get data of type for year and month
        for i in range(1, 4):
            try:
                response = requests.get(f'{self.base_url}{self.codecs[typ]}{year}{month}.grb.bz2')
                break
            except Exception as e:
                print(repr(e))
                time.sleep(i**2)

        log.info(f'get weather for {typ} with status code {response.status_code}')

        # unzip an save data in file (parameter_weather.grb)
        weather_data = bz2.decompress(response.content)
        file_name = f'{self.folder}/{typ}_{self.weather_file_name}'
        with open(file_name, 'wb') as file:
            file.write(weather_data)

        log.info(f'file {file_name} saved')

    def read_data_file(self, date, typ):
        # load dwd file with typ
        file_name = f'{self.folder}/{typ}_{self.weather_file_name}'
        weather_data = pygrib.open(file_name)
        # extract the selector to get the correct parameter
        selector = str(weather_data.readline()).split('1:')[1].split(':')[0]
        # build dataframe and write data for each hour in month
        hours = pd.date_range(start=pd.to_datetime(date), end=pd.to_datetime(date) + pd.DateOffset(months=1),
                              freq='h')[:-1]
        # slice the current hour with counter
        size = len(weather_data.select(name=selector))
        data_frames = []
        for k in range(len(hours)):
            data_ = weather_data.select(name=selector)[k]
            df = pd.DataFrame()
            # build dataframe
            df[typ] = data_.values[self.nuts_matrix != 'x'].reshape((-1))
            df['nuts'] = self.nuts_matrix[[self.nuts_matrix != 'x']].reshape((-1))
            df = pd.DataFrame(df.groupby(['nuts'])[typ].mean())
            df['nuts'] = df.index
            df['country'] = [nut[:2] for nut in df['nut'].values]
            df['time'] = hours[k]

        log.info(f'read data with type: {typ} in month {date.month}')
        weather_data.close()
        log.info('closed weather file')

        return pd.concat(data_frames)

    def write_weather_in_timescale(self, start='199501', end='199502'):
        date_range = pd.date_range(start=pd.to_datetime(start, format='%Y%m'),
                                   end=pd.to_datetime(end, format='%Y%m'),
                                   freq='MS')
        for date in tqdm(date_range):
            try:
                # download files
                for key in self.codecs.keys():
                    self.save_data_in_file(year=str(date.year), month=f'{date.month:02d}', typ=key)
                df = pd.DataFrame(columns=[key for key in self.codecs.keys()])
                # create data frame for each parameter
                for key in self.codecs.keys():
                    df[key] = self.read_data_file(date, key)[key]
                    log.info('build dataset for import')

                index = pd.MultiIndex.from_arrays([df['time'], df['nuts']], names=['time', 'nuts'])
                df.index = index
                del df['time'], df['nuts']
                log.info(f'built data for  {date.month_name()} and start import to postgres')
                df.to_sql('cosmo_test', con=self.engine, if_exists='append')
                log.info('import in postgres complete --> start with next hour')

            except Exception as e:
                print(repr(e))
                log.exception(f'could not read {date}')
