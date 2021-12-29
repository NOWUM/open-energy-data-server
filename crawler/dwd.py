import requests
import bz2
import pygrib
import pandas as pd
import numpy as np
import logging
import os
from tqdm import tqdm
from sqlalchemy import create_engine


log = logging.getLogger('openDWD_cosmo')
log.setLevel(logging.INFO)

nuts_matrix = np.load(r'./nuts_matrix.npy', allow_pickle=True)
nuts = np.unique(nuts_matrix[[nuts_matrix != 'x']].reshape((-1)))
countries = np.asarray([area[:2] for area in nuts])
values = np.zeros_like(nuts)

# open dwd data
base_url = 'https://opendata.dwd.de/climate_environment/REA/COSMO_REA6/hourly/2D/'
to_download = dict(temp_air='T_2M/T_2M.2D.',
                   ghi='ASOB_S/ASOB_S.2D.',
                   dni='ASWDIFD_S/ASWDIFD_S.2D.',
                   dhi='ASWDIR_S/ASWDIR_S.2D.',
                   wind_meridional='V_10M/V_10M.2D.',
                   wind_zonal='U_10M/U_10M.2D.',
                   rain_con='RAIN_CON/RAIN_CON.2D.',
                   rain_gsp='RAIN_GSP/RAIN_GSP.2D.',
                   cloud_cover='CLCT/CLCT.2D.')

def create_table():
    engine = create_engine(f'postgresql://opendata:opendata@10.13.10.41:5432/weather')
    engine.execute("CREATE TABLE IF NOT EXISTS cosmo( "
                    "time timestamp without time zone NOT NULL, "
                    "nuts text, "
                    "country text, "
                    "temp_air double precision, "
                    "ghi double precision, "
                    "dni double precision, "
                    "dhi double precision, "
                    "wind_meridional double precision, "
                    "wind_zonal double precision, "
                    "rain_con double precision,"
                    "rain_gsp double precision, "
                    "cloud_cover double precision, "
                    "PRIMARY KEY (time , nuts));")

    query_create_hypertable = "SELECT create_hypertable('cosmo', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(query_create_hypertable)


def download_data(key, year, month):
    response = requests.get(f'{base_url}{to_download[key]}{year}{month}.grb.bz2')
    log.info(f'get weather for {key} with status code {response.status_code}')

    weather_data = bz2.decompress(response.content)
    with open(f'./grb_files/weather{year}{month}', 'wb') as file:
        file.write(weather_data)


def delete_data(year, month):
    if os.path.isfile(f'./grb_files/weather{year}{month}'):
        os.remove(f'./grb_files/weather{year}{month}')


def create_dataframe(key, year, month):
    weather_data = pygrib.open(f'./grb_files/weather{year}{month}')
    selector = str(weather_data.readline()).split('1:')[1].split(':')[0]
    size = len(weather_data.select(name=selector))

    data_frames = []
    for k in tqdm(range(size)):
        data_ = weather_data.select(name=selector)[k]
        df = pd.DataFrame(columns=[key, 'nuts'],
                          data={key: data_.values[nuts_matrix != 'x'].reshape((-1)),
                                'nuts': nuts_matrix[nuts_matrix != 'x'].reshape((-1))})
        df = pd.DataFrame(df.groupby(['nuts'])[key].mean())
        df['nuts'] = df.index
        df['time'] = pd.to_datetime(f'{year}{month}', format='%Y%m') + pd.DateOffset(hours=k)
        data_frames.append(df)

    log.info(f'read data with type: {key} in month {month} \n')
    weather_data.close()
    log.info('closed weather file \n')

    return pd.concat(data_frames, ignore_index=True)


def write_data(start, end):
    engine = create_engine(f'postgresql://opendata:opendata@10.13.10.41:5432/weather')
    date_range = pd.date_range(start=pd.to_datetime(start, format='%Y%m'),
                               end=pd.to_datetime(end, format='%Y%m'),
                               freq='MS')
    for date in tqdm(date_range):
        try:
            df = pd.DataFrame(columns=[key for key in to_download.keys()])
            for key in to_download.keys():
                download_data(key, str(date.year), f'{date.month:02d}')
                data = create_dataframe(key, str(date.year), f'{date.month:02d}')
                df['time'] = data['time']
                df['nuts'] = data['nuts']
                df[key] = data[key]
                delete_data(str(date.year), f'{date.month:02d}')
            df['country'] = [area[:2] for area in df['nuts'].values]
            index = pd.MultiIndex.from_arrays([df['time'], df['nuts']], names=['time', 'nuts'])
            df.index = index
            del df['time'], df['nuts']
            log.info(f'built data for  {date.month_name()} and start import to postgres')
            df.to_sql('cosmo', con=engine, if_exists='append')
            log.info('import in postgres complete --> start with next hour')
        except Exception as e:
            print(repr(e))
            log.exception(f'could not read {date}')
