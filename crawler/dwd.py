
import requests
import bz2
import pygrib
import pandas as pd
import numpy as np
import logging
import os
import os.path as osp
from tqdm import tqdm
from sqlalchemy import create_engine

import multiprocessing as mp
from .nuts_mapper import create_nuts_map

log = logging.getLogger('openDWD_cosmo')
log.setLevel(logging.INFO)

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
from .base_crawler import BasicDbCrawler

class DWDCrawler(BasicDbCrawler):

    def __init__(self, nuts_matrix, download_dir, database):
        super().__init__(database)
        self.nuts_matrix = nuts_matrix
        self.download_dir = download_dir
        nuts = np.unique(nuts_matrix[[nuts_matrix != 'x']].reshape((-1)))
        self.countries = np.asarray([area[:2] for area in nuts])
        self.values = np.zeros_like(nuts)

    def create_table(self):
        with self.db_accessor() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS cosmo( "
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

        try:
            query_create_hypertable = "SELECT create_hypertable('cosmo', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
            with self.db_accessor() as conn:
                conn.execute(query_create_hypertable)
            log.info(f'created hypertable cosmo')
        except Exception as e:
            log.error(f'could not create hypertable: {e}')

    def _download_data(self, key, year, month):
        response = requests.get(f'{base_url}{to_download[key]}{year}{month}.grb.bz2')
        log.info(f'get weather for {key} with status code {response.status_code}')

        weather_data = bz2.decompress(response.content)
        try:
            os.makedirs(self.download_dir)
        except FileExistsError:
            # directory already exists
            pass

        with open(f'{self.download_dir}/weather{year}{month}', 'wb') as file:
            file.write(weather_data)


    def _delete_data(self, year, month):
        if os.path.isfile(f'{self.download_dir}/weather{year}{month}'):
            os.remove(f'{self.download_dir}/weather{year}{month}')

    def _create_dataframe(self, key, year, month):
        weather_data = pygrib.open(f'{self.download_dir}/weather{year}{month}')
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


    def write_data(self, start, end):
        date_range = pd.date_range(start=pd.to_datetime(start, format='%Y%m'),
                                end=pd.to_datetime(end, format='%Y%m'),
                                freq='MS')
        with self.db_accessor() as connection:
            for date in tqdm(date_range):
                try:
                    df = pd.DataFrame(columns=[key for key in to_download.keys()])
                    for key in to_download.keys():
                        self._download_data(key, str(date.year), f'{date.month:02d}')
                        data = self._create_dataframe(key, str(date.year), f'{date.month:02d}')
                        df['time'] = data['time']
                        df['nuts'] = data['nuts']
                        df[key] = data[key]
                        self._delete_data(str(date.year), f'{date.month:02d}')
                    df['country'] = [area[:2] for area in df['nuts'].values]
                    index = pd.MultiIndex.from_arrays([df['time'], df['nuts']], names=['time', 'nuts'])
                    df.index = index
                    del df['time'], df['nuts']
                    log.info(f'built data for  {date.month_name()} and start import to db')
                    df.to_sql('cosmo', con=connection, if_exists='append')
                    log.info('import in db complete --> start with next hour')
                except Exception as e:
                    log.error(repr(e))
                    log.exception(f'could not read {date}')

def create_nuts_matrix(nuts_matrix_path):
    max_processes = mp.cpu_count() - 1
    log.info('(re)creating nuts matrix')

    with mp.Pool(max_processes) as pool:
        result = pool.map(create_nuts_map, [(i, j) for i in range(824) for j in range(848)])

    result = np.asarray(result).reshape((824, 848))
    np.save(nuts_matrix_path, result)
    log.info(f'created nuts matrix at {nuts_matrix_path}')

def main(db_uri):
    nuts_matrix_path = osp.join(osp.dirname(__file__),'data','nuts_matrix.npy')
    if not os.path.isfile(nuts_matrix_path):
        create_nuts_matrix(nuts_matrix_path)

    nuts_matrix = np.load(nuts_matrix_path, allow_pickle=True)
    download_dir = osp.join(osp.dirname(__file__),'grb_files')

    crawler = DWDCrawler(nuts_matrix, download_dir, db_uri)
    crawler.create_table()
    crawler.write_data('199501', '199502')


if __name__ == '__main__':
    import numpy as np

    logging.basicConfig()

    nuts_matrix_path = osp.join(osp.dirname(__file__),'data','nuts_matrix.npy')

    if not os.path.isfile(nuts_matrix_path):
        create_nuts_matrix(nuts_matrix_path)
    nuts_matrix = np.load(nuts_matrix_path, allow_pickle=True)
    db_uri = 'postgresql://opendata:opendata@10.13.10.41:5432/weather'
    db_uri = 'sqlite://weather.db'
    download_dir = osp.join(osp.dirname(__file__),'grb_files')

    crawler = DWDCrawler(nuts_matrix, download_dir, db_uri)
    crawler.create_table()
    #crawler.write_data('199501', '199502')

    def collect_data(start, end):
        try:
            log.info(f'started downloading for {start} to {end}')
            crawler._download_data(start, end)
            log.info(f'finished downloading for {start} to {end}')
        except Exception as e:
            log.error(repr(e))
            log.exception(f'Error in worker with interval {start} - {end}')

    processes = []
    for year in range(1995, 2019):
        process = mp.Process(target=collect_data, args=([f'{year}01', f'{year}12']))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

