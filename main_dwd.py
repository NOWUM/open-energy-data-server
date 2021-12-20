import numpy as np
import geopandas as gpd
import pandas as pd

from crawler.dwd import OpenDWDCrawler
import logging
import os
from sqlalchemy import create_engine
import multiprocessing as mp
from crawler.nut_mapper import create_nuts_map

logging.basicConfig()

user = os.getenv('TIMESCALEDB_USER', 'opendata')
password = os.getenv('TIMESCALEDB_PASSWORD', 'opendata')
database = os.getenv('TIMESCALEDB_DATABASE', 'weather')
host = os.getenv('TIMESCALEDB_HOST', '10.13.10.41')
port = int(os.getenv('TIMESCALEDB_PORT', 5432))


def collect_data(start, end):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        crawler = OpenDWDCrawler(engine, folder=f'./grb_files{start}', create_database=False)
        crawler.write_weather_in_timescale(start, end)
    except Exception as e:
        print(repr(e))
        logging.exception(f'Error in worker with interval {start} - {end}')


if __name__ == "__main__":

    max_processes = mp.cpu_count() - 1
    dwd_latitude_range = np.load(r'./crawler/data/lat_coordinates.npy')
    dwd_longitude_range = np.load(r'./crawler/data/lon_coordinates.npy')

    with mp.Pool(max_processes) as pool:
        result = pool.map(create_nuts_map, [(i, j) for i in range(824) for j in range(848)])

    result = np.asarray(result).reshape((824, 848))
    np.save('./nuts_matrix', result)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    crawler = OpenDWDCrawler(engine, create_database=True)

    processes = []
    for year in range(1995, 2019):
        process = mp.Process(target=collect_data, args=([f'{year}01', f'{year}12']))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

