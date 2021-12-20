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


def collect_data(start, end):
    try:
        user = os.getenv('TIMESCALEDB_USER', 'opendata')
        password = os.getenv('TIMESCALEDB_PASSWORD', 'opendata')
        database = os.getenv('TIMESCALEDB_DATABASE', 'weather')
        host = os.getenv('TIMESCALEDB_HOST', '10.13.10.41')
        port = int(os.getenv('TIMESCALEDB_PORT', 5432))

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        crawler = OpenDWDCrawler(engine, folder=f'./grb_files{start}')
        crawler.write_weather_in_timescale(start, end)

    except Exception as e:
        print(repr(e))
        logging.exception(f'Error in worker with interval {start} - {end}')



if __name__ == "__main__":

    geo_information = gpd.read_file('./shapes/NUTS_EU.shp')
    geo_information = geo_information.to_crs(4326)
    nut_levels = {
        'DE': 3,
        'NL': 1,
        'BE': 1,
        'LU': 1,
        'PO': 1,
        'DK': 1,
        'FR': 1,
        'CZ': 1,
        'AT': 1,
        'CH': 1
    }
    data_frames = []
    for key, value in nut_levels.items():
        df = geo_information[(geo_information['CNTR_CODE'] == key) &
                             (geo_information['LEVL_CODE'] == value)]
        data_frames.append(df)

    geo_information = gpd.GeoDataFrame(pd.concat(data_frames))

    max_processes = mp.cpu_count() - 1
    dwd_latitude_range = np.load(r'./crawler/data/lat_coordinates.npy').reshape((-1,))
    dwd_longitude_range = np.load(r'./crawler/data/lon_coordinates.npy').reshape((-1))

    dwd_latitude_range = np.array_split(dwd_latitude_range, max_processes)
    dwd_longitude_range = np.array_split(dwd_longitude_range, max_processes)

    coordinates = []
    for i in range(max_processes):
        coordinates.append((dwd_longitude_range[i], dwd_latitude_range[i], geo_information))

    with mp.Pool(max_processes) as pool:
        result = pool.map(create_nuts_map, coordinates)

    result = np.asarray(result).reshape((-1,))
    np.save('./nuts_matrix', result)

    processes = []
    for year in range(1995, 2019):
        process = mp.Process(target=collect_data, args=([f'{year}01', f'{year}12']))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

