import numpy as np
import geopandas as gpd
import pandas as pd

from crawler.dwd import write_data as download_data, create_table
import logging
import os
from sqlalchemy import create_engine
import multiprocessing as mp
from crawler.nut_mapper import create_nuts_map

logging.basicConfig()


def collect_data(start, end):
    try:
        download_data(start, end)
    except Exception as e:
        print(repr(e))
        logging.exception(f'Error in worker with interval {start} - {end}')


if __name__ == "__main__":

    create_nut_matrix = False

    if create_nut_matrix:
        max_processes = mp.cpu_count() - 1
        dwd_latitude_range = np.load(r'./crawler/data/lat_coordinates.npy')
        dwd_longitude_range = np.load(r'./crawler/data/lon_coordinates.npy')

        with mp.Pool(max_processes) as pool:
            result = pool.map(create_nuts_map, [(i, j) for i in range(824) for j in range(848)])

        result = np.asarray(result).reshape((824, 848))
        np.save('./nuts_matrix', result)

    create_table()

    processes = []
    for year in range(1995, 2019):
        process = mp.Process(target=collect_data, args=([f'{year}01', f'{year}12']))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()
