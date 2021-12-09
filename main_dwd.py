from crawler.dwd import OpenDWDCrawler
import logging
import os
from sqlalchemy import create_engine
import multiprocessing

logging.basicConfig()


def worker(start, end):
    try:
        user = os.getenv('TIMESCALEDB_USER', 'opendata')
        password = os.getenv('TIMESCALEDB_PASSWORD', 'opendata')
        database = os.getenv('TIMESCALEDB_DATABASE', 'weather')
        host = os.getenv('TIMESCALEDB_HOST', '10.13.10.41')
        port = int(os.getenv('TIMESCALEDB_PORT', 5432))

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        crawler = OpenDWDCrawler(engine, folder=f'./grb_files{start}')
        crawler.write_weather_in_timescale(start, end)

    except Exception:
        logging.getLogger().exception()


if __name__ == "__main__":

    if False:
        worker(start=os.getenv('START_DATE', '199501'),
               end=os.getenv('END_DATE', '201905'))
    else:
        procs = []
        for year in range(1995, 2019):
            t = multiprocessing.Process(target=worker, args=([f'{year}01',f'{year}12']))
            procs.append(t)
            t.start()

        for t in procs:
            t.join()