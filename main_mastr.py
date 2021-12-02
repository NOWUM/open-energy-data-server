from crawler.MaStR_Crawler import init_database, create_db_from_export
import os
import time
from sqlalchemy import create_engine
import logging

logging.basicConfig()

if __name__ == "__main__":

    host = os.getenv('TIMESCALEDB_HOST', '10.13.10.41')
    port = int(os.getenv('TIMESCALEDB_PORT', 5432))
    user = os.getenv('TIMESCALEDB_USER', 'opendata')
    password = os.getenv('TIMESCALEDB_PASSWORD', 'opendata')
    database = os.getenv('TIMESCALEDB_DATABASE', 'mastr')

    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}')
        init_database(connection=engine, database=database)
        engine.dispose()

        while True:
            try:
                engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
                create_db_from_export(connection=engine)
                engine.dispose()
                time.sleep(2 * (60 * 60 * 24))
            except Exception as e:
                time.sleep(300)
                print(e)
    except Exception as e:
        print(e)





