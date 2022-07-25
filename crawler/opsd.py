import pandas as pd
import os.path as osp
import sqlite3
import requests
import logging
import os.path as osp
log = logging.getLogger('opsd')
log.setLevel(logging.INFO)

when2heat_path = osp.join(osp.dirname(__file__), 'data/when2heat.db')
when2heat_url = 'https://data.open-power-system-data.org/when2heat/latest/when2heat.sqlite'

def write_when2_heat(engine, db_path = when2heat_path):
    '''
    efficiency of heat pumps in different countries for different types of heatpumps
    '''
    if osp.isfile(db_path):
        log.info(f'{db_path} already exists')
    else:
        when2heat_file = requests.get(when2heat_url)
        with open(when2heat_path, 'wb') as f:
            f.write(when2heat_file.content)
        log.info(f'downloaded when2heat.db to {db_path}')

    conn = sqlite3.connect(when2heat_path)

    data = pd.read_sql('select * from when2heat', conn)
    data.index = pd.to_datetime(data['utc_timestamp'], infer_datetime_format=True)
    data['cet_cest_timestamp'] = pd.to_datetime(data['cet_cest_timestamp'], infer_datetime_format=True)
    del data['utc_timestamp']
    log.info('data read successfully')
    
    data.to_sql('when2heat', engine, if_exists='replace')
    log.info('data written successfully')


def main(db_uri):
    from sqlalchemy import create_engine
    engine = create_engine(db_uri)
    write_when2_heat(engine)


if __name__ == "__main__":
    logging.basicConfig()
    import os
    host = os.getenv('HOST', '10.13.10.41')
    port = int(os.getenv('PORT', 5432))
    user = os.getenv('DB_USER', 'opendata')
    password = os.getenv('PASSWORD', 'opendata')
    database = os.getenv('TIMESCALEDB_DATABASE', 'opsd')

    db_uri = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    main(db_uri)
