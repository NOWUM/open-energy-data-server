import time
import requests
import pandas as pd
import sqlite3

import requests
import io
import zipfile


def download_extract_zip(url):
    """
    Download a ZIP file and extract its contents in memory
    yields (filename, file-like object) pairs
    """
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
        for zipinfo in thezip.infolist():
            with thezip.open(zipinfo) as thefile:
                yield zipinfo.filename, thefile, len(thezip.infolist())


def crawl_frequency(db_conn, first=2011, last=2020):
    for year in range(first, last+1):
        print(year)
        url = f'https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip'

        for name, thefile, count in download_extract_zip(url):
            print(name)
            if count == 1:  # only 2010
                df = pd.read_csv(thefile, sep=';', decimal=",", header=None,
                                 names=['date_time', 'frequency'],
                                 # index_col='date',
                                 # parse_dates=['date_time'], infer_datetime_format=True
                                 )
                df.index = pd.to_datetime(
                    df['date_time'], format='%d.%m.%Y %H:%M:%S')

                del df['date_time']
            else:
                df = pd.read_csv(thefile, sep=',', header=None,
                                 parse_dates=[[0, 1]],
                                 infer_datetime_format=True
                                 )
                if len(df.columns) == 3:
                    del df[2]
                df.columns = ['date_time', 'frequency']
                df.set_index('date_time')
            df.to_sql('frequency', db_conn, if_exists='append')


if __name__ == '__main__':
    if False:
        year = 2010
        thefile = 'Netzfrequenz 2019/201901_Frequenz.csv'
        thefile = 'Netzfrequenz 2011/201101_Frequenz.txt'
        thefile = 'Netzfrequenz 2010/Frequenz2010.csv'
        # try parsing 2010 csv files
        conn = sqlite3.connect('freq.db')
        crawl_frequency(conn)

        import matplotlib.pyplot as plt
        sql = 'select date_time, frequency from frequency where date_time>2019-01-01'
        df = pd.read_sql(sql, conn)
        plt.plot(df['date_time'], df['frequency'])

    from sqlalchemy import create_engine
    import os
    user=os.getenv('TIMESCALEDB_USER', 'opendata'),
    password=os.getenv('TIMESCALEDB_PASSWORD', 'opendata'),
    database=os.getenv('TIMESCALEDB_DATABASE', 'frequency'),
    host=os.getenv('TIMESCALEDB_HOST', '10.13.10.41'),
    port=int(os.getenv('TIMESCALEDB_PORT', 5432))
    create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')