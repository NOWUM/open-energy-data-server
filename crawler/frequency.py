import time
import requests
import pandas as pd
import sqlite3

import requests
import io
import zipfile

from crawler.base_crawler import BasicDbCrawler

import logging

log = logging.getLogger('frequency')
log.setLevel(logging.INFO)

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


class FrequencyCrawler(BasicDbCrawler):
    def crawl_year_by_url(self, url):
        for name, thefile, count in download_extract_zip(url):
                log.info(name)
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
                try:
                    with self.db_accessor() as conn:
                        df.to_sql('frequency', conn, if_exists='append')
                except Exception as e:
                    log.error(f'Error: {e}')

    def crawl_frequency(self, first=2011, last=2020):
        for year in range(first, last+1):
            log.info(year)
            url = f'https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip'
            crawl_year_by_url(url)

            

def main(db_uri):
    fc = FrequencyCrawler(db_uri)
    fc.crawl_frequency(first=2014)

if __name__ == '__main__':
    logging.basicConfig()
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

    conn_uri = 'sqlite:///frequency.db'
    log.info(f'connect to {conn_uri}')
    fc = FrequencyCrawler(conn_uri)
    #fc.crawl_frequency(first=2014)
    year=2015
    url = f'https://www.50hertz.com/Portals/1/Dokumente/Transparenz/Regelenergie/Archiv%20Netzfrequenz/Netzfrequenz%20{year}.zip'
    fc.crawl_year_by_url(url)
