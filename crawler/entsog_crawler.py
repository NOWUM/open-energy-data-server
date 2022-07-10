#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 13 12:04:45 2020

@author: maurer

This crawler downloads all the data of the ENTSO-G transparency platform.
The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.
"""

import requests
import urllib

import time
from datetime import date, timedelta
import pandas as pd
from tqdm import tqdm

import sqlite3
from contextlib import closing

from sqlalchemy import create_engine
from contextlib import contextmanager

import logging

from .base_crawler import BasicDbCrawler

logging.basicConfig()
log = logging.getLogger('entsog')
log.setLevel(logging.INFO)

api_endpoint = 'https://transparency.entsog.eu/api/v1/'

fr = date(2020, 5, 18)
to = date.today()

'''
data = pd.read_csv(
    f'{api_endpoint}operationaldata.csv?limit=1000&indicator=Allocation&from={fr}&to={to}')
response = requests.get(api_endpoint+'operationaldatas')
data = pd.read_csv(api_endpoint+'AggregatedData.csv?limit=1000')
response = requests.get(api_endpoint+'AggregatedData?limit=1000')
data = pd.DataFrame(response.json()['AggregatedData'])
'''

def getDataFrame(name, params=['limit=10000'], useJson=False):
    params_str = ''
    if len(params) > 0:
        params_str = '?'
    for param in params[:-1]:
        params_str = params_str+param+'&'
    params_str += params[-1]

    i = 0
    data = pd.DataFrame()
    success = False
    while i < 10 and not success:
        try:
            i += 1
            if useJson:
                url = f'{api_endpoint}{name}.json{params_str}'
                response = requests.get(url)
                data = pd.DataFrame(response.json()[name])
                # replace empty string with None
                data = data.replace([''], [None])
            else:
                url = f'{api_endpoint}{name}.csv{params_str}'
                data = pd.read_csv(url, index_col=False)
            success = True
        except requests.exceptions.InvalidURL as e:
            raise e
        except requests.exceptions.HTTPError as e:
            log.error('Error getting Dataframe')
            if e.response.status_code >= 500 :
                log.info(f'{e.response.reason} - waiting 30 seconds..')
                time.sleep(30)
        except urllib.error.HTTPError as e:
            log.error(f'Error getting Dataframe')
            if e.code >= 500 :
                log.info(f'{e.msg} - waiting 30 seconds..')
                time.sleep(30)

    if data.empty:
        raise Exception('could not get any data for params:', params_str)
    data.columns = [x.lower() for x in data.columns]
    return data

class EntsogCrawler(BasicDbCrawler):
    def pullData(self, names):
        pbar = tqdm(names)
        for name in pbar:
            try:
                pbar.set_description(name)
                # use Json as connectionpoints have weird csv
                # TODO Json somehow has different data
                # connectionpoints count differ
                # and tpTSO column are named tSO in connpointdirections
                data = getDataFrame(name, useJson=True)

                with self.db_accessor() as conn:
                    tbl_name = name.lower().replace(' ','_')
                    data.to_sql(tbl_name, conn, if_exists='replace')

            except Exception:
                log.exception('error pulling data')

        if 'operatorpointdirections' in names:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opd" ON operatorpointdirections (operatorKey, pointKey,directionkey);')
                conn.execute(query)

    def findNewBegin(self, table_name):
        try:
            with self.db_accessor() as conn:
                query = f'select max(periodfrom) from {table_name}'
                d = conn.execute(query).fetchone()[0]
            begin = pd.to_datetime(d).date()
        except Exception as e:
            begin = date(2017, 7, 10)
            log.error(f'table does not exist yet - using default start {begin} ({e})')
        return begin

    def pullOperationalData(self, indicators, initial_begin=None, end=None):
        log.info('getting values from operationaldata')
        if not end:
            end = date.today()

        for indicator in indicators:
            tbl_name = indicator.lower().replace(' ','_')
            if initial_begin:
                begin = initial_begin
            else:
                begin = self.findNewBegin(tbl_name)

            bulks = (end-begin).days
            log.info(f'start: {begin}, end: {end}, days: {bulks}, indicator: {indicator}')

            if bulks < 1:
                return
            delta = timedelta(days=1)

            pbar = tqdm(range(int(bulks)))
            for i in pbar:
                beg1 = begin + i*delta
                end1 = begin + (i+1)*delta
                pbar.set_description(f'op {beg1} to {end1}')

                params = ['limit=-1', 'indicator='+urllib.parse.quote(indicator), 'from=' +
                      str(beg1), 'to='+str(end1), 'periodType=hour']
                time.sleep(5)
                # impact of sleeping here is quite small in comparison to 50s query length
                # rate limiting Gateway Timeouts
                df = getDataFrame('operationaldata', params)
                df['periodfrom'] = pd.to_datetime(df['periodfrom'], infer_datetime_format=True)
                df['periodto'] = pd.to_datetime(df['periodto'], infer_datetime_format=True)

                try:
                    with self.db_accessor() as conn:
                        df.to_sql(tbl_name, conn, if_exists='append')
                except Exception as e:
                    # allow adding a new column or converting type
                    with self.db_accessor() as conn:
                        log.info(f'handling {repr(e)} by concat')
                        # merge old data with new data
                        prev = pd.read_sql_query(
                            f'select * from {tbl_name}', conn)
                        dat = pd.concat([prev, df])
                        # convert type as pandas needs it
                        dat.to_sql(tbl_name, conn, if_exists='replace')
                        log.info(f'replaced table {tbl_name}')

            try:
                with self.db_accessor() as conn:
                    query_create_hypertable = f"SELECT create_hypertable('{tbl_name}', 'periodfrom', if_not_exists => TRUE, migrate_data => TRUE);"
                    conn.execute(query_create_hypertable)
                    log.info(f'created hypertable {tbl_name}')
            except Exception as e:
                log.error(f'could not create hypertable {tbl_name}: {e}')

        # sqlite will only use one index. EXPLAIN QUERY PLAIN shows if index is used
        # ref: https://www.sqlite.org/optoverview.html#or_optimizations
        # reference https://stackoverflow.com/questions/31031561/sqlite-query-to-get-the-closest-datetime
        if 'Allocation' in indicators:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_opdata" ON Allocation (operatorKey,periodfrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_pointKey" ON Allocation (pointKey,periodfrom);')
                conn.execute(query)
        if 'Physical Flow' in indicators:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_operator" ON Physical_Flow (operatorKey,periodfrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_phys_point" ON Physical_Flow (pointKey,periodfrom);')
                conn.execute(query)

        if 'Firm Technical' in indicators:
            with self.db_accessor() as conn:
                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_opdata" ON Firm_Technical (operatorKey,periodfrom);')
                conn.execute(query)

                query = (
                    'CREATE INDEX IF NOT EXISTS "idx_ft_pointKey" ON Firm_Technical (pointKey,periodfrom);')
                conn.execute(query)

def main(db_uri):
    crawler = EntsogCrawler(db_uri)

    names = ['cmpUnsuccessfulRequests',
            'connectionpoints',
            'operators',
            'balancingzones',
            'operatorpointdirections',
            'Interconnections',
            'aggregateInterconnections']
    crawler.pullData(names)

    indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
    crawler.pullOperationalData(indicators)

if __name__ == "__main__":
    database = 'data/entsog.db'
    import os
    craw = EntsogCrawler(database)

    names = ['cmpUnsuccessfulRequests',
             # 'operationaldata',
             # 'cmpUnavailables',
             # 'cmpAuctions',
             # 'AggregatedData', # operationaldata aggregated for each zone
             # 'tariffssimulations',
             # 'tariffsfulls',
             # 'urgentmarketmessages',
             'connectionpoints',
             'operators',
             'balancingzones',
             'operatorpointdirections',
             'Interconnections',
             'aggregateInterconnections']

    craw.pullData(names)

    indicators = ['Physical Flow', 'Allocation', 'Firm Technical']
    craw.pullOperationalData(indicators)
