'''
mirror all available/bought data from EEX using something like [rclone](https://rclone.org/)

Then store data in a Database to be able to query them with good performance.
Somehow datasource.eex-group.com is often quite slow.

with sshfs you can use:

`sshfs root@eex:/root/eex /mnt/eex/`
'''
import os
import os.path as osp
from glob import glob
import pandas as pd
import logging

log = logging.getLogger('eex')
log.setLevel(logging.INFO)
eex_data_path = '/mnt/eex/'

# limit files per type which are read
FIRST_X = 2 # 1e6

from base_crawler import BasicDbCrawler

'''
reading data from EEX export is a real PITA.
The data format in single files are changing.
Multiple files are stored in one and are not efficiently parseable.

e.g.:
in /market_data/environmental/derivatives/csv/2022/20220107/BiomassFutureMarketResults_20220107.csv
starts with
```
# Prices/Volumes of EEX Biomass Future Market
#
# Data type(ST);Trading Date;Creation Time
# Data type(PR);Product;Long Name;Maturity;Delivery Start;Delivery End;Open Price;Timestamp Open Price;High Price;Timestamp High Price;Low Price;Timestamp Low Price;Last Price;Timestamp Last Price;Settlement Price;Unit of Prices;Lot Size;Traded Lots;Number of Trades;Traded Volume;Open Interest Lots;Open Interest Volume;Unit of Volumes
# Data type(OT);Product;Long Name;Maturity;Delivery Start;Delivery End;Lot Size;Traded Lots;Number of Trades;Traded Volume;Unit of Volumes
# Data type(AL);Number of Lines
#
```
following are lines with different format, depending on their start string.
One would have to create a parser here which reads the lines beginning with `Data type` 
and matches the definition to lines beginning with `ST` and so.

in /market_data/power/at/spot/csv/2016/MCCMarketResult_apg-eles_2016.csv
the TSO Area: APG-ELES is only part of the comment - but it should be stored as a column as TSO region.
No further parsing is needed else.
'''

class EEXCrawler(BasicDbCrawler):
    def read_eex_trade_spot_file(self, filename):
        df = pd.read_csv(path_xx, skiprows=1, index_col='Trade ID')
        df['Time Stamp'] = pd.to_datetime(df['Time Stamp'], infer_datetime_format=True)
        df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)
        if 'Quantity (MW)' in df.columns:
            df['Volume (MW)'] = df['Quantity (MW)']
            del df['Quantity (MW)']
        return df

    def read_eex_market_file(self, filename, name):
        try:
            # ignore lines beginning with a hashtag comment
            exclude_lists = {
                'ST' : [],
                'PR' : [],
                'OT' : [],
                'SP' : [],
                'IL' : []
                # 'AL' : [],
            }

            header_dict = {}

            for i, line in enumerate(open(filename)):
                if line.startswith('#'):
                    if line[2:].startswith('Data type(') :
                        key = line[12:14]
                        header_dict[key]=line[2:].split(';')

                for key in exclude_lists.keys():
                    if not line.startswith(key):
                        exclude_lists[key].append(i)
            line_count = i+1
            
            for key, exclude_list in exclude_lists.items():
                if len(exclude_lists[key]) == line_count:
                    log.debug(f'all lines excluded for {key} - not writing')
                else:
                    df = pd.read_csv(filename, skiprows=exclude_list, sep=';', decimal=',', header=None) #, index_col='Trade ID')
                    df.columns = header_dict[key]

                    with self.db_accessor() as conn:
                        df.to_sql(f'{name}_{key}', conn, if_exists='append')
                    log.debug(osp.basename(filename)[:-4])
        except Exception as e:
            log.error(f'could not save {filename} - {e}')

    def save_trade_data_per_day(self, year_path, name):
        log.debug(year_path)
        # TODO looking for *.csv is not enough
        # as noted above - the name of the file is often crucial for the market area, or definition of content of the file
        for file in glob(year_path+'/*/*.csv', recursive=True)[:FIRST_X]: #XXX limit here for debugging 
            if 'trade_data/power/' in file and '/spot/csv/' in file:
                df = self.read_eex_trade_spot_file(file)
                with self.db_accessor() as conn:
                    df.to_sql(name, conn, if_exists='append')

            else:    
                self.read_eex_market_file(file, name)


    def get_trade_data_per_year(self, data_path, name):
        log.info(name)
        with os.scandir(data_path) as years:
            for year in years:
                year_path = osp.join(data_path,year.name)
                self.save_trade_data_per_day(year_path, name)

    def get_trade_data_per_market(self, path, name):
        with os.scandir(path) as markets:
            for market in markets:
                if 'archive' in market.name.lower():
                    continue
                data_path = osp.join(path,market.name, 'csv')
                self.get_trade_data_per_year(data_path, f'{name}_{market.name}')
                # market_data/power/de/spot/csv

    def download_with_country(self, foldername):
        # market_data/power
        product = osp.basename(foldername)
        with os.scandir(foldername) as countries:
            for country in countries:
                if 'archive' in country.name.lower():
                    continue
                path = osp.join(foldername,country.name)
                self.get_trade_data_per_market(path, f'{product}_{country.name}')

    def download_without_country(self, foldername):
        product = osp.basename(foldername)
        self.get_trade_data_per_market(foldername, product)

'''
exemplary pathes:

trade_data/power/de/spot/csv/2021/20210909/file
market_data/environmental/derivatives
market_data/environmental/spot
market_data/natgas/cegh_vtp/derivatives
market_data/power/at/derivatives
market_data/power/at/spot
'''

def main(db_uri):
    crawler = EEXCrawler(db_uri)
    crawler.download_without_country(eex_data_path + 'market_data/environmental')
    crawler.download_with_country(eex_data_path + 'market_data/power')
    crawler.download_with_country(eex_data_path + 'market_data/natgas')
    crawler.download_with_country(eex_data_path + 'trade_data/power')
    

if __name__ == '__main__':
    logging.basicConfig()
    db_uri = './data/eex.db'
    main(db_uri)
    import matplotlib.pyplot as plt

    path_xx = '/mnt/eex/trade_data/power/de/spot/csv/2021/20210909/intraday_transactions_germany_2021-09-09.csv'
    df = crawler.read_eex_trade_spot_file(path_xx)
    # df['Time Stamp'] = pd.to_datetime(df['Time Stamp'], infer_datetime_format=True)
    # df['Date'] = pd.to_datetime(df['Date'], infer_datetime_format=True)

    #plt.plot(df.index, df['Price (EUR)'])    