import os
import pandas as pd
import requests
import geopandas
import fiona
from sqlalchemy import create_engine, text

# Due to the size of the crawled file, it is not downloaded here but instead can be downloaded from the following link:
# https://www.opengeodata.nrw.de/produkte/umwelt_klima/klima/kwp/
# Make sure to extract the file and place it in the data folder of the project

class DataCrawler:
    def __init__(self):
        self.engine = create_engine(db_uri)
        pass   


    def save_to_database(self, ):
            start_i = 0
            end_i = 1000
            while end_i < 12710309:
                data = geopandas.read_file(os.path.join(os.path.dirname(__file__)) + "\data\Waermebedarf_NRW.gdb", rows=slice(start_i, end_i,None))

                start_i = end_i
                end_i += 1000
                if end_i > 12710308:
                    end_i = 12710308
                with self.engine.begin() as conn: 
                    data.to_postgis("waermedichte", conn, if_exists="append") 


if __name__ == "__main__":
    crawler = DataCrawler()
    #crawler.save_to_database()