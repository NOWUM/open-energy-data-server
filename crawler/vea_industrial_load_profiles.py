import logging
import io

import zipfile
import requests
import pandas as pd
from sqlalchemy import text
from tqdm import tqdm

from common.base_crawler import BaseCrawler

log = logging.getLogger("vea-industrial-load-profiles")


metadata_info = {
    "schema_name": "vea_industrial_load_profiles",
    "data_date": "2016-01-01",
    "data_source": "https://zenodo.org/records/13910298",
    "license": "Creative Commons Attribution 4.0 International Public License",
    "description": """The data consists of 5359 one-year quarterhourly industrial load profiles (2016, leap year, 35136 values).
    Each describes the electricity consumption of one industrial commercial site in Germany used for official accounting.
    Local electricity generation was excluded from the data as far as it could be discovered (no guarantee of completeness).
    Together with load profiles comes respective master data of the industrial sites as well as the information wether each quarterhour was a high load time of the connected German grid operator in 2016.
    The data was collected by the VEA.
    The dataset as a whole was assembled by Paul Hendrik Tiemann in 2017 by selecting complete load profiles without effects of renewable generation from a VEA internal database.
    It is a research dataset and was used for master theses and publications.""",
    "contact": "komanns@fh-aachen.de",
    "temporal_start": "2016-01-01 00:00:00",
    "temporal_end": "2016-12-31 23:45:00",
    "concave_hull_geometry": None,
}


class IndustrialLoadProfileCrawler(BaseCrawler):

    def __init__(self, schema_name):
        super().__init__(schema_name)


    def request_extract_zip_archive(self):
        """
        Requests zip archive for industrial load profiles from zenodo.
        """

        url = "https://zenodo.org/records/13910298/files/load-profile-data.zip?download=1"

        log.info("Requesting zip archive from zenodo")

        response = requests.get(url)

        response.raise_for_status()

        log.info("Succesfully requested zip archive from zenodo")

        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            self.master_data_file = thezip.open(name="master_data_tabsep.csv")
            self.hlt_profiles_file = thezip.open(name="hlt_profiles_tabsep.csv")
            self.load_profiles_file = thezip.open(name="load_profiles_tabsep.csv")


    def read_file(
            self,
            filename: str | None = None):
        """Reads the given file and returns contents as pd.DataFrame.

        Args:
            filename (str | None, default: None): The name of the file being read.
        """

        log.info(f"Trying to read file {filename} into pd.DataFrame")

        if filename == "master":
            file = self.master_data_file
        elif filename == "load":
            file = self.load_profiles_file
        elif filename == "hlt":
            file = self.hlt_profiles_file

        self.df = pd.read_csv(file, sep="\t")

        log.info("Succesfully read file into pd.DataFrame")


    def create_timestep_datetime_dict(
            self,
            columns: list[str]):
        """Creates a dictionary mapping the timesteps (time0, time1, ...) to pd.Timestamp objects.

        Args:
            columns (list[str]): Columns of either the load or hlt profile dataframe (the timesteps).
        """

        log.info("Creating dictionary for timesteps mapping")

        timesteps = list(columns.difference(["id", "Unnamed: 35137"]))

        timestamps = pd.date_range(
            start="2016-01-01 00:00:00",
            end="2016-12-31 23:45:00",
            freq="15min",
            tz="Europe/Berlin")

        timestamps = timestamps.tz_convert("UTC")

        self.timestep_timestamp_map = {}
        for timestep in timesteps:
            idx = int(timestep.split("time")[1])
            self.timestep_timestamp_map[timestep] = timestamps[idx]

        log.info("Succesfully created dictionary")


    def transform_load_hlt_data(
            self,
            name: str | None = None):
        """Transform dataframe of load or hlt profiles into long format.

        Args:
            name (str | None, default None): a
        """

        log.info(f"Trying to convert {name} dataframe")

        # remove unused column
        df.drop(columns="Unnamed: 35137", inplace=True)

        # change to wide format
        df = df.melt(id_vars="id", var_name="timestamp")

        # map timestamps onto timestamp column
        df["timestamp"] = df["timestamp"].map(self.timestep_datetime_map)

        log.info("Succesfully converted hlt / load profile")

        return df


    def write_to_database(
            self,
            name: str) -> None:
        """Writes dataframe to database.

        Args:
            name (str): The name of the table to insert data to.
        """

        log.info(f"Trying to write {name} to database")

        rows = 200000
        list_df = [self.df[i:i+rows] for i in range(0, self.df.shape[0], rows)]

        for df in tqdm(list_df):
            df.to_sql(
                name=name,
                con=self.engine,
                if_exists="append",
                schema="vea-industrial-load-profiles",
                index=False)

        log.info("Succesfully inserted into databse")


    def convert_to_hypertable(
            self,
            relation_name: str):
        """
        Converts table to hypertable.

        Args:
            relation_name (str): The relation to convert to hypertable.
        """

        log.info("Trying to create hypertable")

        with self.engine.begin() as conn:
            query = text(
                f"SELECT public.create_hypertable('{relation_name}', 'timestamp', if_not_exists => TRUE, migrate_data => TRUE);"
            )
            conn.execute(query)

        log.info("Succesfully create hypertable")


def main():

    # create crawler instance
    ilp_crawler = IndustrialLoadProfileCrawler("vea_industrial_load_profiles")

    # request zip archive
    ilp_crawler.request_extract_zip_archive()

    # read load_data
    ilp_crawler.read_file(filename="load")

    # create timestamp dictionary to replace "timeX" with datetime object
    ilp_crawler.create_timestep_datetime_dict(ilp_crawler.df.columns)

    # transform and write load data
    ilp_crawler.transform_load_hlt_data(name="load")
    ilp_crawler.write_to_database(name="load")

    # read, transform and write hlt data
    ilp_crawler.read_file(filename="hlt")
    ilp_crawler.transform_load_hlt_data(name="hlt")
    ilp_crawler.write_to_database(name="high_load_times")

    # read in master data and write to database
    ilp_crawler.read_file(filename="master")
    ilp_crawler.write_to_database(name="master")

    # convert to hypertable
    ilp_crawler.convert_to_hypertable("high_load_times")
    ilp_crawler.convert_to_hypertable("load")

    # set metadata
    ilp_crawler.set_metadata(metadata_info)


if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    main()

