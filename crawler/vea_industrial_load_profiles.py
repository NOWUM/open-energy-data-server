import logging
import io

import zipfile
import requests
import pandas as pd

from common.base_crawler import BaseCrawler
from common.config import db_uri

log = logging.getLogger("vea-industrial-load-profiles")
log.setLevel(logging.INFO)


metadata = {}


def request_zip_archive() -> requests.Response:
    """
    Requests zip archive for industrial load profiles from zenodo.

    Returns:
        requests.Response: Response from server
    """

    url = "https://zenodo.org/records/13910298/files/load-profile-data.zip?download=1"

    try:
        response = requests.get(url)

        response.raise_for_status()

        return response

    except Exception as e:
        log.error(f"Could not request file from zenodo: {e}")
        return -1


def extract_files(response: requests.Response) -> tuple[zipfile.ZipExtFile]:
    """
    Extract files from response object.

    Args:
        response (requests.Response): The response from zenodo request.

    Returns:
        tuple[zipfile.ZipExtFile]: The three needed files: master_data, hlt_profiles and load_profiles.
    """

    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            master_data_file = thezip.open(name="master_data_tabsep.csv")
            hlt_profiles_file = thezip.open(name="hlt_profiles_tabsep.csv")
            load_profiles_file = thezip.open(name="load_profiles_tabsep.csv")

        return master_data_file, hlt_profiles_file, load_profiles_file

    except Exception as e:
        log.error(f"Could not extract file(s): {e}")
        return -1, -1, -1


def read_file(file: zipfile.ZipExtFile) -> pd.DataFrame:
    """Reads the given file and returns contents as pd.DataFrame.

    Args:
        load (zipfile.ZipExtFile): Original file from zip archive.

    Returns:
        pd.DataFrame: The data as pd.DataFrame.
    """

    log.info("Trying to read file into pd.DataFrame")

    df = pd.read_csv(file, sep="\t")

    log.info("Succesfully read file into pd.DataFrame")

    return df


def create_timestep_datetime_dict(columns: list[str]) -> dict[str: pd.Timestamp]:
    """Creates a dictionary mapping the timesteps (time0, time1, ...) to pd.Timestamp objects.

    Args:
        columns (list[str]): Columns of either the load or hlt profile dataframe (the timesteps).

    Returns:
        dict[str: pd.Timestamp]: Dictionary containing a pd.Timestamp for each timestep.
    """

    try:

        log.info("Creating dictionary for timesteps mapping")

        timesteps = list(columns.columns.difference(["id", "Unnamed: 35137"]))

        timestamps = pd.date_range(
            start="2016-01-01 00:00:00",
            end="2016-12-31 23:45:00",
            freq="15min",
            tz="Europe/Berlin")

        timestamps = timestamps.tz_convert("UTC")

        timestep_timestamp_map = {}
        for timestep in timesteps:
            idx = int(timestep.split("time")[1])
            timestep_timestamp_map[timestep] = timestamps[idx]

        log.info("Succesfully created dictionary")

        return timestep_timestamp_map

    except Exception as e:
        log.error(f"Could not create dictionary: {e}")
        return -1


def transform_load_hlt_data(
        df: pd.DataFrame,
        timestep_datetime_map: dict) -> pd.DataFrame:
    """Transform given dataframe of load or hlt profiles into long format.

    Args:
        df (pd.DataFrame): Original dataframe.

    Returns:
        pd.DataFrame: The transformed dataframe.
    """

    log.info("Trying to convert dataframe")

    # remove unused column
    df.drop(columns="Unnamed: 35137", inplace=True)

    # change to wide format
    df = df.melt(id_vars="id", var_name="timestamp")

    # map timestamps onto timestamp column
    df["timestamp"] = df["timestamp"].map(timestep_datetime_map)

    log.info("Succesfully converted hlt / load profil")

    return df


def main():
    # request zip archive
    response = request_zip_archive()

    if response == -1:
        return

    # extract files from response
    master_file, hlt_file, load_file = extract_files(response=response)

    # create timestamp dictionary to replace "timeX" with datetime object
    timestep_dt_map = create_timestep_datetime_dict(load_file.columns)

    if master_file == -1:
        return

    # read in files
    master_data = read_file(master_file)
    hlt_data = read_file(hlt_file)
    load_data = read_file(load_file)

    # transform files
    hlt_data = transform_load_hlt_data(df=hlt_data, timestep_datetime_map=timestep_dt_map)
    load_data = transform_load_hlt_data(df=load_data, timestep_datetime_map=timestep_dt_map)


if __name__ == "__main__":
    main()

