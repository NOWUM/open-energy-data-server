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
    try:
        df = pd.read_csv(file, sep="\t")
        log.info("Succesfully read file into pd.DataFrame")

        return df

    except Exception as e:
        log.error(f"Could not read file: {e}")
        return -1


def main():
    # request zip archive
    response = request_zip_archive()

    if response == -1:
        return

    # extract files from response
    master_file, hlt_file, load_file = extract_files(response=response)

    if master_file == -1:
        return

    # read in files
    master_data = read_file(master_file)
    hlt_data = read_file(hlt_file)
    load_data = read_file(load_file)


if __name__ == "__main__":
    main()

