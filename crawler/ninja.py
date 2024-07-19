import os.path as osp
import pandas as pd
import requests
import zipfile
from io import BytesIO
from common.config import db_uri
from common.base_crawler import create_schema_only, set_metadata_only

from sqlalchemy import create_engine

metadata_info = {
    "schema_name": "ninja",
    "data_date": "2016-12-31",
    "data_source": "https://www.renewables.ninja/downloads",
    "license": "CC-BY-4.0",
    "description": "NINJA renewables capacity. Country specific capacities for wind and solar.",
    "contact": "",
    "temporal_start": "1980-01-01 00:00:00",
    "temporal_end": "2016-12-31 23:00:00",
}


def download_and_extract(url, extract_to):
    response = requests.get(url)
    with zipfile.ZipFile(BytesIO(response.content)) as z_file:
        z_file.extractall(extract_to)

def write_wind_capacity_factors(engine, wind_path):
    data = pd.read_csv(wind_path, index_col=0)
    data.index = pd.to_datetime(data.index)
    onshore = {
        col.split("_")[0].lower(): data[col].values
        for col in data.columns
        if "ON" in col
    }
    df_on = pd.DataFrame(data=onshore, index=data.index)
    df_on.to_sql("capacity_wind_on", engine, if_exists="replace")
    offshore = {
        col.split("_")[0].lower(): data[col].values
        for col in data.columns
        if "OFF" in col
    }
    df_off = pd.DataFrame(data=offshore, index=data.index)
    df_off.to_sql("capacity_wind_off", engine, if_exists="replace")


def write_solar_capacity_factors(engine, solar_path):
    data = pd.read_csv(solar_path, index_col=0)
    data.index = pd.to_datetime(data.index)
    data.columns = [col.lower() for col in data.columns]
    data.to_sql("capacity_solar_merra2", engine, if_exists="replace")


def main(schema_name):
    engine = create_engine(db_uri(schema_name))

    create_schema_only(engine, schema_name)
    base_path = osp.join(osp.dirname(__file__), "data")
    wind_url = "https://www.renewables.ninja/downloads/ninja_europe_wind_v1.1.zip"
    solar_url = "https://www.renewables.ninja/downloads/ninja_europe_pv_v1.1.zip"

    download_and_extract(wind_url, base_path)
    download_and_extract(solar_url, base_path)

    wind_path = osp.join(
        base_path, "ninja_wind_europe_v1.1_current_on-offshore.csv")
    solar_path = osp.join(base_path, "ninja_pv_europe_v1.1_merra2.csv")
    write_wind_capacity_factors(engine, wind_path)
    write_solar_capacity_factors(engine, solar_path)
    set_metadata_only(engine, metadata_info)


if __name__ == "__main__":
    main("ninja")
