# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError


def main(db_uri):
    # Download shp zip for EU NUTS here:
    # https://ec.europa.eu/eurostat/web/gisco/geodata/reference-data/administrative-units-statistical-units/nuts
    download_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/shp/NUTS_RG_01M_2021_4326.shp.zip"
    # download file
    r = requests.get(download_url)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    # extract to shapes folder
    z.extractall("shapes")
    geo_path = Path(__file__).parent / "shapes" / "NUTS_RG_01M_2021_4326.shp"

    geo_information = gpd.read_file(geo_path)
    geo_information = geo_information.to_crs(4326)

    connection = create_engine(db_uri)
    query = text("CREATE EXTENSION postgis;")
    try:
        with connection.connect() as conn:
            conn.execute(query)
    except ProgrammingError:
        pass

    # columns to lower
    geo_information.columns = map(str.lower, geo_information.columns)
    geo_information.to_postgis("nuts", con=connection, if_exists="replace")

    # https://gisco-services.ec.europa.eu/tercet/flat-files
    # download zip
    download_plz = "https://gisco-services.ec.europa.eu/tercet/NUTS-2021/pc2020_DE_NUTS-2021_v4.0.zip"
    r = requests.get(download_plz)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    # open pc2020_DE_NUTS-2021_v4.0.csv with pandas
    with z.open("pc2020_DE_NUTS-2021_v4.0.csv") as f:
        plz_list = pd.read_csv(f, sep=";", index_col="CODE", quotechar="'")

    # remove str literals from plzlist with read_csv

    # ignore warning, geographic CRS centroid are enough for us
    # also see here: https://github.com/openclimatefix/nowcasting_dataset/issues/154#issuecomment-927148746
    centroids = geo_information["geometry"].centroid
    geo_information["longitude"] = centroids.x
    geo_information["latitude"] = centroids.y
    # where levl_code == 1 and country == DE
    geo_information = geo_information[geo_information["levl_code"] == 3]
    geo_information = geo_information[geo_information["cntr_code"] == "DE"]
    geo_information["nuts3"] = geo_information["nuts_id"]

    plz_list.columns = map(str.lower, plz_list.columns)
    plz_list["nuts2"] = plz_list["nuts3"].str[:5]
    plz_list["nuts1"] = plz_list["nuts3"].str[:4]
    plz_list.index.name = "code"

    # join geo on plz_list
    plz_join = plz_list.join(geo_information.set_index("nuts3"), on="nuts3")
    plz_join = plz_join[["nuts1", "nuts2", "nuts3", "longitude", "latitude"]]
    plz_join.to_sql("plz", con=connection, if_exists="replace")


if __name__ == "__main__":
    from crawler.config import db_uri
    main(db_uri("public"))
