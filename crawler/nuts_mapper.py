# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import zipfile
from pathlib import Path

import geopandas as gpd
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


if __name__ == "__main__":
    from crawler.config import db_uri
    main(db_uri("nuts"))
