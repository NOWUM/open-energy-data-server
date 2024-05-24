# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

import json5  # parse js-dict to python
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup  # parse html
from tqdm import tqdm  # fancy for loop

from crawler.config import db_uri

"""
Downloads the available powercurve data from https://www.wind-turbine-models.com/ to a csv file

The raw measured data from https://www.wind-turbine-models.com/powercurves is used.

Interpolated values would be available from the individual wind turbines page are also available but are harder to crawl:

e.g.: https://www.wind-turbine-models.com/turbines/1502-fuhrlaender-llc-wtu3.0-120

Therefore, interpolation from scipy is used.
For the given model, this interpolation is not good, as it produces negative values (which are nulled in the script)

The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.
"""

log = logging.getLogger("windmodel")
log.setLevel(logging.INFO)


def get_turbines_with_power_curve():
    # create list of turbines with available powercurves
    page = requests.get("https://www.wind-turbine-models.com/powercurves")
    soup = BeautifulSoup(page.text, "html.parser")
    # pull all text from the div
    name_list = soup.find(class_="chosen-select")

    wind_turbines_with_curve = []
    for i in name_list.find_all("option"):
        wind_turbines_with_curve.append(i.get("value"))

    return wind_turbines_with_curve


def download_turbine_curve(turbine_id, start=0, stop=25):
    url = "https://www.wind-turbine-models.com/powercurves"
    headers = dict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    data = {
        "_action": "compare",
        "turbines[]": turbine_id,
        "windrange[]": [start, stop],
    }

    resp = requests.post(url, headers=headers, data=data)
    strings = resp.json()["result"]
    begin = strings.find("data:")
    end = strings.find('"}]', begin)
    relevant_js = "{" + strings[begin : end + 3] + "}}"
    curve_as_dict = json5.loads(relevant_js)
    x = curve_as_dict["data"]["labels"]
    y = curve_as_dict["data"]["datasets"][0]["data"]
    label = curve_as_dict["data"]["datasets"][0]["label"]
    url = curve_as_dict["data"]["datasets"][0]["url"]
    df = pd.DataFrame(np.asarray(y, dtype=float), index=x, columns=[label])
    try:
        df = df.interpolate(method="polynomial", order=3)
        df = df.fillna(0)
    except Exception as e:
        log.error(f"Error: {e}")
    df.index.name = "wind_speed"
    return df


def download_all_turbines():
    wind_turbines = get_turbines_with_power_curve()
    curves = []
    for turbine_id in tqdm(wind_turbines):
        curve = download_turbine_curve(turbine_id)
        curves.append(curve)
    df = pd.concat(curves, axis=1)
    all_turbines_trunc = df[df.any(axis=1)]
    df = all_turbines_trunc.fillna(0)
    df[df < 0] = 0
    return df


def main(db_uri):
    from sqlalchemy import create_engine

    engine = create_engine(db_uri)
    turbine_data = download_all_turbines()
    turbine_data.to_sql("turbine_data", engine, if_exists="replace")
    return turbine_data


if __name__ == "__main__":
    logging.basicConfig()
    wind_turbines = get_turbines_with_power_curve()
    turbine_data = main(db_uri("windmodel"))

    with open("turbine_data.csv", "w") as f:
        turbine_data.to_csv(f)
    turbine_data = pd.read_csv("turbine_data.csv", index_col=0)
