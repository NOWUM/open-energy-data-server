# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import json

import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Point, shape
from tqdm import tqdm

if __name__ == "__main__":
    # load and write to current folder
    data_path = osp.join(osp.dirname(__file__))

    # load GeoJSON file containing sectors
    with open(data_path + "/nordic.json") as f:
        nordic = json.load(f)
    # load GeoJSON file containing sectors
    with open(data_path + "/baltic.json") as f:
        baltic = json.load(f)

    lat_coordinates = np.load(data_path + "/lat_coordinates.npy")
    lon_coordinates = np.load(data_path + "/lon_coordinates.npy")

    # from https://www.suche-postleitzahl.org/downloads
    plz_matrix = np.load(data_path + "/plz_matrix.npy")

    for row in tqdm(range(lat_coordinates.shape[0])):
        for col in range(lat_coordinates.shape[1]):
            point = Point(lon_coordinates[row][col], lat_coordinates[row][col])
            # check each polygon to see if it contains the point
            for feature in nordic["features"]:
                polygon = shape(feature["geometry"])
                if polygon.contains(point):
                    plz_matrix[row][col] = 100000
            for feature in baltic["features"]:
                polygon = shape(feature["geometry"])
                if polygon.contains(point):
                    plz_matrix[row][col] = 100001

    plt.imshow(plz_matrix)
    plt.show()
