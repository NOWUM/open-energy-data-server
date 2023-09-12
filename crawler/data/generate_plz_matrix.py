import os.path as osp

import numpy as np
import shapefile
from shapely.geometry import Point, shape
from tqdm import tqdm


def get_pos_nums(num):
    pos_nums = []
    while num != 0:
        pos_nums.append(num % 10)
        num = num // 10
    pos_nums.reverse()
    mul, plz = 100, 0
    for digit in range(3):
        plz += pos_nums[digit] * mul
        mul /= 10
    return int(plz)


def generate_plz_matrix():
    shape5_path = osp.join(osp.dirname(__file__), "shapes", "plz-5stellig.shp")
    shape5_germany = shapefile.Reader(shape5_path)
    plz_areas_5 = {
        feature.record["plz"]: shape(feature.shape.__geo_interface__)
        for feature in shape5_germany.shapeRecords()
    }
    data_path = osp.join(osp.dirname(__file__))
    lat_coordinates = np.load(data_path + "/lat_coordinates.npy")
    lon_coordinates = np.load(data_path + "/lon_coordinates.npy")
    plz5_matrix = np.zeros(lon_coordinates.shape)
    rows, cols = plz5_matrix.shape
    for i in tqdm(range(rows)):
        for j in range(cols):
            for key, area in plz_areas_5.items():
                if area.contains(Point((lon_coordinates[i][j], lat_coordinates[i][j]))):
                    plz5_matrix[i][j] = int(key)
                    break
    np.save(data_path + "/plz5_matrix.npy", plz5_matrix, allow_pickle=True)


def generate_plz3_matrix(plz5_matrix):
    plz3_matrix = np.zeros(plz5_matrix.shape)
    rows, cols = plz5_matrix.shape
    for i in tqdm(range(rows)):
        for j in range(cols):
            if plz5_matrix[i][j] > 0:
                plz3_matrix[i][j] = int(get_pos_nums(plz5_matrix[i][j]))
    data_path = osp.join(osp.dirname(__file__))
    np.save(data_path + "/plz3_matrix.npy", plz5_matrix, allow_pickle=True)


if __name__ == "__main__":
    generate_plz3_matrix(np.load("plz_matrix.npy"))
