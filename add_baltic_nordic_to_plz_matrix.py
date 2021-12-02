import json
from shapely.geometry import shape, Point
import numpy as np
import matplotlib.pyplot as plt

if __name__ == "__main__":

    # load GeoJSON file containing sectors
    with open('Nordsee.json') as f:
        nordic = json.load(f)
    # load GeoJSON file containing sectors
    with open('Ostsee.json') as f:
        baltic = json.load(f)

    lat_coordinates = np.load(r'./lat_coordinates.npy')
    lon_coordinates = np.load(r'./lon_coordinates.npy')

    plz_matrix = np.load(r'./plz_matrix.npy')

    for row in range(lat_coordinates.shape[0]):
        for col in range(lat_coordinates.shape[1]):
            point = Point(lon_coordinates[row][col], lat_coordinates[row][col])

            # check each polygon to see if it contains the point
            for feature in nordic['features']:
                polygon = shape(feature['geometry'])
                if polygon.contains(point):
                    plz_matrix[row][col] = 100
            for feature in baltic['features']:
                polygon = shape(feature['geometry'])
                if polygon.contains(point):
                    plz_matrix[row][col] = 101


    plt.imshow(plz_matrix)
    plt.show()