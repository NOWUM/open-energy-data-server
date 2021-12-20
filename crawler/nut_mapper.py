from shapely.geometry import Point
import numpy as np
import geopandas as gpd
import pandas as pd

geo_information = gpd.read_file('./shapes/NUTS_EU.shp')
geo_information = geo_information.to_crs(4326)
nut_levels = {'DE': 3, 'NL': 1, 'BE': 1, 'LU': 1, 'PO': 1, 'DK': 1, 'FR': 1, 'CZ': 1, 'AT': 1, 'CH': 1}

data_frames = []
for key, value in nut_levels.items():
    df = geo_information[(geo_information['CNTR_CODE'] == key) &
                         (geo_information['LEVL_CODE'] == value)]
    data_frames.append(df)

geo_information = gpd.GeoDataFrame(pd.concat(data_frames))
dwd_latitude = np.load(r'./crawler/data/lat_coordinates.npy')
dwd_longitude = np.load(r'./crawler/data/lon_coordinates.npy')


def create_nuts_map(coords):
    i, j = coords
    nut = 'x'
    point = Point(dwd_longitude[i][j], dwd_latitude[i][j])
    for _, row in geo_information.iterrows():
        if row['geometry'].contains(point):
            nut = row['NUTS_ID']
            break
    return nut
