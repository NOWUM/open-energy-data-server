import cdsapi
import pygrib
import pandas as pd
import numpy as np
import os
from pygeotile.tile import Tile
from itertools import product

# key = '690d1282-fe2c-4a1c-9367-c14f58fff451'

c = cdsapi.Client()                     # --> client for ECMWF-Service
year = 2020                             # --> requested year

var_ = ['10m_u_component_of_wind',      # --> requested weather variable
        '10m_v_component_of_wind',
        '2m_temperature',
        'surface_net_solar_radiation']

names = ['10 metre V wind component',   # --> names in response
         '10 metre U wind component',
         '2 metre temperature',
         'Surface net solar radiation']

keys = {'10 metre V wind component': 'wind_meridional',
        '10 metre U wind component': 'wind_zonal',
        '2 metre temperature': 'temp_air',
        'Surface net solar radiation': 'ghi'}


# --> build request dictionary
request = dict(format='grib', variable=var_,
               year=f'{year}',
               month=[f'{i:02d}' for i in range(1, 13)],
               day=[f'{i:02d}' for i in range(1, 32)],
               time=[f'{i:02d}:00' for i in range(24)])

def create_table(engine):
    engine.execute("CREATE TABLE IF NOT EXISTS ecmwf( "
                    "time timestamp without time zone NOT NULL, "
                    "temp_air double precision, "
                    "ghi double precision, "
                    "wind_meridional double precision, "
                    "wind_zonal double precision, "
                    "east double precision, "
                    "west double precision, "
                    "north double precision, "
                    "south double precision, "
                    "x integer, "
                    "y integer, "
                    "PRIMARY KEY (time , east, west, north, south));")

    try:
        query_create_hypertable = "SELECT create_hypertable('ecmwf', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
        with engine.connect() as conn, conn.begin():
            conn.execute(query_create_hypertable)
        log.info(f'created hypertable ecmwf')
    except Exception as e:
        log.error(f'could not create hypertable: {e}')



def get_position(x_pos, y_pos, z):
    bbox = Tile.from_google(x_pos, y_pos, z).bounds

    north = round(max(bbox[0].latitude, bbox[1].latitude), 2)
    south = round(min(bbox[0].latitude, bbox[1].latitude), 2)
    east = round(max(bbox[0].longitude, bbox[1].longitude), 2)
    west = round(min(bbox[0].longitude, bbox[1].longitude), 2)

    return [north, west, south, east]


def save_data(x_pos, y_pos, z):
    request['area'] = get_position(x_pos, y_pos, z)
    c.retrieve('reanalysis-era5-land', request, fr'./{year}_{x_pos}_{y_pos}ecmwf.grb')


def build_dataframe(engine, x_pos, y_pos, z):
    north, west, south, east = get_position(x_pos, y_pos, z)
    weather_data = pygrib.open(fr'./{year}_{x_pos}_{y_pos}ecmwf.grb')
    data_set, len_ = dict(), 0
    for name in names:
        arrays = weather_data.select(name=name)
        len_ = len(arrays)
        data_set[keys[name]] = [np.mean(array) for array in arrays]
    data_set['time'] = pd.date_range(start=f'{year}-01-01', periods=len_, freq='h')

    df = pd.DataFrame(data=data_set)
    df['north'] = north
    df['west'] = west
    df['south'] = south
    df['east'] = east
    df['x'] = x_pos
    df['y'] = y_pos

    df = df.set_index(['time', 'east', 'west', 'north', 'south'])
    df.to_sql('ecmwf', con=engine, if_exists='append')


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine(f'postgresql://opendata:opendata@10.13.10.41:5432/weather')
    create_table(engine)

    # --> x coords for tiles
    x_min = int(os.getenv('X_MIN', 66))
    x_max = int(os.getenv('X_MAX', 69))
    x_range = np.arange(x_min, x_max + 1)
    # --> y coords for tiles
    y_min = int(os.getenv('Y_MIN', 40))
    y_max = int(os.getenv('Y_MAX', 44))
    y_range = np.arange(y_min, y_max + 1)
    # --> zoom level
    zoom = int(os.getenv('ZOOM', 7))

    for x, y in product(x_range, y_range):
        print(x, y)
        save_data(x, y, 7)



