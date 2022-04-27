import os
import pandas as pd
from sqlalchemy import create_engine


host = os.getenv('HOST', '10.13.10.41')
port = int(os.getenv('PORT', 5432))
user = os.getenv('USER', 'opendata')
password = os.getenv('PASSWORD', 'opendata')
database = os.getenv('TIMESCALEDB_DATABASE', 'ninja')

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


def write_wind_capacity_factors():
    data = pd.read_csv(r'./data/ninja_wind_europe_v1.1_current_on-offshore.csv', index_col=0)
    data.index = pd.to_datetime(data.index)
    onshore = {col.split('_')[0].lower(): data[col].values for col in data.columns if 'ON' in col}
    df_on = pd.DataFrame(data=onshore, index=data.index)
    df_on.to_sql('capacity_wind_on', engine, if_exists='replace')
    offshore = {col.split('_')[0].lower(): data[col].values for col in data.columns if 'OFF' in col}
    df_off = pd.DataFrame(data=offshore, index=data.index)
    df_off.to_sql('capacity_wind_off', engine, if_exists='replace')


def write_solar_capacity_factors():
    data = pd.read_csv(r'./data/ninja_pv_europe_v1.1_merra2.csv', index_col=0)
    data.index = pd.to_datetime(data.index)
    data.columns = [col.lower() for col in data.columns]
    data.to_sql('capacity_solar_merra2', engine, if_exists='replace')
    data = pd.read_csv(r'./data/ninja_pv_europe_v1.1_sarah.csv', index_col=0)
    data.index = pd.to_datetime(data.index)
    data.columns = [col.lower() for col in data.columns]
    data.to_sql('capacity_solar_sarah', engine, if_exists='replace')


if __name__ == "__main__":
    # write_wind_capacity_factors()
    write_solar_capacity_factors()