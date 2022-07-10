import numpy as np
from bs4 import BeautifulSoup  # parse html
import requests
import json5  # parse js-dict to python
import pandas as pd
from tqdm import tqdm  # fancy for loop
from sqlalchemy import create_engine
import scipy # needed for interpolation

def get_turbines_with_power_curve():
    # create list of turbines with available powercurves
    page = requests.get('https://www.wind-turbine-models.com/powercurves')
    soup = BeautifulSoup(page.text, 'html.parser')
    # pull all text from the div
    name_list = soup.find(class_='chosen-select')

    wind_turbines_with_curve = []
    for i in name_list.find_all('option'):
        wind_turbines_with_curve.append(i.get('value'))

    return wind_turbines_with_curve

def download_turbine_curve(turbine_id, start=0, stop=25):
    url = "https://www.wind-turbine-models.com/powercurves"
    headers = dict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    data = {'_action': 'compare', 'turbines[]': turbine_id, 'windrange[]': [start, stop]}

    resp = requests.post(url, headers=headers, data=data)
    strings = resp.json()['result']
    begin = strings.find('data:')
    end = strings.find('"}]', begin)
    relevant_js = '{' + strings[begin:end + 3] + '}}'
    curve_as_dict = json5.loads(relevant_js)
    x = curve_as_dict['data']['labels']
    y = curve_as_dict['data']['datasets'][0]['data']
    label = curve_as_dict['data']['datasets'][0]['label']
    url = curve_as_dict['data']['datasets'][0]['url']
    df = pd.DataFrame(np.asarray(y, dtype=float), index=x, columns=[label])
    try:
        df = df.interpolate(method='polynomial', order=3)
        df = df.fillna(0)
    except Exception as e:
        print(repr(e))
    df.index.name = 'wind_speed'
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
    df[df<0] = 0
    return df

if __name__ == "__main__":

    wind_turbines = get_turbines_with_power_curve()
    turbine_data = download_all_turbines()
    with open('turbine_data.csv', 'w') as f:
        turbine_data.to_csv(f)
    turbine_data = pd.read_csv('turbine_data.csv', index_col=0)
    
    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432/windmodel')
    turbine_data.to_sql('turbine_data', engine, if_exists='replace')
