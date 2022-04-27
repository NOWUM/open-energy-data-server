import numpy as np
import pandas as pd
from glob import glob as gb
from sqlalchemy import create_engine
import os

host = os.getenv('HOST', '10.13.10.41')
port = int(os.getenv('PORT', 5432))
user = os.getenv('USER', 'opendata')
password = os.getenv('PASSWORD', 'opendata')
database = os.getenv('TIMESCALEDB_DATABASE', 'axxteq')

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

garage_name = {318: 'Stadt_A',
               464: 'Stadt_D',
               500: 'to_replace',
               572: 'to_replace',
               638: 'to_replace',
               656: 'to_replace',
               671: 'to_replace'}

ticket_types = {'Seasonparker Card': 'middle_term',
                'Shorttermticket': 'short_term',
                'PREPAID CARD': 'middle_term',
                'One Time Exit Ticket': 'lost',
                'Kurzparker': 'short_term',
                'Dauerparkerkarte': 'long_term',
                'Ausfahrticket': 'lost',
                'Dauerparkerticket': 'long_term',
                'Ausfahrt man': 'lost',
                'Kongressticket Var': 'middle_term',
                'Dauerparker Ticket': 'long_term',
                'Kongresskarte Fix': 'middle_term',
                'Ausfahrtticket': 'lost',
                'Verlorenes Ticket': 'lost',
                'Dauerparker Karte': 'long_term',
                'Kurzparkerticket': 'short_term',
                'Monatsticket': 'middle_term',
                'Dauerparker Transponder': 'long_term',
                'Verlorenes Ticket Stadtpark': 'lost'}


def create_table():
    engine.execute("CREATE TABLE IF NOT EXISTS parking_data( "
                    "time timestamp without time zone NOT NULL, "
                    "ticket_id integer, "
                    "card_type text, "
                    "entry_time timestamp without time zone NOT NULL, "
                    "exit_time timestamp without time zone NOT NULL, "
                    "park_duration integer, "
                    "name text, "
                    "PRIMARY KEY (time , ticket_id));")

    query_create_hypertable = "SELECT create_hypertable('parking_data', 'time', if_not_exists => TRUE, migrate_data => TRUE);"
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(query_create_hypertable)

def read(file):
    if '.csv' in file:
        try:
            data = pd.read_csv(file, encoding='utf-8', sep=',', quotechar="'")
            if len(data.columns) < 3:
                raise Exception('wrong format')
        except:
            data = pd.read_csv(file, encoding='utf-8', sep=';', quotechar="'")
        data.columns = list(map(lambda x: x.replace("'", "").strip(), data.columns))
    else:
        data = pd.read_excel(r'axxteq/xlsx/Mall_0520.xlsx', skiprows=3, usecols=[0, 3, 5, 7])
        data.columns = ['ticket_id', 'card_type', 'entry_time', 'exit_time']
        data = data.dropna(axis=0, how='any')
    # print(data.head(5))
    return data


def read_files(excel: bool = False, convert_utc: bool = False):
    if excel:
        files = gb(r'./axxteq/xlsx/*.xlsx')
    else:
        files = gb(r'./axxteq/csv/*.csv')
    dataframe = []
    for file in files:
        data = read(file)
        if convert_utc:
            data['entry_time'] = data['entry_time'].apply(lambda x: pd.to_datetime(x).tz_convert('utc'))
            data['exit_time'] = data['exit_time'].apply(lambda x: pd.to_datetime(x).tz_convert('utc'))
        else:
            data['entry_time'] = data['entry_time'].apply(lambda x: pd.to_datetime(x.split('+')[0]))
            data['exit_time'] = data['exit_time'].apply(lambda x: pd.to_datetime(x.split('+')[0]))

        data['card_type'] = data['card_type'].apply(lambda x: ticket_types[x])
        data['park_duration'] = data['exit_time'] - data['entry_time']
        data['park_duration'] = data['park_duration'].apply(lambda x: np.timedelta64(x, 'm') / np.timedelta64(1, 'm'))
        if not excel:
            data['name'] = garage_name[int(file.split('_')[0][-3:])]
        else:
            f = file.split('\\')[-1]
            if len(f.split('_')) > 2:
                data['name'] = f.split('_')[0] + '_' + f.split('_')[1]
            else:
                data['name'] = f.split('_')[0]
        dataframe += [data]

    return pd.concat(dataframe).dropna(axis=1, how='any').set_index(['ticket_id', 'entry_time'])


if __name__ == "__main__":

    # create_table()

    df_1 = read_files(excel=False, convert_utc=False)
    df_1.to_sql('parking_data', engine, if_exists='replace')
    df_2 = read_files(excel=True, convert_utc=False)
    df_2.to_sql('parking_data', engine, if_exists='replace')


