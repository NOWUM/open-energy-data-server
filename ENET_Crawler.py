import pandas as pd
import logging
import sqlite3
import glob
import pickle
logging.basicConfig()
log = logging.getLogger('MaStR')
log.setLevel(logging.INFO)
import pickle

def get_keys_from_export(connection):
    keys = {i: {'nsp': [], 'msp': [], 'hsp': []} for i in range(1, 100)}
    grids = {'nsp': [], 'msp': [], 'hsp': []}
    vals = {'nsp': 'netz_nsp',
            'msp': 'netz_nr_msp',
            'hsp': 'netz_nr_hsp'}

    for i in range(1, 100):
        start = i*1000
        end = start + 1000

        for voltage in ['nsp', 'msp', 'hsp']:
            query = f'SELECT distinct (no.ortsteil, no.ort, {vals[voltage]}) ' \
                    f'FROM netze_ortsteile no where plz >= {start} and plz < {end} ' \
                    f'and no.gueltig_bis = \'2100-01-01 00:00:00\''

            df = pd.read_sql(query, connection)
            if not df.empty:
                for _, series in df.iterrows():
                    x = tuple(map(str, series.values[0][1:-1].split(',')))
                    grid_id = int(x[-1])
                    if grid_id not in grids[voltage]:
                        keys[i][voltage].append(grid_id)
                        grids[voltage].append(grid_id)
    return keys


def init_database(connection, database):

    query = f"DROP DATABASE IF EXISTS {database}"
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    query = f"CREATE DATABASE {database}"
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    log.info('initialize database')


def create_db_from_export(connection):
    for table in glob.glob(r'../enet_database/*.csv'):
        df = pd.read_csv(table, sep=';', encoding='cp1252', decimal=',')
        df.columns = [x.lower() for x in df.columns]
        date_fields = ['stand', 'von', 'bis', 'gueltig_seit', 'gueltig_bis', 'datum_erfassung',
                       'datum_aenderung', 'letzte_pruefung','letzte_aenderung' ,'ersterfassung',
                       'aenderungsdatum']
        for field in date_fields:
            if field in df.columns:
                # df[field] = df[field].replace('2999-12-31 00:00:00', '2100-01-01 00:00:00')
                df[field] = pd.to_datetime(df[field], cache=True, errors='coerce')
                df[field] = df[field].fillna(pd.to_datetime('2100-01-01'))
        table_name = table.split('1252_')[-1][:-4]
        print(table_name)
        df.to_sql(table_name.lower(), connection, if_exists='append', index=False)


if __name__ == '__main__':
    from sqlalchemy import create_engine
    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432')
    # init_database(engine, 'enet')
    engine = create_engine('postgresql://opendata:opendata@10.13.10.41:5432/enet')
    keys = get_keys_from_export(engine)

    with open('enet_keys.pkl', 'wb') as handle:
        pickle.dump(keys, handle, protocol=pickle.HIGHEST_PROTOCOL)
    energy = {i: 0 for i in range(1, 100)}
    total = 0

    vals = {'nsp': 'arbeit_ns',
            'msp': 'arbeit_ms',
            'hsp': 'arbeit_hs'
            }
    for key, value in keys.items():
        for name, grids in value.items():
            for grid in grids:
                if name == 'nsp':
                    query=f'SELECT {vals[name]} ' \
                          f'FROM netzdaten where netz_nr = {grid} order by stand desc limit 1 '
                if name == 'msp':
                    query=f'SELECT {vals[name]},  arbeit_ms_ns as t ' \
                          f'FROM netzdaten where netz_nr = {grid} order by stand desc limit 1 '
                if name == 'hsp':
                    query=f'SELECT {vals[name]},  arbeit_hs_ms as t ' \
                          f'FROM netzdaten where netz_nr = {grid} order by stand desc limit 1 '
                df = pd.read_sql(query, engine)

                query = f'SELECT arbeit_hoes, arbeit_hoes_hs ' \
                        f'FROM netzdaten where netz_nr = {grid} order by stand desc limit 1 '

                test = pd.read_sql(query, engine)
                # print(test)
                if not df.empty:
                    df = df.fillna(0)
                    if name != 'nsp':
                        e = df[vals[name]].to_numpy()[0] # - df['t'].to_numpy()[0]
                    else:
                        e = df[vals[name]].to_numpy()[0]
                    e2 = test['arbeit_hoes'].to_numpy()[0]

                    total += e + e2

                else:
                    print(grid)

    print(total/10**6)


    try:
        # create_db_from_export(engine)
        pass

    except Exception as e:
        print(repr(e))
    # engine = sqlite3.connect('mastr.db')
    # try:
    #     tables = create_db_from_export(connection=engine)
    # except Exception:
    #     log.exception('error in mastr')