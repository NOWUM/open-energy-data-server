import pandas as pd
import requests
import zipfile
import io
import logging
import sqlite3


logging.basicConfig()
log = logging.getLogger('MaStR')
log.setLevel(logging.INFO)


def get_mastr_url():
    base_url = 'https://download.marktstammdatenregister.de/Gesamtdatenexport'

    response = requests.get('https://www.marktstammdatenregister.de/MaStR/Datendownload')
    html_site = response.content.decode('utf-8')
    begin = html_site.find(base_url)
    if begin == -1:
        raise Exception('Error while collecting data from MaStR')

    end = html_site.find('"', begin)
    return html_site[begin:end]

def get_data_from_mastr(data_url):
    response = requests.get(data_url)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for info in zip_file.infolist():
            with zip_file.open(info) as file:
                yield file, info

def init_database(connection, database):

    query = f"DROP DATABASE IF EXISTS {database}"
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    query = f"CREATE DATABASE {database}"
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    log.info('initialize database')


id_fields = ['MastrNummer', 'EinheitMastrNummer', 'EegMastrNummer',
             'KwkMastrNummer',
             'NetzanschlusspunktMastrNummer', 'Id', 'GenMastrNummer']

def set_index(data_):
    # Mastr should always be lowercase to avoid confusion
    new_cols = list(data_.columns.copy())
    for i in range(len(new_cols)):
        new_cols[i] = new_cols[i].replace('MaStR', 'Mastr')
    data_.columns = new_cols

    for field in id_fields:
        if field in data_.columns:
            # only one field can be index
            data_.set_index(field)
            return field

def create_db_from_export(connection):
    tables = {}

    data_url = get_mastr_url()
    print(data_url)
    log.info(f'get data from MaStR with url {data_url}')
    for file, info in get_data_from_mastr(data_url):
        log.info(f'read file {info.filename}')
        if info.filename.endswith('.xml'):
            table_name = info.filename[0:-4].split('_')[0]
            df = pd.read_xml(file.read(), encoding='utf-16le')
            pk = set_index(df)

            try:
                # this will fail if there is a new column
                df.to_sql(table_name, connection, if_exists='append', index=False)
            except Exception as e:
                log.info(repr(e))
                data = pd.read_sql(f'SELECT * FROM "{table_name}"', connection)
                if 'level_0' in data.columns:
                    del data['level_0']
                if 'index' in data.columns:
                    del data['index']
                pk = set_index(data)
                df2 = pd.concat([data, df])
                df2.to_sql(name=table_name, con=connection, if_exists='replace', index=False)

            if table_name not in tables.keys():
                tables[table_name]=pk

    for table_name, pk in tables.items():
        try:
            if type(connection)==sqlite3.Connection:
                connection.execute(f'CREATE UNIQUE INDEX idx_{table_name}_{pk} ON {table_name}({pk});')
            else:
                connection.execute(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{pk}");')
        except Exception:
            log.exception('Error adding pk')
    return tables

def main(db_uri):
    from sqlalchemy import create_engine
    engine = create_engine(db_uri)
    #engine = sqlite3.connect('mastr.db')

    init_database(engine, 'mastr')
    engine = create_engine(f'{db_uri}/mastr')
    try:
        tables = create_db_from_export(connection=engine)
    except Exception:
        log.exception('error in mastr')

if __name__ == '__main__':
    db_uri = 'postgresql://opendata:opendata@10.13.10.41:5432/mastr'
    main(db_uri)
