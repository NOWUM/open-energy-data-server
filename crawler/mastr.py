# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine, text

import lxml


logging.basicConfig()
log = logging.getLogger("MaStR")
log.setLevel(logging.INFO)


def get_mastr_url():
    # taken from https://www.marktstammdatenregister.de/MaStR/Datendownload
    # Objektmodell:
    # https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Objektmodell%20-%20Fachliche%20Ansicht%20V1.2.0.pdf
    # Dokumentation statische Katalogwerte:
    # https://www.marktstammdatenregister.de/MaStRHilfe/files/webdienst/Funktionen_MaStR_Webdienste_V23.2.112.html
    # Dynamische Katalogwerte sind in Tabelle "Katalogkategorien" und "Katalogwerte"
    base_url = "https://download.marktstammdatenregister.de/Gesamtdatenexport"

    response = requests.get(
        "https://www.marktstammdatenregister.de/MaStR/Datendownload"
    )
    html_site = response.content.decode("utf-8")
    begin = html_site.find(base_url)
    if begin == -1:
        raise Exception("Error while collecting data from MaStR")

    end = html_site.find('"', begin)
    return html_site[begin:end]


def get_data_from_mastr(data_url):
    response = requests.get(data_url)

    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        for info in zip_file.infolist():
            with zip_file.open(info) as file:
                yield file, info


def init_database(connection, database):
    query = text(f"DROP DATABASE IF EXISTS {database}")
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    query = text(f"CREATE DATABASE {database}")
    connection.execution_options(isolation_level="AUTOCOMMIT").execute(query)

    log.info("initialize database")


id_fields = [
    "MastrNummer",
    "EinheitMastrNummer",
    "EegMastrNummer",
    "KwkMastrNummer",
    "NetzanschlusspunktMastrNummer",
    "Id",
    "GenMastrNummer",
]


def set_index(data_):
    # Mastr should always be lowercase to avoid confusion
    new_cols = list(data_.columns.copy())
    for i in range(len(new_cols)):
        new_cols[i] = new_cols[i].replace("MaStR", "Mastr")
    data_.columns = new_cols

    for field in id_fields:
        if field in data_.columns:
            # only one field can be index
            data_.set_index(field)
            return field


def create_db_from_export(connection):
    tables = {}

    data_url = get_mastr_url()
    log.info(f"get data from MaStR with url {data_url}")
    for file, info in get_data_from_mastr(data_url):
        log.info(f"read file {info.filename}")
        if info.filename.endswith(".xml"):
            table_name = info.filename[0:-4].split("_")[0]
            df = pd.read_xml(file.read(), encoding="utf-16le")
            pk = set_index(df)

            try:
                # this will fail if there is a new column
                with connection.begin() as conn:
                    df.to_sql(table_name, conn, if_exists="append", index=False)
            except Exception as e:
                log.info(repr(e))
                with connection.begin() as conn:
                    data = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
                if "level_0" in data.columns:
                    del data["level_0"]
                if "index" in data.columns:
                    del data["index"]
                pk = set_index(data)
                df2 = pd.concat([data, df])
                with connection.begin() as conn:
                    df2.to_sql(
                        name=table_name,
                        con=connection,
                        if_exists="replace",
                        index=False,
                    )

            if table_name not in tables.keys():
                tables[table_name] = pk

    for table_name, pk in tables.items():
        if str(connection.url).startswith("sqlite:/"):
            query = f"CREATE UNIQUE INDEX idx_{table_name}_{pk} ON {table_name}({pk});"
        else:
            query = f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("{pk}");'
        try:
            connection.execute(text(query))
        except Exception:
            log.exception("Error adding pk")
    return tables


def main(db_uri):
    engine = create_engine(db_uri)

    try:
        create_db_from_export(connection=engine)
    except Exception:
        log.exception("error in mastr")


if __name__ == "__main__":
    from config import db_uri

    main(db_uri("mastr"))
