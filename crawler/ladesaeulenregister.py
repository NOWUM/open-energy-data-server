# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
The Charging station map is available at:
https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/Karte/Ladesaeulenkarte.html
One can download the raw file as CSV from this link:
https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister_CSV.csv?__blob=publicationFile&v=42
"""
import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine

from .config import db_uri

log = logging.getLogger("ladesaeulenregister")
log.setLevel(logging.INFO)


def main(db_uri):
    engine = create_engine(db_uri)
    url = "https://data.bundesnetzagentur.de/Bundesnetzagentur/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister.csv"
    df = pd.read_csv(url, skiprows=10, delimiter=";")
    # there were two empty lines at the end
    df = df.dropna(how="all")
    # the PLZ should not be interpreted as a float but be integer
    df["Postleitzahl"] = pd.to_numeric(df["Postleitzahl"], downcast="integer")
    # some entries have whitespace before and after
    df["Längengrad"] = df["Längengrad"].str.replace(",", ".").str.strip()
    df["Längengrad"] = pd.to_numeric(df["Längengrad"])
    # some entries also have an extra delimiter at the end
    df["Breitengrad"] = df["Breitengrad"].str.replace(",", ".").str.strip(" .")
    df["Breitengrad"] = pd.to_numeric(df["Breitengrad"])
    # now conversion works fine

    with engine.begin() as conn:
        df.to_sql("ladesaeulenregister", conn, if_exists="replace")
    log.info("Finished writing Ladesäulenregister to Database")


if __name__ == "__main__":
    logging.basicConfig()
    main(db_uri("ladesaeulenregister"))
