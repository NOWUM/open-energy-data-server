# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import create_engine

from .config import db_uri

log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


class IwuCrawler:
    def __init__(self, db_uri):
        self.engine = create_engine(db_uri)

    def pullData(self):
        url = "https://www.iwu.de/fileadmin/tools/tabula/TABULA-Analyses_DE-Typology_DataTables.zip"
        response = requests.get(url)
        if response.status_code == 200:
            # load, read and close the zip file
            z = zipfile.ZipFile(io.BytesIO(response.content))
            iwu_data = pd.read_excel(
                z.open("TABULA-Analyses_DE-Typology_ResultData.xlsx"),
                sheet_name="DE Tables & Charts",
            )
            z.close()
            # Drop unrelated columns, rows and assign column names
            iwu_data.drop(columns=iwu_data.columns[75:], inplace=True)
            iwu_data.drop(columns=iwu_data.columns[0:51], inplace=True)
            iwu_data.drop(range(13), inplace=True)
            iwu_data.ffill(inplace=True)
            iwu_data.bfill(inplace=True)

            self.assign_columns(iwu_data)

            iwu_data["Sanierungsstand"] = iwu_data.apply(
                self.set_sanierungsstand, axis=1
            )
            iwu_data["Heizklasse"] = iwu_data.apply(self.set_heizmittel, axis=1)
            iwu_data["IWU_ID"] = iwu_data.apply(self.create_identifier, axis=1)

            self.handle_dates(iwu_data)

            # fill nan & reset index
            iwu_data.reset_index(drop=True, inplace=True)
            return iwu_data
        else:
            log.info("Failed to download the ZIP file")
            return []

    def handle_dates(self, iwu_data):
        datecol = iwu_data[iwu_data.columns[5]]
        # Extract starting year
        fromcol = datecol.str.extract(r"(\d{4})", expand=False)
        fromcol = fromcol.replace("1859", "1800")
        fromcol = pd.to_datetime(fromcol, format="%Y")
        # Extract ending year
        untilcol = datecol.str.extract(r"(\d{4})$", expand=False)
        untilcol = untilcol.fillna("2023")
        untilcol = pd.to_datetime(untilcol, format="%Y")
        # split date into 2 columns by ...
        iwu_data.insert(5, "Baualtersklasse_von", fromcol)
        iwu_data.insert(6, "Baualtersklasse_bis", untilcol)

    def set_sanierungsstand(self, row):
        variante = row["Gebäude_variante"]
        sanierungsstand = variante[2]
        if sanierungsstand == "1":
            sanierungsstand = "Unsaniert"
        elif sanierungsstand == "2":
            sanierungsstand = "Saniert"
        else:
            sanierungsstand = "Modern"
        return sanierungsstand

    def set_heizmittel(self, row):
        variante = row["Gebäude_variante"]
        heizmittel = variante[1]
        if heizmittel == "0":
            heizmittel = "Gas"
        elif heizmittel == "1":
            heizmittel = "Bio"
        else:
            heizmittel = "Strom"
        return heizmittel

    def create_identifier(self, row):
        baualater = row["Baualtersklasse"]
        verfahren = row["Rechenverfahren"]

        baualater = baualater.replace(" ... ", "-")
        baualater = baualater.replace("- ...", "")
        baualater = baualater.replace("... -", "")

        verfahren = verfahren.replace(
            "TABULA Berechnungsverfahren / Standardrandbedingungen", "A"
        )
        verfahren = verfahren.replace(
            "TABULA Berechnungsverfahren / korrigiert auf Niveau von Verbrauchswerten",
            "B",
        )

        # Construct identifier value
        identifier = (
            row["Gebäude_typ_klasse"]
            + "_"
            + baualater
            + "_"
            + row["Sanierungsstand"]
            + "_"
            + row["Heizklasse"]
            + "_"
            + verfahren
        )

        return identifier

    def sendData(self, data):
        with self.engine.begin() as conn:
            tbl_name = "IWU_Typgebäude"
            data.to_sql(tbl_name, conn, if_exists="replace")

    def assign_columns(self, df):
        df.columns = [
            "Rechenverfahren",
            "Gebäude_variante_klasse",
            "Gebäude_typ_klasse",
            "Gebäude_typ",
            "Kombination_ID",
            "Baualtersklasse",
            "Gebäude_variante",
            "Heiz_klasse",
            "Tabula EBZ_m2",
            "Wohnfläche_m2",
            "Wärmetransferkoeffizient_Hüllfläche_W/(m2K)",
            "Wärmetransferkoeffizient_Wohnfläche_W/(m2K)",
            "Nutzwärme_Nettoheizwärmebedarf_kWh/(m2a)",
            "Nutzwärme_Warmwasser_kWh/(m2a)",
            "Warmwassererzeugung_Heizung_kWh/(m2a)",
            "Warmwassererzeugung_Warmwasser_kWh/(m2a)",
            "Endenergiebedarf_fossil_kWh/(m2a)",
            "Endenergiebedarf_holz_bio_kWh/(m2a)",
            "Endenergiebedarf_strom_kWh/(m2a)",
            "Endenergiebedarf_strom_erzeugung_kWh/(m2a)",
            "Primärenergiebedarf_gesamt_kWh/(m2a)",
            "Primärenergiebedarf_nicht_erneuerbar_kWh/(m2a)",
            "Co2_Heizung_ww_kg/(m2a)",
            "Energiekosten_Heizung_ww_€/(m2a)",
        ]


def main(db_uri):
    logging.basicConfig()
    craw = IwuCrawler(db_uri)
    data = craw.pullData()
    craw.sendData(data)

if __name__ == "__main__":
    main(db_uri("iwugebaeudetypen"))
