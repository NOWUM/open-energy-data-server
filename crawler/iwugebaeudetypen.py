# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import io
import logging
import zipfile

import pandas as pd
import requests

from common.base_crawler import BaseCrawler


log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "iwugebaeudetypen",
    "data_date": "2015-02-10",
    "data_source": "https://www.iwu.de/fileadmin/tools/tabula/TABULA-Analyses_DE-Typology_DataTables.zip",
    "licence": "© Institut Wohnen und Umwelt GmbH",
    "description": "IWU German building types. Building types with energy and sanitation metrics attached.",
    "contact": "",
    "temporal_start": None,
    "temporal_end": None,
    "concave_hull_geometry": None,
}

class IwuCrawler(BaseCrawler):
    def __init__(self, schema_name):
        super().__init__(schema_name)

    def pull_data(self):
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
            iwu_data["Heizklasse"] = iwu_data.apply(
                self.set_heizmittel, axis=1)
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

    def send_data(self, data):
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
            "Wärmetransferkoeffizient_Hüllfläche_W_div_(m2K)",
            "Wärmetransferkoeffizient_Wohnfläche_W_div_(m2K)",
            "Nutzwärme_Nettoheizwärmebedarf_kWh_div_(m2a)",
            "Nutzwärme_Warmwasser_kWh_div_(m2a)",
            "Warmwassererzeugung_Heizung_kWh_div_(m2a)",
            "Warmwassererzeugung_Warmwasser_kWh_div_(m2a)",
            "Endenergiebedarf_fossil_kWh_div_(m2a)",
            "Endenergiebedarf_holz_bio_kWh_div_(m2a)",
            "Endenergiebedarf_strom_kWh_div_(m2a)",
            "Endenergiebedarf_strom_erzeugung_kWh_div_(m2a)",
            "Primärenergiebedarf_gesamt_kWh_div_(m2a)",
            "Primärenergiebedarf_nicht_erneuerbar_kWh_div_(m2a)",
            "Co2_Heizung_ww_kg_div_(m2a)",
            "Energiekosten_Heizung_ww_€_div_(m2a)",
        ]


def main(schema_name):
    logging.basicConfig()
    craw = IwuCrawler(schema_name)
    data = craw.pull_data()
    craw.send_data(data)
    craw.set_metadata(metadata_info)

if __name__ == "__main__":
    main("iwugebaeudetypen")


