import logging
import pandas as pd
from base_crawler import BasicDbCrawler
from config import db_uri
import requests
import zipfile
import io
import openpyxl
import os

log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


class IwuCrawler(BasicDbCrawler):
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
            self.assign_columns(iwu_data)
            iwu_data.drop(range(0, 13), inplace=True)

            # fill nan & reset index
            iwu_data.ffill(inplace=True)
            iwu_data.bfill(inplace=True)
            iwu_data.reset_index(drop=True, inplace=True)
            return iwu_data
        else:
            log.info("Failed to download the ZIP file")
            return []

    def sendData(self, data):
        with self.db_accessor() as conn:
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


if __name__ == "__main__":
    logging.basicConfig()
    # database = 'sqlite:///data/entsog.db'
    database = db_uri("iwu_gebaeudetypen")
    craw = IwuCrawler(database)
    data = craw.pullData()
    craw.sendData(data)
    log.info("Done")
    log.info(data)
