# SPDX-FileCopyrightText: Bing Zhe Puah
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import os.path as osp

import pandas as pd
from sqlalchemy import text

from common.base_crawler import BaseCrawler

log = logging.getLogger("iwu")
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "prosumer_uebersicht",
    "data_date": "2024-10-16",
    "data_source": "generated",
    "license": "third party usage allowed",
    "description": "Fernwärme Preisübersicht.",
    "contact": "aliseyko@fh-aachen.de",
    "temporal_start": "2022-01-01",
    "temporal_end": "2024-01-04",
    "concave_hull_geometry": None,
}


class FWCrawler(BaseCrawler):
    def __init__(self, schema_name):
        super().__init__(schema_name)

    def pull_data(self):
        base_path = osp.join(osp.dirname(__file__), "data")

        prosumer_path = osp.join(base_path, "prosumer.csv")
        df = pd.read_csv(prosumer_path)
        print(df)
        return df

    def write_to_sql(self, data):
        with self.engine.begin() as conn:
            tbl_name = "prosumer_zr"
            data.to_sql(tbl_name, conn, if_exists="replace")

        try:
            with self.engine.begin() as conn:
                query = text(
                    "SELECT public.create_hypertable('prosumer', 'Date', if_not_exists => TRUE, migrate_data => TRUE);"
                )
            conn.execute(query)
            log.info("created hypertable frequency")
        except Exception as e:
            log.error(f"could not create hypertable: {e}")


def main(schema_name):
    iwu = FWCrawler(schema_name)
    data = iwu.pull_data()
    iwu.write_to_sql(data)


if __name__ == "__main__":
    main("prosumer")
