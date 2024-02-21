# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

# place a valid base host with credentials in this function
# for example


def db_uri_postgresql(db_name):
    return (
        "postgresql://username:password@host:5432/opendata?options=--search_path="
        + db_name
    )


def db_uri_sqlite(db_name):
    return "sqlite:///" + db_name + ".db"


db_uri = db_uri_postgresql
