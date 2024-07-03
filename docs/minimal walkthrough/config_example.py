# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

def db_uri_local_default(db_name):
    return (
        "postgresql://opendata:opendata@localhost:6432/opendata?options=--search_path="
        + db_name
    )

db_uri = db_uri_local_default
