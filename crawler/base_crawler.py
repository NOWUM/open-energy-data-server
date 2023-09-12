"""
helper functions for crawling open access data into a database
"""

import logging
import sqlite3
from contextlib import closing, contextmanager

from sqlalchemy import create_engine


class BasicDbCrawler:
    """
    class to allow easier crawling of oopen data
    abstracts the data base accessor creation

    Parameters
    ----------
    database: str
        database connection string or path to sqlite db
    """

    def __init__(self, database):
        # try sqlalchemy connection first
        # fall back to using sqlite3
        try:
            self.engine = create_engine(database)

            @contextmanager
            def access_db():
                """contextmanager to handle opening of db, similar to closing for sqlite3"""
                with self.engine.begin() as conn:
                    yield conn

            self.db_accessor = access_db
        except Exception as es:
            logging.error(
                f"did not use sqlalchemy connection, using sqlite3 instead {es}"
            )
            self.db_accessor = lambda: closing(sqlite3.connect(database))
