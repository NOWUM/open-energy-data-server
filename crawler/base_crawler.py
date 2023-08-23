'''
helper functions for crawling open access data into a database
'''

from contextlib import contextmanager
from sqlalchemy import create_engine
from contextlib import closing
import sqlite3
import logging

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
                with self.engine.connect() as conn, conn.begin():
                    yield conn

            self.db_accessor = access_db
        except Exception as es:
            logging.error(f"did not use sqlalchemy connection, using sqlite3 instead {es}")
            self.db_accessor = lambda: closing(sqlite3.connect(database))