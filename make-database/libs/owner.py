import functools
import sqlite3
import pandas
from . import sql


TABLE_NAME = __name__.split('.')[-1]
UNIQUES = ["ownerName"]
FOREIGN_TABLES = []

def make_table(conn: sqlite3.Connection, channel_df: pandas.DataFrame):
    owner_df = channel_df[["ownerName"]]
    with conn:
        conn.execute(
            sql.create(
                table_name=TABLE_NAME, df=owner_df, uniques=UNIQUES, foreign_tables=FOREIGN_TABLES
            )
        ),
        conn.executemany(
            sql.insert(
                table_name=TABLE_NAME, df=owner_df, insert_or_ignore=True
            ),
            [value for value in owner_df.values]
        )