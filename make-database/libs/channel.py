import pandas
import sqlite3
from . import sql
from . import owner
from . import select


TABLE_NAME = __name__.split('.')[-1]
UNIQUES = ["channelId"]
FOREIGN_TABLES = [owner.TABLE_NAME]


def make_table(conn: sqlite3.Connection, channel_df: pandas.DataFrame):
    cursor = conn.cursor()
    channel_df["ownerNo"] = [select.number(cursor=cursor, table_name=owner.TABLE_NAME, where_column="ownerName", word=name) for name in channel_df.pop("ownerName")]

    with conn:
        conn.execute(
            sql.create(
                table_name=TABLE_NAME, df=channel_df, uniques=UNIQUES, foreign_tables=FOREIGN_TABLES
            )
        )
        conn.executemany(
            sql.insert(
                table_name=TABLE_NAME, df=channel_df, insert_or_ignore=True
            ),
            [value for value in channel_df.values]
        )
