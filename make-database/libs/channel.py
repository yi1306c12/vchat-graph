import pandas
import sqlite3
from . import table
from . import owner
from . import select


TABLE_NAME = __name__.split('.')[-1]


def make_table(conn: sqlite3.Connection, channel_df: pandas.DataFrame):
    cursor = conn.cursor()
    channel_df["ownerNo"] = [select.number(cursor=cursor, table_name=owner.TABLE_NAME, where_column="ownerName", word=name) for name in channel_df.pop("ownerName")]

    with conn:
        table.make_table(
            table_name=TABLE_NAME,
            conn=conn,
            df=channel_df,
            uniques=["channelId"],
            foreign_tables=[owner.TABLE_NAME]
        )
