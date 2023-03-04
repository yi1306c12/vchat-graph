import functools
import sqlite3
import pandas
from . import table


TABLE_NAME = __name__.split('.')[-1]


def make_table(conn: sqlite3.Connection, channel_df: pandas.DataFrame):
    table.make_table(
        table_name=TABLE_NAME,
        conn=conn,
        df=channel_df[["ownerName"]],
        uniques=["ownerName"],
        foreign_tables=[]
    )