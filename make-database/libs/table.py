import numpy
import pandas
import sqlite3
import logging
from . import returnlog


logger = logging.getLogger(__name__)


TYPE_CONVERT_DICT = {
        numpy.dtype(numpy.object_).__class__: 'TEXT',
        numpy.dtype(numpy.int64).__class__: 'INTEGER',
}


@returnlog.decorator
def create_query(table_name: str, df: pandas.DataFrame, uniques: list[str] = [], foreign_tables: list[str] = []) -> str:
    table_keys = [
        "{0}No INTEGER PRIMARY KEY".format(table_name),
        *[name + " " + TYPE_CONVERT_DICT[dtype.__class__] for name, dtype in df.dtypes.items()],
        *["FOREIGN KEY ({0}No) REFERENCES {0} ({0}No)".format(name) for name in foreign_tables],
        "UNIQUE ({})".format(', '.join(uniques))
    ]
    return "CREATE TABLE {0} (".format(table_name) + ", ".join(table_keys) + ");"


def make_table(table_name: str, conn: sqlite3.Connection, df: pandas.DataFrame, uniques: list[str] = [], foreign_tables: list[str] = []) -> None:
    if len(uniques) > 0:
        df = df.drop_duplicates(uniques)
    with conn:
        conn.execute(
            create_query(
                table_name=table_name, df=df, uniques=uniques, foreign_tables=foreign_tables
            )
        )
        df.to_sql(table_name, conn, if_exists='append', index=False)
