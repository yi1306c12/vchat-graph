import numpy
import pandas
import sqlite3
import logging
from . import returnlog


logger = logging.getLogger(__name__)


TYPE_CONVERT_DICT = {
        numpy.dtype(numpy.object_): 'TEXT',
        numpy.dtype(numpy.int64): 'INTEGER',
}


@returnlog.decorator
def create(table_name: str, df: pandas.DataFrame, uniques: list[str] = [], foreign_tables: list[str] = []) -> str:
    table_keys = [
        "{0}No INTEGER PRIMARY KEY".format(table_name),
        *[name + " " + TYPE_CONVERT_DICT[dtype] for name, dtype in df.dtypes.items()],
        *["FOREIGN KEY ({0}No) REFERENCES {0} ({0}No)".format(name) for name in foreign_tables],
        "UNIQUE ({})".format(', '.join(uniques))
    ]
    return "CREATE TABLE {0} (".format(table_name) + ", ".join(table_keys) + ");"

@returnlog.decorator
def insert(table_name: str, df: pandas.DataFrame, insert_or_ignore=False):
    return "{} INTO {} ({}) VALUES ({})".format(
        "INSERT OR IGNORE" if insert_or_ignore else "INSERT",
        table_name,
        ', '.join(df.keys()),
        ', '.join(["?"] * len(df.keys()))
    )
