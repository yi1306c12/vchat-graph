import logging
from . import intertype
from . import returnlog


logger = logging.getLogger(__name__)


@returnlog.decorator
def create(table_name: str, data_types: dict[str, intertype.InterType], uniques: list[str] = [], foreign_tables: list[str] = []) -> str:
    table_keys = [
        "{0}No INTEGER PRIMARY KEY".format(table_name),
        *[name + " " + it.sql for name, it in data_types.items()],
        *["FOREIGN KEY ({0}No) REFERENCES {0} ({0}No)".format(name) for name in foreign_tables],
        "UNIQUE ({})".format(', '.join(uniques))
    ]
    return "CREATE TABLE {0} (".format(table_name) + ", ".join(table_keys) + ");"

@returnlog.decorator
def insert(table_name: str, keys: list[str], insert_or_ignore: bool = False):
    return "{} INTO {} ({}) VALUES ({})".format(
        "INSERT OR IGNORE" if insert_or_ignore else "INSERT",
        table_name,
        ', '.join(keys),
        ', '.join(["?"] * len(keys))
    )
