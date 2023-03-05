import sqlite3


def number(cursor: sqlite3.Cursor, table_name: str, where_column: str, word: str):
    selected = cursor.execute(
        "SELECT {0}No FROM {0} WHERE {1}='{2}'".format(table_name, where_column, word)
    ).fetchall()
    assert len(selected) == 1#TODO: error message
    assert len(selected[0]) == 1#TODO: error message
    return selected[0][0]
