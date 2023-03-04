import argparse
import sqlite3
import pathlib
import pandas
from .libs import owner
from .libs import channel


parser = argparse.ArgumentParser()
parser.add_argument('--db_name', type=pathlib.Path, default=":memory:")
parser.add_argument('--channel_csv', type=pathlib.Path, required=True)

args = parser.parse_args()


connection = sqlite3.Connection(args.db_name)
cursor = connection.cursor()
cursor.execute("PRAGMA foreign_keys=true")


owner.make_table(connection, pandas.read_csv(args.channel_csv))
channel.make_table(connection, pandas.read_csv(args.channel_csv))


connection.commit()
connection.close()