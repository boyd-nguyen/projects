from mysql import connector
import sqlite3

raw_db_path = 'goodreads_raw.db'
raw_db = sqlite3.connect(raw_db_path)

