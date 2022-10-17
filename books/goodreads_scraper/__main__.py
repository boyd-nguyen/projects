import argparse
import logging
import sys
import time
import os
from pathlib import Path

import scraper
import etl
import databases

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

error_log = logging.FileHandler('error.log', mode='a')
error_log.setLevel(logging.ERROR)
error_log.setFormatter(formatter)

fh = logging.FileHandler("scraper.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)
logger.addHandler(error_log)

parser = argparse.ArgumentParser()
parser.add_argument("dbname",
                    help="Name of the database to store raw responses")

args = parser.parse_args()


def etl(clean_db: str, folder="archive"):
    while True:
        check_time = time.time() + (60 * 15)

        files = os.listdir(folder)

        clean_db = databases.connect_db(clean_db)
        databases.create_sqlite_schema(clean_db, schema="books")

        for file in files:

            if Path(file).suffix != ".db":
                continue

            clean_db.execute("BEGIN")
            clean_db.execute(
                """
                INSERT OR IGNORE INTO archive_meta(archive_id) VALUES (?)
                """,
                (file,))
            clean_db.execute("COMMIT")

        to_process = clean_db.execute(
            """
            SELECT archive_id FROM archive_meta
            WHERE process_date IS NULL
            """)

        for archive_id in to_process:
            archive = databases.connect_db("archive/" + archive_id[0])
            logger.info(f"Processing {archive_id[0]}")
            etl.clean_to_database(clean_conn=clean_db, raw_conn=archive)
            etl.update_archive_meta(clean_db, archive_id[0], 123)

        if time.time() < check_time:
            logger.info("SLEEPING NOW")
            time.sleep(check_time - time.time())


if __name__ == "__main__":
    base_url = r'https://www.goodreads.com/'
    start_url = r'https://www.goodreads.com/list/tag/non-fiction'

    db = scraper.connect_db(db_name=args.dbname)
    try:
        scraper.get_book_data(raw_db=db, url=start_url, base_url=base_url)
    except KeyboardInterrupt:
        db.close()
        sys.exit(0)
    finally:
        db.close()
