import argparse
import logging
import sys
<<<<<<< HEAD
import os
import time
from pathlib import Path
from multiprocessing import Process

import scraper
from etl import clean_to_database, update_archive_meta
=======
import time
import os
from pathlib import Path

import scraper
import etl
>>>>>>> 7550eb8aa90bd14fd4cc1380a6ec7c6c58590f2f
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
parser.add_argument("rawdb",
                    help="Name of the database to store raw responses")
parser.add_argument("cleandb",
                    help="Name of the database to store cleaned data")
args = parser.parse_args()


def master_scraper(db_name=args.rawdb,
                   base_url="https://www.goodreads.com/",
                   start_url="https://www.goodreads.com/list/tag/non-fiction"):

    db = databases.connect_db(db_name=db_name)
    try:
        scraper.get_book_data(raw_db=db, url=start_url, base_url=base_url)
    except KeyboardInterrupt:
        db.close()
        sys.exit(0)
    finally:
        db.close()


def etl(archive_dir="archive", clean_db_name=args.cleandb):
    now = time.time()

    while True:
        check_time = time.time() + (60 * 15)

        try:
            files = os.listdir(archive_dir)
        except FileNotFoundError:
            logger.info("Folder not found. Sleeping...")
            time.sleep(60 * 30)
            continue

        db = databases.connect_db(clean_db_name)
        databases.create_sqlite_schema(db, schema="books")

        for file in files:

            if Path(file).suffix != ".db":
                continue

            db.execute("BEGIN")
            db.execute(
                """
                INSERT OR IGNORE INTO archive_meta(archive_id) VALUES (?)
                """,
                (file,))
            db.execute("COMMIT")

        to_process = db.execute(
            """
            SELECT archive_id FROM archive_meta
            WHERE process_date IS NULL
            """)

        for archive_id in to_process:
            archive = databases.connect_db("archive/" + archive_id[0])
            logger.info(f"Processing {archive_id[0]}")
            clean_to_database(clean_conn=db, raw_conn=archive)
            update_archive_meta(db, archive_id[0], 123)

        if time.time() < check_time:
            logger.info("SLEEPING NOW")
            time.sleep(check_time - time.time())


if __name__ == "__main__":
    p_1 = Process(target=master_scraper,
                  name="scraper")
    p_2 = Process(target=etl,
                  name="etl")
    p_1.start()
    p_2.start()



