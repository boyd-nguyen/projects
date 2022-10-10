import argparse
import logging
import sys

import scraper

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

fh = logging.FileHandler("scraper.log")
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

logger.addHandler(ch)
logger.addHandler(fh)

parser = argparse.ArgumentParser()
parser.add_argument("dbname",
                    help="Name of the database to store raw responses")

args = parser.parse_args()

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
