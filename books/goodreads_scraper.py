import requests
from urllib.parse import urljoin
import sqlite3
import random
import time
import logging
from datetime import datetime
from dateutil import tz
from bs4 import BeautifulSoup
import os


#def fix_ownership(path):
#   """Change the owner of the file to SUDO_UID"""

#    uid = os.environ.get('SUDO_UID')
#    gid = os.environ.get('SUDO_GID')
#    if uid is not None:
#        os.chown(path, int(uid), int(gid))


#if not os.path.exists('goodreads_scraper.log'):
#    open('goodreads_scraper.log', 'a').close()
#    fix_ownership('goodreads_scraper.log')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

file_handler = logging.FileHandler('goodreads_scraper.log', mode='a')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

    
def get_book_data(db, url, base_url):
    db.execute("INSERT OR IGNORE INTO raw_responses(link) VALUES (?)",
               (url,))
    db.commit()

    while True:
        to_retrieve = [row[0] for row in
                       db.execute(
                           """
                           SELECT link FROM raw_responses
                           WHERE retrieval_time IS NULL
                           """)
                       ]
        if not to_retrieve:
            logger.info("No more url to retrieve...")
            break

        else:
            for i, link in enumerate(to_retrieve):
                try:
                    logger.info(f"""
                                Retrieving {link}...
                                Progress: {i + 1}/{len(to_retrieve)}
                                """.strip())

                    headers = {'user-agent': 'my-book-collection/0.0.1'}
                    r = requests.get(link, headers=headers)
                    r.raise_for_status()

                    logger.info('INSERTING RAW HTML TO DATABASE...')

                    with db:
                        db.execute(
                            """
                            REPLACE INTO raw_responses VALUES (?,?,?)
                            """,
                            (link, r.text, datetime.now(tz=tz.UTC))
                        )
                except Exception as e:
                    logger.error(f"{e}: {link}.")

                logger.info('GETTING NEW LINKS...')

                parsed = BeautifulSoup(r.text, 'html.parser')
                selector = 'a[href*="?page="],a.bookTitle,a.listTitle'
                new_links = parsed.select(selector)

                if not new_links:
                    logger.warning("NO LINKS FOUND...")

                else:
                    logger.info(f"FOUND NEW LINKS: {len(new_links)} LINKS.")

                    for j, new_link in enumerate(new_links):
                        try:
                            if (r'book/show/' in new_link['href'] or
                                    r'list/show/' in new_link['href'] or
                                    r'list/tag/' in new_link['href']):

                                logger.info(
                                  f"""
                                  INSERTING NEW LINKS {j + 1}/{len(new_links)}
                                  """)

                                new_href = urljoin(base_url, new_link['href'])

                                logger.info(f"INSERTING {new_href}...")

                                with db:
                                    db.execute(
                                        """
                                        INSERT OR IGNORE INTO 
                                        raw_responses(link)
                                        VALUES (?)
                                        """,
                                        (new_href,))
                        except Exception as e:
                            logger.error(f"{e}: {new_link}")

                time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    base_url = r'https://www.goodreads.com/'
    start_url = r'https://www.goodreads.com/list/tag/non-fiction'

    db = sqlite3.connect('goodreads_raw.db')
#    fix_ownership('goodreads_raw.db')

    db.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_responses(
            link primary key,
            response,
            retrieval_time
            )
        """)
    db.commit()

    get_book_data(db=db, url=start_url, base_url=base_url)
    db.close()
