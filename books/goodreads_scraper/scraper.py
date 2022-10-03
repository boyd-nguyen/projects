import requests
from urllib.parse import urljoin
import sqlite3
import random
import time
import logging
from datetime import datetime
from dateutil import tz
from bs4 import BeautifulSoup
from typing import Iterable
from pathlib import Path

from databases import connect_db
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

file_handler = logging.FileHandler('../goodreads_scraper.log', mode='a')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)

    
def get_book_data(raw_db: sqlite3.Connection, url: str, base_url: str) -> None:
    _create_schema(raw_db)

    raw_db.execute("BEGIN")
    raw_db.execute("INSERT OR IGNORE INTO raw_responses(link) VALUES (?)",
                   (url,))
    raw_db.execute("COMMIT")

    now = time.time()
    while True:

        flush_time = time.time() + (60*60*30)

        to_retrieve = [row[0] for row in
                       raw_db.execute(
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
                logger.info(f"""
                            Retrieving {link}...
                            Progress: {i + 1}/{len(to_retrieve)}
                            """.strip())

                r = _request_with_error_handling(url=link)

                logger.info('INSERTING RAW HTML TO DATABASE...')
                _update_db(conn=raw_db,
                           link=link,
                           response=r,
                           time_utc=datetime.now(tz=tz.UTC))

                logger.info('GETTING NEW LINKS...')

                new_links = _get_new_links(r)

                if not new_links:
                    logger.warning("NO LINKS FOUND...")

                else:
                    logger.info(
                        f"FOUND NEW LINKS: {len(new_links)} LINKS.")

                    for j, new_link in enumerate(new_links):
                        is_good_link = (r'book/show/' in new_link['href']) or \
                                (r'list/show/' in new_link['href']) or \
                                (r'list/tag/' in new_link['href'])

                        if is_good_link:
                            logger.info(
                                f"""
                                INSERTING NEW LINKS 
                                {j + 1}/{len(new_links)}
                                """)

                            new_href = urljoin(base_url,
                                               new_link['href'])

                            logger.info(f"INSERTING {new_href}...")

                            _insert_link_db(raw_db, link=new_href)

                        if time.time() > flush_time:
                            backup_db_name = Path(
                                f"archive/{int(time.time())}.db")

                            if not backup_db_name.parent.exists():
                                backup_db_name.mkdir(parents=True)

                            backup_db = _create_backup_db(backup_db_name)
                            _back_up_data(backup_db=backup_db, input_db=raw_db)

                time.sleep(random.uniform(2, 3))


def _request_with_error_handling(url: str, **kwargs) -> requests.Response:
    headers = {'user-agent': 'my-book-collection/0.0.1',
               'Cookie': 'ccsid=215-1754205-4322857; locale=en; srb_8=1'}
    try:
        r = requests.get(url, headers=headers, **kwargs)
    except requests.ConnectionError:
        sleep_time = random.uniform(2, 10)
        time.sleep(sleep_time)
    else:
        r.raise_for_status()
    return r


def _update_db(conn: sqlite3.Connection,
               link: str,
               response: requests.Response,
               time_utc: datetime) -> None:
    conn.execute("BEGIN")
    conn.execute(
        """
        REPLACE INTO raw_responses (link, response, retrieval_time)
        VALUES (?,?,?)
        """,
        (link, response.text, time_utc)
    )
    conn.execute("COMMIT")


def _get_new_links(response: requests.Response) -> Iterable:
    selector = 'a[href*="?page="],a.bookTitle,a.listTitle'
    parsed = BeautifulSoup(response.text, 'html.parser')
    new_links = parsed.select(selector)

    return new_links


def _insert_link_db(conn: sqlite3.Connection, link) -> None:
    conn.execute("BEGIN")
    conn.execute(
        """
        INSERT OR IGNORE INTO 
        raw_responses(link)
        VALUES (?)
        """,
        (link,))
    conn.execute("COMMIT")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("BEGIN")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_responses(
            link primary key,
            response,
            retrieval_time,
            archived
            )
        """)
    conn.execute("COMMIT")


def _create_backup_db(db_name: str) -> sqlite3.Connection:
    conn = connect_db(db_name=db_name)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_responses(
            link primary key,
            response,
            backed_up_time
            );
        """
    )

    return conn


def _back_up_data(backup_db: sqlite3.Connection, input_db: sqlite3.Connection):
    logger.info("Backing up data")
    responses = input_db.execute(
        """
        SELECT link, response FROM raw_responses
        WHERE archived IS NULL AND retrieval_time NOT NULL
        """)

    for response in responses:
        backup_db.execute("BEGIN")
        backup_db.execute(
            """
            REPLACE INTO raw_responses 
            VALUES (?,?,?)
            """,
            (response + (datetime.now(tz=tz.UTC),))
        )
        backup_db.execute("COMMIT")

        input_db.execute("BEGIN")
        input_db.execute(
            """
            UPDATE raw_responses
            SET archived = ?
            WHERE link = ?
            """,
            (datetime.now(tz=tz.UTC), response[0])
        )
        input_db.execute("COMMIT")


if __name__ == "__main__":
    base_url = r'https://www.goodreads.com/'
    start_url = r'https://www.goodreads.com/list/tag/non-fiction'

    dbname = "goodreads_raw_test.db"
    db = connect_db(db_name=dbname)
    get_book_data(raw_db=db, url=start_url, base_url=base_url)
    db.close()
