import os
import sqlite3
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
from dateutil import tz
from mysql import connector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('goodreads_clean_html.log')
file_handler.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


def get_book_id(url):
    pattern = re.compile(r"(book/show/)([^A-z-\.]+)")
    book_id = re.search(pattern, url)[2]

    return book_id


def get_number(text):
    pattern = re.compile(r"([0-9,]+)")
    number = re.search(pattern, text)[1]
    number = re.sub(',', '', number)

    return int(number)


def get_url(db: sqlite3.Connection):
    urls = db.execute("""
        SELECT link 
        FROM raw_responses 
        WHERE link like '%book/show%'
          AND retrieval_time IS NOT NULL
        """)
    return urls


def process_string(string):
    string = re.sub("(.+)", "'\\1'", string)
    return string


def clean_to_database(clean, raw, staging):
    staging.executescript(
        """
        CREATE TABLE IF NOT EXISTS staging(
            url PRIMARY KEY,
            process_at
            );
            
        CREATE TABLE IF NOT EXISTS error_links(
            url PRIMARY KEY,
            status
            );
        """
    )
    staging.commit()

    cur = clean.cursor()

    while True:
        logger.info("UPDATE RAW RESPONSES FOR PROCESSING...")
        urls = get_url(db=raw)

        for url in urls:
            logger.debug(f"INSERTING {url}...")
            with staging:
                staging.execute(
                    """
                    INSERT OR IGNORE
                    INTO staging(url)
                    VALUES (?)
                    """,
                    (url[0],))

        logger.info("RETRIEVING RAW RESPONSES FOR PROCESSING...")
        to_clean = [row[0]
                    for row in
                    staging.execute(
                        """
                        SELECT url 
                        FROM staging
                        WHERE process_at IS NULL
                        """)]

        if not to_clean:
            logger.warning("NO MORE RAW RESPONSES TO PROCESS...")
            break

        else:
            to_clean = [process_string(string)
                        for string in to_clean]

            raw_response = list(raw.execute(
                            f"""
                            SELECT link, response
                            FROM raw_responses
                            WHERE link IN ({','.join(to_clean)})
                              AND retrieval_time NOTNULL
                            """))
            raw.close()

            for row in raw_response:
                link = row[0]
                response = row[1]

                logger.info("PROCESSING RESPONSES...")
                logger.debug(f"Processing url {link}...")

                try:
                    parsed = BeautifulSoup(response, features="html.parser")
                except TypeError:
                    logger.warning(f"""
                        The link {link} has not been re-recorded, and
                        there's no response to scrape.
                        """)
                    continue

                try:
                    book_id = get_book_id(link)

                    title = parsed.find(id='bookTitle')
                    title = title.text.strip()
                    logger.info(f'Getting book {book_id}: {title}')

                    book_series = parsed.find(id='bookSeries')
                    if book_series:
                        book_series = book_series.text.strip()

                    authors = parsed.findAll('a', class_='authorName')
                    authors = [author.text
                               for author in
                               authors] if authors else None
                    # author_links = [author['href']
                    #                 for author in
                    #                 authors] if authors else None

                    ratings = parsed.find('span', itemprop='ratingValue')
                    ratings = float(ratings.text.strip()) if ratings else None

                    rating_count = parsed.find('meta', itemprop='ratingCount')
                    if rating_count:
                        rating_count = get_number(rating_count.text.strip())

                    review_count = parsed.find('meta', itemprop='reviewCount')
                    if review_count:
                        review_count = get_number(review_count.text.strip())

                    description = parsed.find(id='description')
                    if description:
                        description = description.find(style='display:none')
                        description = description.text.strip()

                    book_format = parsed.find('span', itemprop='bookFormat')
                    if book_format:
                        book_format = book_format.text

                    book_edition = parsed.find('span', itemprop='bookEdition')
                    if book_edition:
                        book_edition = book_edition.text

                    number_of_pages = parsed.find('span',
                                                  itemprop='numberOfPages')
                    if number_of_pages:
                        number_of_pages = get_number(number_of_pages.text)

                    awards = parsed.findAll('a', class_='award')
                    if awards:
                        awards = [award.text for award in awards]

                    isbn_13 = parsed.find('span', itemprop="isbn")
                    isbn_13 = isbn_13.text if isbn_13 else None

                    logger.info("INSERT CLEANED DATA TO DATABASE...")
                    cur.execute(
                            """
                            REPLACE INTO goodreads_books
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                                    %s,%s)
                            """,
                            (book_id, link, datetime.now(tz=tz.UTC), title,
                             book_series, str(authors), ratings, rating_count,
                             review_count, description, book_format,
                             book_edition, number_of_pages, str(awards),
                             isbn_13)
                        )
                    clean.commit()

                    with staging:
                        staging.execute(
                            """
                            REPLACE INTO staging
                            VALUES (?,?)
                            """,
                            (link, datetime.now(tz=tz.UTC))
                        )

                except Exception as e:
                    logger.error(f"{e}: {link}")
                    logger.warning(f"""
                        Potential scraping error: {link}.
                        Reinserting link back to raw database
                        to be rescraped.
                        """)
                    clean.rollback()
                    with staging:
                        staging.execute(
                            """
                            REPLACE INTO error_links(url)
                            VALUES (?)
                            """,
                            (link,))


if __name__ == "__main__":
    raw_db_path = 'goodreads_raw.db'
    raw_db = sqlite3.connect(raw_db_path)

    staging_path = 'goodreads_staging.db'
    staging = sqlite3.connect(staging_path)
    # clean_db_path = 'goodreads_cleaned.db'
    # clean_db = sqlite3.connect(clean_db_path)

    clean_db = connector.connect(user=os.environ['DB_USERNAME'],
                                 database='boyd_nguyen',
                                 password=os.environ['DB_PASSWORD'],
                                 host='34.82.140.19')

    clean_to_database(clean=clean_db, raw=raw_db, staging=staging)
    clean_db.close()
    raw_db.close()
    staging_path.close()











