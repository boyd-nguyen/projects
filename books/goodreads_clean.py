import sqlite3
from bs4 import BeautifulSoup
import logging
import glob
import warcio
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

console_formatter = logging.Formatter(
    "%(name)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

logger.addHandler(console_handler)

collection_name = 'collection_2022-06-14'
warc_path = f"collections/{collection_name}/archive/*.warc.gz"

db = sqlite3.connect('goodreads_cleaned.db')

db.executescript(
    """
    CREATE TABLE IF NOT EXISTS books(
        book_id primary key,
        title,
        book_series,
        authors,
        ratings,
        rating_count,
        review_count,
        description,
        book_format,
        book_edition,
        number_of_pages,
        awards,
        isbn13);
    """
)
db.commit()


def get_book_id(url):
    pattern = re.compile(r"(book/show/)([^A-z-]+)")
    book_id = re.search(pattern, url)[2]

    return book_id


def get_number(text):
    pattern = re.compile(r"([0-9,]+)")
    number = re.search(pattern, text)[1]

    return number


def get_warc(warc_path):
    for filename in glob.glob(warc_path):
        logger.info(f"Looking for responses in archive: {filename}")
        print(filename)
        with open(filename, "rb") as f:
            try:
                for record in warcio.ArchiveIterator(f):
                    if record.rec_type == "response":
                        yield record
            except Exception as e:
                print(filename, e)
    else:
        logger.warning(f"No archives found at path {warc_path}...")


warc_collections = get_warc(warc_path=warc_path)

error_urls = []

for record in warc_collections:
    url = record.rec_headers.get_header("WARC-Target-URI")
    try:
        if (url.startswith("https://www.goodreads.com/book/show/")
                and record.http_headers.get_statuscode() == "200"):

            parsed = BeautifulSoup(record.content_stream(),
                                   features="html.parser")

            book_id = get_book_id(url)

            title = parsed.find(id='bookTitle')
            title = title.text.strip()
            logger.warning(f'Getting book {book_id}: {title}')

            book_series = parsed.find(id='bookSeries')
            book_series = book_series.text.strip()

            authors = parsed.findAll('a', class_='authorName')
            authors = [author.text for author in authors]

            ratings = parsed.find('span', itemprop='ratingValue')
            ratings = ratings.text.strip()

            rating_count = parsed.find('meta', itemprop='ratingCount')
            rating_count = get_number(rating_count.text.strip())

            review_count = parsed.find('meta', itemprop='reviewCount')
            review_count = get_number(review_count.text.strip())

            description = parsed.find(id='description')
            description.find('a').replaceWith('')
            description = description.text.strip()

            book_format = parsed.find('span', itemprop='bookFormat')
            book_format = book_format.text

            book_edition = parsed.find('span', itemprop='bookEdition')
            book_edition = book_edition.text if book_edition is not None \
                else None

            number_of_pages = parsed.find('span', itemprop='numberOfPages')
            number_of_pages = get_number(number_of_pages.text)

            awards = parsed.findAll('a', class_='award')
            awards = [award.text for award in awards] if awards is not None \
                else None

            isbn_13 = parsed.find('span', itemprop="isbn")
            isbn_13 = isbn_13.text if isbn_13 is not None else None

            with db:
                db.execute(
                    """
                    INSERT OR IGNORE INTO books
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (book_id, title, book_series, str(authors), ratings,
                     rating_count, review_count, description, book_format,
                     book_edition, number_of_pages, str(awards), isbn_13)
                )

    except Exception as e:
        logger.error(f"Error: {e}. At url {url}.")
        error_urls.append(url)

logger.warning("These URLs fucked up...")
logger.warning(f"{error_urls}...")
db.close()











