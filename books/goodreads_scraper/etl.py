import os
import sqlite3
import sys

from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
from dateutil import tz
# from mysql import connector
import json

import databases

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

file_handler = logging.FileHandler('goodreads_clean.log')
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


def clean_to_database(clean: sqlite3.Connection, raw: sqlite3.Connection):
    databases.create_sqlite_schema(clean, schema="books")

    while True:
        logger.info("UPDATE RAW RESPONSES FOR PROCESSING...")
        urls = get_url(db=raw)

        logger.info("RETRIEVING RAW RESPONSES FOR PROCESSING...")
        to_clean = raw.execute(
            f"""
                            SELECT link, response
                            FROM raw_responses
                            WHERE retrieval_time NOTNULL
                            AND link LIKE '%/book/show/%'
                            """)

        if not to_clean:
            logger.warning("NO MORE RAW RESPONSES TO PROCESS...")
            break

        else:
            for row in to_clean:
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
                    logger.info(f'Getting book {link}')
                    data_map = dict()
                    data = parsed.find("script", id="__NEXT_DATA__")
                    data = json.loads(data.text)
                    parent_dict = data['props']['pageProps']['apolloState']

                    data_map["book_id"] = get_book_id(link)
                    data_map["book_series_title"] = None
                    data_map["book_series_url"] = None

                    for key in parent_dict:
                        if 'Book' in key:
                            book_data = parent_dict[key]
                        if 'Work' in key:
                            work_data = parent_dict[key]
                        if 'Series' in key:
                            logger.info(f"Series in key: {'Series' in key}")
                            series_data = parent_dict[key]
                            data_map["book_series_title"] = series_data["title"]
                            data_map["book_series_url"] = series_data["webUrl"]

                    data_map["title"] = book_data["titleComplete"]
                    for key in book_data:
                        if "description" in key:
                            data_map["description"] = book_data[key]
                    data_map["link"] = book_data["webUrl"]

                    book_details = book_data["details"]
                    data_map["format"] = book_details["format"]
                    data_map["num_pages"] = book_details["numPages"]
                    data_map["publication_time"] = book_details[
                        "publicationTime"]
                    data_map["publisher"] = book_details["publisher"]
                    data_map["isbn"] = book_details.get("isbn")
                    data_map["isbn_13"] = book_details.get("isbn_13")
                    data_map["language"] = book_details["language"]["name"]

                    work_details = work_data["details"]
                    data_map["num_of_awards"] = len(
                        work_details["awardsWon"])

                    stats = work_data["stats"]
                    data_map["average_rating"] = stats["averageRating"]
                    data_map["ratings_count"] = stats["ratingsCount"]
                    data_map["reviews_count"] = stats["textReviewsCount"]
                    data_map["rating_0_count"] = stats["ratingsCountDist"][0]
                    data_map["rating_1_count"] = stats["ratingsCountDist"][1]
                    data_map["rating_2_count"] = stats["ratingsCountDist"][2]
                    data_map["rating_3_count"] = stats["ratingsCountDist"][3]
                    data_map["rating_4_count"] = stats["ratingsCountDist"][4]

                    for key in work_data:
                        if "quotes" in key:
                            quote = work_data[key]
                    data_map["quotes_count"] = quote["totalCount"]

                    for key in work_data:
                        if "questions" in key:
                            question = work_data[key]
                    data_map["questions_count"] = question['totalCount']

                    for key in work_data:
                        if "topics" in key:
                            topic = work_data[key]
                    data_map["topics_count"] = topic['totalCount']

                    logger.info("INSERT CLEANED DATA TO DATABASE...")

                    databases.insert_data_sqlite(clean,
                                                 data_map,
                                                 schema="books")

                except Exception as e:
                    logger.error(f"{e}: {link}")


if __name__ == "__main__":
    raw_db = databases.connect_db("test.db")
    clean_db = databases.connect_db("clean.db")
    try:
        clean_to_database(clean=clean_db, raw=raw_db)
        clean_db.close()
    except KeyboardInterrupt:
        clean_db.close()
        sys.exit(0)