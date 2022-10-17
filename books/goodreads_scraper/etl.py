import os
import sqlite3
from pathlib import Path
import time
from bs4 import BeautifulSoup
import logging
import re
from datetime import datetime
from dateutil import tz
from typing import Iterable, Union
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



def _get_book_id(url) -> str:
    pattern = re.compile(r"(book/show/)([^A-z-\.]+)")
    book_id = re.search(pattern, url)[2]

    return book_id


def _get_number(text) -> int:
    pattern = re.compile(r"([0-9,]+)")
    number = re.search(pattern, text)[1]
    number = re.sub(',', '', number)

    return int(number)


def _get_url(db_conn: sqlite3.Connection) -> Iterable:
    urls = db_conn.execute("""
        SELECT link 
        FROM raw_responses 
        WHERE link like '%book/show%'
        """)
    return urls


def process_string(string) -> str:
    string = re.sub("(.+)", "'\\1'", string)
    return string


def clean_to_database(clean_conn: sqlite3.Connection,
                      raw_conn: sqlite3.Connection) -> None:
    databases.create_sqlite_schema(clean_conn, schema="books")

    logger.info("UPDATE RAW RESPONSES FOR PROCESSING...")

    logger.info("RETRIEVING RAW RESPONSES FOR PROCESSING...")
    to_clean = raw_conn.execute(
        f"""
        SELECT link, response
        FROM raw_responses
        WHERE link LIKE '%/book/show/%'
        """)

    for row in to_clean:
        link = row[0]
        response = row[1]

        logger.info(f"Processing url {link}...")

        try:
            parsed = BeautifulSoup(response, features="html.parser")
        except TypeError:
            logger.error(f"""
                The link {link} has not been re-recorded, and
                there's no response to scrape.
                """)
            continue

        data_map = _book_mapping()
        data = parsed.find("script", id="__NEXT_DATA__")

        logger.info(f'Getting book {link}')

        data_map["book_id"] = _get_book_id(link)

        if data:
            logger.debug(f"Parsing JSON...")
            data = json.loads(data.text)
            parent_dict = data['props']['pageProps']['apolloState']

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
            data_map["publication_time"] = \
                book_details["publicationTime"] / 1000 if book_details[
                    "publicationTime"] else None

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
            data_map["rating_1_count"] = stats["ratingsCountDist"][0]
            data_map["rating_2_count"] = stats["ratingsCountDist"][1]
            data_map["rating_3_count"] = stats["ratingsCountDist"][2]
            data_map["rating_4_count"] = stats["ratingsCountDist"][3]
            data_map["rating_5_count"] = stats["ratingsCountDist"][4]

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

        else:
            data = parsed.find(id="metacol")

            if not data:
                logger.error(f"ERROR CANNOT PARSE THIS: {link}")
                continue

            data_map["title"] = data.find(id="bookTitle").text.strip()

            description = data.find(id="description")
            if description:
                description_text = description.find_all("span")[-1].text
                data_map["description"] = description_text

            data_map["link"] = link

            book_format = data.find(attrs={'itemprop': 'bookFormat'})
            if book_format:
                data_map["format"] = book_format.text

            num_pages = data.find(attrs={'itemprop': 'numberOfPages'})
            if num_pages:
                data_map["num_pages"] = _get_int(num_pages.text)[0]

            details = data.find(id="details").find(class_="row",
                                                   string=re.compile(
                                                       "published",
                                                       re.IGNORECASE))
            if details:
                publication_list = details.text.strip().split('\n')
                publication_time = publication_list[1].strip()
                try:
                    data_map["publication_time"] = _convert_to_timestamp(
                        publication_time)
                except ValueError:
                    logger.error(f"{link}: cannot parse date")
                try:
                    publisher = publication_list[2].strip()
                    data_map["publisher"] = _get_publisher(publisher)
                except IndexError:
                    logger.error(f"{link}: cannot get publisher")

            book_data_box = data.find(id="bookDataBox")

            series = book_data_box.find("div", string="Series")
            if series:
                series_title = series.find_next_sibling("div").text.strip()
                series_url = series.find_next_sibling("div").find("a")["href"]

                base_url = "https://www.goodreads.com"

                data_map["book_series_title"] = series_title
                data_map["book_series_url"] = base_url + series_url

            isbn = book_data_box.find("div", string="ISBN")

            data_map["isbn_13"] = None
            if isbn:
                isbn = isbn.find_next_sibling("div")
                if 'ISBN13' in isbn.text.strip():
                    data_map["isbn_13"] = isbn.text.strip().split()[2].replace(
                        ')',
                        '')

            data_map["isbn"] = isbn.text.strip().split()[0] if isbn else None
            language = book_data_box.find(attrs={"itemprop": "inLanguage"})
            data_map["language"] = language.text if language else None

            awards = book_data_box.find(attrs={"itemprop": "awards"})
            if awards:
                if awards.find_all(awards.find_all("a", class_="award")):
                    num_of_awards = len(awards.find_all("a", class_="award"))
                    data_map["num_of_awards"] = num_of_awards

            average_rating = data.find(attrs={"itemprop": "ratingValue"})
            if average_rating:
                data_map["average_rating"] = float(average_rating.text)

            ratings_count = data.find(attrs={"itemprop": "ratingCount"})
            data_map["ratings_count"] = int(ratings_count['content'])

            reviews_count = data.find(attrs={"itemprop": "reviewCount"})
            data_map["reviews_count"] = int(reviews_count["content"])

            ratings_info = parsed.find(id="reviewControls")
            ratings_info_list = ratings_info.find("script").text.split('\n')

            for string in ratings_info_list:
                if _is_rating_list(string):
                    rating_list = _process_rating_list(string=string)

            data_map["rating_1_count"] = int(rating_list[4])
            data_map["rating_2_count"] = int(rating_list[3])
            data_map["rating_3_count"] = int(rating_list[2])
            data_map["rating_4_count"] = int(rating_list[1])
            data_map["rating_5_count"] = int(rating_list[0])

            questions = parsed.find(class_="moreReaderQA")

            if questions:
                questions = questions.find("a")
                data_map["questions_count"] = int(
                    _get_num_question(questions.text))
            else:
                data_map["questions_count"] = 0

        logger.debug("Inserting to database...")
        databases.insert_data_sqlite(clean_conn, data_map, schema="books")


def _update_archive_meta(db_conn: sqlite3.Connection,
                         archive_name: str,
                         archive_volume: int) -> None:
    db_conn.execute("BEGIN")
    db_conn.execute("REPLACE INTO archive_meta VALUES (?, ?, ?)",
                    (archive_name, datetime.now(tz=tz.UTC), archive_volume))
    db_conn.execute("COMMIT")


def _db_in_use(db: sqlite3.Connection):
    return db.in_transaction


def _get_int(string) -> str:
    pattern = '([0-9]+)'
    value = re.findall(pattern=pattern, string=string)

    return value


def _is_rating_list(string) -> Union[bool, None]:
    pattern = r"\[([0-9]+[,]?\s?)+\]"
    if re.search(pattern=pattern, string=string):
        return True
    else:
        return None


def _process_rating_list(string) -> str:
    pure_digits = re.sub(r"[\[\]]", "", string)

    return pure_digits.strip().split(",")


def _convert_to_timestamp(string) -> int:
    pattern = r"\b([0123]?[0-9])(st|th|nd|rd)\b"
    string = re.sub(pattern, r"\1", string)

    date = datetime.strptime(string, "%B %d %Y")
    date = datetime.date(date)

    return time.mktime(date.timetuple())


def _get_publisher(string) -> str:
    return re.sub(r"(^by\b\s)(.+)", r"\2", string)


def _get_num_question(string) -> str:
    pattern = r"\b([0-9]+)\b (question)"

    return re.search(pattern, string).group(1)


def _book_mapping() -> dict:
    mapping = {key: None for key in
               ["book_id",
                "title",
                "description",
                "link",
                "book_series_title",
                "book_series_url",
                "format",
                "num_pages",
                "publication_time",
                "publisher",
                "isbn",
                "isbn_13",
                "language",
                "num_of_awards",
                "average_rating",
                "ratings_count",
                "reviews_count",
                "rating_1_count",
                "rating_2_count",
                "rating_3_count",
                "rating_4_count",
                "rating_5_count",
                "quotes_count",
                "questions_count",
                "topics_count"]
               }

    return mapping


if __name__ == "__main__":
    now = time.time()

    while True:
        check_time = time.time() + (60 * 15)

        files = os.listdir("archive")

        db = databases.connect_db("clean_16_oct.db")
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
            _update_archive_meta(db, archive_id[0], 123)

        if time.time() < check_time:
            logger.info("SLEEPING NOW")
            time.sleep(check_time - time.time())
