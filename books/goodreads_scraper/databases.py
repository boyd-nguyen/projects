import sqlite3
import logging

logger = logging.getLogger()


def connect_db(db_name: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_name, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL")

    return conn


def create_sqlite_schema(conn: sqlite3.Connection, schema: str) -> None:
    valid_schemas = ["books", "awards", "authors"]
    if schema not in valid_schemas:
        raise ValueError(f"Schema has to be one of {valid_schemas}.")

    if schema == "books":
        logger.info(f"Creating schema `{schema}...")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS books (
                id PRIMARY KEY,
                title,
                description,
                link,
                book_series_title,
                book_series_url,
                format,
                num_pages,
                publication_time,
                publisher,
                isbn,
                isbn_13,
                language,
                num_of_awards,
                average_rating,
                ratings_count,
                reviews_count,
                rating_1_count,
                rating_2_count,
                rating_3_count,
                rating_4_count,
                rating_5_count,
                quotes_count,
                questions_count,
                topics_count
                );
                
            CREATE TABLE IF NOT EXISTS archive_meta (
                archive_id PRIMARY KEY,
                process_date,
                volume
                );
                
            CREATE TABLE IF NOT EXISTS book_author (
                book_id,
                author_id
                );
            """
        )


def insert_data_sqlite(conn: sqlite3.Connection, mapping: dict, schema: str) -> None:

    valid_schemas = ["books", "awards", "authors", "book_author"]
    if schema not in valid_schemas:
        raise ValueError(f"Schema has to be one of {valid_schemas}.")

    conn.execute("BEGIN")
    logger.info(f"Insert data to {schema} schema...")

    if schema == "books":
        conn.execute(
            """
            REPLACE INTO books
            VALUES (
                :book_id,
                :title,
                :description,
                :link,
                :book_series_title,
                :book_series_url,
                :format,
                :num_pages,
                :publication_time,
                :publisher,
                :isbn,
                :isbn_13,
                :language,
                :num_of_awards,
                :average_rating,
                :ratings_count,
                :reviews_count,
                :rating_1_count,
                :rating_2_count,
                :rating_3_count,
                :rating_4_count,
                :rating_5_count,
                :quotes_count,
                :questions_count,
                :topics_count
                )
            """,
            mapping,
        )

    if schema == "book_author":
        conn.execute(
            """
            INSERT INTO book_author
            VALUES (
                :book_id,
                :author_id
            )
            """,
            mapping,
        )

    conn.execute("COMMIT")
