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
                rating_0_count,
                rating_1_count,
                rating_2_count,
                rating_3_count,
                rating_4_count,
                quotes_count,
                questions_count,
                topics_count
                );
                
            CREATE TABLE IF NOT EXISTS archive_meta (
                archive_id PRIMARY KEY,
                process_date,
                volume
                );
            """
        )


def insert_data_sqlite(conn: sqlite3.Connection,
                       mapping: dict,
                       schema: str) -> None:
    valid_schemas = ["books", "awards", "authors"]
    if schema not in valid_schemas:
        raise ValueError(f"Schema has to be one of {valid_schemas}.")

    if schema == "books":
        logger.info(f"Insert data to {schema} schema...")
        conn.execute("BEGIN")
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
                :rating_0_count,
                :rating_1_count,
                :rating_2_count,
                :rating_3_count,
                :rating_4_count,
                :quotes_count,
                :questions_count,
                :topics_count
                )
            """,
            mapping
        )
        conn.execute("COMMIT")

