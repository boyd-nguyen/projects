import os
import json
import random
import time
import requests
import sqlite3
import argparse
from datetime import datetime
import logging
from urllib.parse import urlencode

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s: %(message)s.")

parser = argparse.ArgumentParser(description=
                                 """
        This script collects book data from Google Books API matching specified keywords.
        
        Collected raw data and processed data are stored in the specified SQLite database, in api_response
        and google_books, respectively.
        
        Requires a Google Books API key put in a GOOGLE_BOOK_API_KEY environment variable.
                                 """, formatter_class=argparse.RawTextHelpFormatter)

parser.add_argument("dbpath", help="Filepath of the database")
parser.add_argument("query", help="Search term")
args = parser.parse_args()

dbpath = args.dbpath
search_query = args.query

if os.path.exists(dbpath):
    user_input = input("This database already exists. Continue using the same database? [Y/N]").lower()
    if user_input == 'y':
        logging.info(f"Updating existing database {dbpath}...")
        pass
    else:
        dbpath = input("Choose a new database:")

db = sqlite3.connect(dbpath)

# Step 1: Query data from Google API

logging.info("Creating api_response schema...")

with db:
    db.execute(f"""
        create table if not exists api_response(
            request_url,
            search_term,
            status,
            item_count,
            response,
            retrieval_time)
    """)
logging.info("api_response schema created...")

url = "https://www.googleapis.com/books/v1/volumes"

params = {
    'q': search_query,
    'maxResults': 40,
    'langRestrict': 'en',
    'orderBy': 'newest',
    'printType': 'books',
    'key': os.environ['GOOGLE_BOOK_API_KEY'],
}

logging.info("Start querying data from Google API...")

while True:
    with db:
        results_retrieved = sum([row[0]
                                 for row in
                                 db.execute("select item_count from api_response where search_term = ?",
                                            (search_query,))])

    if results_retrieved > 0:
        params['startIndex'] = results_retrieved

    logging.info(f"Getting request: {url}{urlencode(params)}...")
    r = requests.get(url, params=params)
    r.raise_for_status()

    total_items = r.json()['totalItems']

    if 'items' not in r.json():
        logging.info('No more books to get.')
        break

    elif 'items' in r.json():
        item_count = len(r.json()['items'])

        logging.info("Inserting data to database...")
        with db:
            db.execute(
                """
                insert or ignore into api_response(request_url, search_term, status, item_count, response, retrieval_time)
                values (?,?,?,?,?,?)
                """,
                (r.url, search_query, r.status_code, item_count, json.dumps(r.json()), datetime.utcnow()))

    sleep = random.uniform(3, 6)
    time.sleep(sleep)

# Step 2: process data

logging.info("Processing json data...")

db.execute(
    """
    create table if not exists google_books(
    id primary key,
    title,
    subtitle,
    authors,
    publisher,
    publishDate,
    description,
    isbn_10,
    isbn_13,
    issn,
    pageCount,
    height,
    width,
    thickness,
    mainCategory,
    categories,
    averageRating,
    ratingCount,
    saleCountry,
    saleability,
    onSaleDate,
    isEbook,
    listPrice,
    lpCurrencyCode,
    retailPrice,
    rPCurrencyCode,
    searchText
    )
    """
)

responses = db.execute("select response from api_response")

batch = 1

for row in responses:

    logging.info(f"Processing batch {batch}...")
    items = json.loads(row[0])['items']

    for i, item in enumerate(items):
        logging.info(f"Processing book {i + 1}/{len(items)}")
        book_id = item['id']
        title = item['volumeInfo']['title']
        subtitle = item['volumeInfo'].get('subtitle')
        authors = str(item['volumeInfo'].get('authors'))
        publisher = item['volumeInfo'].get('publisher')
        publishedDate = item['volumeInfo'].get('publishedDate')
        description = item['volumeInfo'].get('description')
        issn = None
        isbn_10 = None
        isbn_13 = None
        if item['volumeInfo'].get('industryIdentifiers'):
            for inId in item['volumeInfo']['industryIdentifiers']:
                if inId['type'] == "ISBN_10":
                    isbn_10 = inId['identifier']
                elif inId['type'] == "ISBN_13":
                    isbn_13 = inId['identifier']
                elif inId['type'] == "ISSN":
                    issn = inId['identifier']
        pageCount = item['volumeInfo'].get('pageCount')
        height = None
        width = None
        thickness = None
        if item['volumeInfo'].get('dimensions'):
            height = item['volumeInfo']['dimensions'].get('height')
            width = item['volumeInfo']['dimensions'].get('width')
            thickness = item['volumeInfo']['dimensions'].get('thickness')
        mainCategory = item['volumeInfo'].get('mainCategory')
        categories = str(item['volumeInfo'].get('categories'))
        averageRating = item['volumeInfo'].get('averageRating')
        ratingsCount = item['volumeInfo'].get('ratingCount')
        saleInfo = None
        saleCountry = None
        saleability = None
        onSaleDate = None
        isEbook = None
        listPrice = None
        lpCurrencyCode = None
        retailPrice = None
        rPCurrencyCode = None
        if item.get('saleInfo'):
            saleInfo = item.get('saleInfo')
            saleCountry = saleInfo.get('country')
            saleability = saleInfo.get('saleability')
            onSaleDate = saleInfo.get('onSaleDate')
            isEbook = saleInfo.get('isEbook')
            if saleInfo.get('listPrice'):
                listPrice = saleInfo.get('listPrice').get('amount')
                lpCurrencyCode = saleInfo.get('listPrice').get('currencyCode')
            if saleInfo.get('retailPrice'):
                retailPrice = saleInfo.get('retailPrice').get('amount')
                rPCurrencyCode = saleInfo.get('retailPrice').get('rPCurrencyCode')
        searchText = search_query

        with db:
            db.execute(
                """
                insert or ignore into google_books values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                [book_id, title, subtitle, authors, publisher, publishedDate, description, isbn_10,
                 isbn_13, issn, pageCount, height, width, thickness, mainCategory, categories, averageRating,
                 ratingsCount, saleCountry, saleability, onSaleDate, isEbook, listPrice, lpCurrencyCode, retailPrice,
                 rPCurrencyCode, searchText])

    batch += 1

logging.info("Finished.")
