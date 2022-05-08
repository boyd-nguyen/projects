import os
import json
import requests
import sqlite3
import argparse
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(".env")
search_query = str()

db = sqlite3.connect(db_path)

with db:
    db.execute(f"""
        create table if not exists api_response(
            request_url,
            query check (query = {search_query}),
            status,
            item_count,
            response,
            retrieval_time)
    """)


base_url = "https://www.googleapis.com/books/v1/volumes"

params = {
    'q': search_query,
    'maxResults': 40,
    'langRestrict': 'en',
    'orderBy': 'newest',
    'printType': 'books',
    'key': os.environ.get('GOOGLE_BOOK_API_KEY'),
}

while True:
    with db:
        results_retrieved = sum([row[0] for row in db.execute("select item_count from api_response")])

    if results_retrieved > 0:
        params['startIndex'] = results_retrieved

    r = requests.get(base_url, params=params)
    r.raise_for_status()

    total_items = r.json()['totalItems']

    if 'items' not in r.json():
        print('No more books to get.')
        break

    elif 'items' in r.json():
        item_count = len(r.json()['items'])

        with db:
            db.execute("insert or ignore into api_response(request_url, status, item_count, response) values (?,?,?,?)",
                       r.url, r.status_code, item_count, json.dumps(r.json()), datetime.utcnow())



