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

collection_name = 'test-archive'
warc_path = f"collections/{collection_name}/archive/*.warc.gz"


def get_book_id(url):
    pattern = re.compile(r"(book/show/)([^A-z-]+)")
    book_id = re.search(pattern, url)[2]

    return book_id

url = 'https://www.goodreads.com/book/show/52848968-one-hundred-years-of-socialism?from_search=true&from_srp=true&qid=rRmw6q7MIS&rank=124'
get_book_id(url)
print('done')

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

# for record in warc_collections:
#     try:
#         url = record.rec_headers.get_header("WARC-Target-URI")
#         if url.startswith("https://www.goodreads.com/book/show/"):
#             if record.http_headers.get_statuscode() == "200":
#                 parsed = BeautifulSoup(record.content_stream(), features="html.parser")
#
#
#



