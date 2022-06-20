from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import sqlite3
import random
import time
import logging
from datetime import datetime
from dateutil import tz
from urllib.parse import urlparse, urljoin

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

console_formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s")
console_handler.setFormatter(console_formatter)

logger.addHandler(console_handler)


def set_up_chrome_options(proxy_path="http://localhost:8080"):
    chrome_options = Options()
    chrome_options.headless = True
    # need this for pywb wayback recording via proxy
    chrome_options.accept_insecure_certs = True
    chrome_options.add_argument(f'--proxy-server={proxy_path}')
    # disable images
    chrome_options.add_experimental_option(
        name='prefs',
        value={"profile.managed_default_content_settings.images": 2}
    )
    chrome_options.add_experimental_option(
        name='prefs',
        value={"profile.default_content_settings.images": 2}
    )

    return chrome_options


def set_up_databases(dbpath):
    db = sqlite3.connect(dbpath)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS links(
            link primary key,
            retrieval_time
            )
        """
    )

    return db


def remove_query_string(url):
    base_url = 'https://www.goodreads.com/'
    url = urlparse(url)

    return urljoin(base_url, url.path)


css = {
    'search': 'a[href*="search?page="],a.bookTitle',
    'list': 'a[href*="?page="],a.bookTitle,a.listTitle'
}


def crawl(driver, url, db):
    db.execute(
        "INSERT OR IGNORE INTO links(link) VALUES(?)",
        (url,)
    )
    db.commit()

    while True:
        to_retrieve = [row[0] for row in
                       db.execute(
                        """
                        SELECT link FROM links
                        WHERE retrieval_time IS NULL
                        """)
                       ]
        if not to_retrieve:
            logger.info("No more url to retrieve...")
            break

        else:
            for i, link in enumerate(to_retrieve):
                error_wait = 10
                try:
                    logger.info(f"""
                                Retrieving {link}...
                                Progress: {i + 1}/{len(to_retrieve)}
                                """.strip())

                    driver.get(link)
                    driver.add_cookie({'name': 'blocking_sign_in_interstitial',
                                       'value': 'true'})

                    # sleep for at least 10 seconds and on average 20 seconds
                    time.sleep(10 + random.expovariate(0.1))

                    with db:
                        db.execute(
                            """
                            REPLACE INTO links
                            VALUES (?,?)
                            """,
                            (link, datetime.now(tz=tz.UTC))
                        )

                    css_selector = css['list']

                    links_to_be_recorded = driver.find_elements(
                        By.CSS_SELECTOR, css_selector
                    )

                    if links_to_be_recorded:
                        with db:
                            for each in links_to_be_recorded:
                                href = each.get_attribute('href')
                                if ('pywb.proxy' not in href and
                                        'goodreads' in href):
                                    logger.info(
                                            f"""
                                            FOUND URL:
                                            {href}...
                                            
                                            INSERTING TO DATABASE...
                                            """)
                                    if '?page=' in href:
                                        db.execute(
                                            """
                                            INSERT OR IGNORE INTO links(link)
                                            VALUES (?)
                                            """,
                                            (href,))
                                    else:
                                        db.execute(
                                            """
                                            INSERT OR IGNORE INTO links(link)
                                            VALUES (?)
                                            """,
                                            (remove_query_string(href),))

                    else:
                        logger.info("NO MORE LINK TO RETRIEVE...")

                    retrieved_total = [row[0]
                                       for row in
                                       db.execute(
                                           """
                                           SELECT COUNT(*)
                                           FROM links
                                           """
                                       )][0]

                    logger.info(f"TOTAL LINKS RETRIEVED: {retrieved_total}.")

                except Exception as e:
                    logger.error(f"Error {e}. Waiting {error_wait} seconds.")
                    time.sleep(error_wait)
                    error_wait *= 1.5


if __name__ == '__main__':
    db = set_up_databases("books_goodreads.db")

    driver = webdriver.Chrome(options=set_up_chrome_options())
    driver.set_page_load_timeout(30)
    driver.implicitly_wait(10)

    # url = r'https://www.goodreads.com/search?q=poverty&search_type=books'
    url = r'https://www.goodreads.com/list/tag/non-fiction'
    crawl(driver=driver, url=url, db=db)







