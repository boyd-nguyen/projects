from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import sqlite3
import random
import time
import logging
from datetime import datetime
from dateutil import tz

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

console_formatter = logging.Formatter(
    "%(name)s - %(levelname)s - %(message)s")
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
                driver.get(link)
                driver.add_cookie({'name': 'blocking_sign_in_interstitial',
                                   'value': 'false'})
                logger.info(f"""
                    Retrieving {link}...
                    Progress: {i+1}/{len(to_retrieve)}
                    """.strip())
                # sleep for at least 15 seconds and on average 25 seconds
                time.sleep(15 + random.expovariate(0.065))

                with db:
                    db.execute(
                        """
                        REPLACE INTO links
                        VALUES (?,?)
                        """,
                        (link, datetime.now(tz=tz.UTC))
                    )

                css_selector = 'a[href*="search?page="],a.bookTitle'
                links_to_be_recorded = driver.find_elements(By.CSS_SELECTOR,
                                                            css_selector)

                if links_to_be_recorded:
                    for each in links_to_be_recorded:
                        if 'pywb.proxy' not in each.get_attribute('href'):
                            logger.info(f"{each.get_attribute('href')}...")
                            db.execute("""
                                INSERT OR IGNORE INTO links(link)
                                VALUES (?)
                                """, (each.get_attribute('href'),))

                else:
                    logger.info("NO MORE LINK TO RETRIEVE...")


if __name__ == '__main__':
    db = set_up_databases("test.db")

    driver = webdriver.Chrome(options=set_up_chrome_options())
    driver.set_page_load_timeout(60)
    driver.implicitly_wait(10)

    url = r'https://www.goodreads.com/search?q=lee+kuan+yew&search_type=books'

    crawl(driver=driver, url=url, db=db)







