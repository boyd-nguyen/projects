# Data project: Books

Goals:

- To produce a cleaned dataset of books scraped from Goodreads's nonfiction lists.
- To write a commandline tool for easily extracting book data from Google Books API.

Sources of data:
- Google Books API
- Goodreads: starting from https://www.goodreads.com/list/tag/non-fiction

## Goodreads

As Goodreads API was deprecated, the aim is to create a database of cleaed book metadata by scraping as many books from Goodreads as possible, following a crawling approach:

- The script starts from https://www.goodreads.com/list/tag/non-fiction and extracts all book links and list links, then inserts it into a database for subsequent recording.
- Each page is recorded and its page source loaded into a database as raw response, to be cleaned at a later stage.

Thankfully, all Goodreads pages are static, so the process can be executed without the need for Selenium. However, in this project I'll introduce both ways of scraping web data in a reproducible fashion.

1. `goodreads_scraper.py`: using `requests`

Given that Goodreads pages are static, its page source can be easily obtained by using a simple `requests.get()` method. Raw response text is loaded into an SQLite database for subsequent cleaning. Looping and error handling are added to make the script resumable. 

2. `goodreads_crawler.py`: using **selenium, pywb, warcio**

This is to demonstrate another approach to web scraping, especially applicable to dynamic websites. The aim is to render the website in a headless browser, pass it through a warc-recording proxy to obtain web archive files that can be processed at a later stage. This method generally takes longer and is more storage-heavy, so it is not used in this project other than as a demonstration.
