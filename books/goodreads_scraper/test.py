from datetime import datetime
import random
import re

import bs4
import requests
import time
import json


def get_int(string):
    pattern = '([0-9]+)'
    value = re.findall(pattern=pattern, string=string)

    return value


def is_rating_list(string):
    pattern = r"\[([0-9]+[,]?\s?)+\]"
    if re.search(pattern=pattern, string=string):
        return True
    else:
        return None


def process_rating_list(string):
    pure_digits = re.sub(r"[\[\]]", "", string)

    return pure_digits.strip().split(",")


def convert_to_date(string):
    pattern = r"\b([0123]?[0-9])(st|th|nd|rd)\b"
    string = re.sub(pattern, r"\1", string)

    date = datetime.strptime(string, "%B %d %Y")
    date = datetime.date(date)

    return time.mktime(date.timetuple())


def get_publisher(string):
    return re.sub(r"(^by\b\s)(.+)", r"\2", string)


def get_num_question(string):
    pattern = r"\b([0-9]+)\b (questions)"

    return re.search(pattern, string).group(1)


check = {}

for i in range(1):
    headers = {'User-Agent': "Mozilla/5.0",
               'Cookie': 'ccsid=215-4754242-4322857; locale=en; srb_8=1'}
    r = requests.get(
        "https://www.goodreads.com/book/show/6930002-the-iron-witch",
        headers=headers)
    if 'json' in r.text:
        check[i] = r.text

    # time.sleep(random.uniform(2, 3))

parsed = bs4.BeautifulSoup(r.text, "html.parser")
# if 'logged_out_browsing_page_count' not in r.headers:
#     parsed = bs4.BeautifulSoup(r.text, "html.parser")

data_map = {}

data = parsed.find("script", id="__NEXT_DATA__")

if data:
    data = json.loads(data.text)
    parent_dict = data['props']['pageProps']['apolloState']
    for key in parent_dict:
        if 'Book' in key:
            book_data = parent_dict[key]
        if 'Work' in key:
            work_data = parent_dict[key]
        if 'Series' in key:
            series_data = parent_dict[key]
            data_map["book_series_title"] = series_data.get("title")
            data_map["book_series_url"] = series_data.get("webUrl")
        else:
            data_map["book_series_title"] = None
            data_map["book_series_url"] = None

    data_map["title"] = book_data["titleComplete"]
    for key in book_data:
        if "description" in key:
            data_map["description"] = book_data[key]
    data_map["link"] = book_data["webUrl"]

    book_details = book_data["details"]
    data_map["format"] = book_details["format"]
    data_map["num_pages"] = book_details["numPages"]
    data_map["publication_time"] = book_details[
        "publicationTime"] / 1000
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

else:
    data = parsed.find(id="metacol")
    data_map["title"] = data.find(id="bookTitle").text.strip()
    data_map["link"] = "link"
    data_map["format"] = data.find(attrs={'itemprop': 'bookFormat'}).text
    num_pages = data.find(attrs={'itemprop': 'numberOfPages'}).text
    data_map["num_pages"] = get_int(num_pages)[0]

    details = data.find(id="details").find(class_="row",
                                           string=re.compile("published",
                                                             re.IGNORECASE))
    # publication_list = details.text.strip().split('\n')
    # publication_time = publication_list[1].strip()
    # data_map["publication_time"] = convert_to_date(publication_time)

    # publisher = publication_list[2].strip()
    # data_map["publisher"] = get_publisher(publisher)

    book_data_box = data.find(id="bookDataBox")

    isbn = book_data_box.find("div", string="ISBN")

    data_map["isbn_13"] = None
    if isbn:
        isbn = isbn.find_next_sibling("div")
        if 'ISBN13' in isbn.text.strip():
            data_map["isbn_13"] = isbn.text.strip().split()[2].replace(')', '')

    data_map["isbn"] = isbn.text.strip().split()[0] if isbn else None
    language = book_data_box.find(attrs={"itemprop": "inLanguage"})
    data_map["language"] = language.text if language else None

    # awards = book_data_box.find(attrs={"itemprop": "awards"})
    # if awards.find_all(awards.find_all("a", class_="award")):
    #     num_of_awards = len(awards.find_all("a", class_="award"))
    #     data_map["num_of_awards"] = num_of_awards
    # else:
    #     data_map["num_of_awards"] = 0
    #
    # average_rating = data.find(attrs={"itemprop": "ratingValue"})
    # if average_rating:
    #     data_map["average_rating"] = float(average_rating.text)
    #
    # ratings_count = data.find(attrs={"itemprop": "ratingCount"})
    # data_map["ratings_count"] = int(ratings_count['content'])
    #
    # reviews_count = data.find(attrs={"itemprop": "reviewCount"})
    # data_map["reviews_count"] = int(reviews_count["content"])
    #
    # ratings_info = parsed.find(id="reviewControls")
    # ratings_info_list = ratings_info.find("script").text.split('\n')
    #
    # for string in ratings_info_list:
    #     if is_rating_list(string):
    #         rating_list = process_rating_list(string=string)
    #
    # data_map["rating_1_count"] = int(rating_list[4])
    # data_map["rating_2_count"] = int(rating_list[3])
    # data_map["rating_3_count"] = int(rating_list[2])
    # data_map["rating_4_count"] = int(rating_list[1])
    # data_map["rating_5_count"] = int(rating_list[0])
    #
    # questions = parsed.find(class_="moreReaderQA").find("a")
    #
    # data_map["quotes_count"] = None
    #
    # if questions:
    #     data_map["questions_count"] = int(get_num_question(questions.text))
    #
    # data_map["topics_count"] = None

    series = book_data_box.find("div", string="Series")
    # if series:
    #     series_title = series.find_next_sibling("div").text
    #     series_url = series.find_next_sibling("div")["href"]
    #
    #     base_url = "https://www.goodreads.com"
    #
    #     data_map["book_series_title"] = series_title
    #     data_map["book_series_url"] = base_url + series_url
    # else:
    #     data_map["book_series_title"] = None
    #     data_map["book_series_url"] = None

print("Done")
