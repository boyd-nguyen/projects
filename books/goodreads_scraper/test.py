import random
import bs4
import requests
import time
import json

check = {}

for i in range(1):
    headers = {'User-Agent': "Mozilla/5.0",
               'Cookie': 'ccsid=215-1754205-4322857; locale=en; srb_8=1'}
    r = requests.get("https://www.goodreads.com/book/show/157993.The_Little_Prince",
                     headers=headers)
    if 'json' in r.text:
        check[i] = r.text

    time.sleep(random.uniform(2, 3))

parsed = bs4.BeautifulSoup(r.text, "html.parser")
# if 'logged_out_browsing_page_count' not in r.headers:
#     parsed = bs4.BeautifulSoup(r.text, "html.parser")
data = parsed.find("script", id="__NEXT_DATA__")
data = json.loads(data.text)
parent_dict = data['props']['pageProps']['apolloState']

data_map = {}

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
    "publicationTime"]
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
print("Done")