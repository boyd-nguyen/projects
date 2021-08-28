# -*- coding: utf-8 -*-
"""
Created on Sun Jul 11 10:27:11 2021

@author: boydn
"""
#%% IMPORTING LIBRARIES
import bs4, requests
from selenium import webdriver
import time
import pandas as pd, numpy as np
import os

#%%SET UP SELENIUM DRIVER
driver = webdriver.Chrome(r'C:\Users\boydn\Desktop\Work and Study\Projects\chromedriver.exe')
driver.get('https://www.xbox.com/en-AU/games/all-games')
time.sleep(10)

#%%SCRAPE

page = 1
links = []
product_id = []
release_date = []
ms_product = []
multiplayer = []
rating = []
price = []
name = []


while page < 49:   
    soup = bs4.BeautifulSoup(driver.page_source, features='lxml')
    
    items = soup.select('div.m-product-placement-item')
    
    for item in items:
        product_id.append(item.get('data-bigid'))
        release_date.append(item.get('data-releasedate'))
        ms_product.append(item.get('data-msproduct'))
        multiplayer.append(item.get('data-multiplayer'))
        rating.append(item.get('data-rating'))
        price.append(item.get('data-listprice'))
    
    game_links = soup.select('a.gameDivLink')
    
    for game in game_links:
        links.append(game.get('href'))
        for child in game.children:
            if child.name == 'div':
                name.append(child.getText())
    page += 1
    page_css = 'a[data-bi-name="' + str(page) + '"]'
    elem = driver.find_element_by_css_selector(page_css)
    
    elem.click()
    time.sleep(2)
    
game_data = {
    'product_id': product_id,
    'release_date': release_date,
    'name': name,
    'ms_product': ms_product,
    'rating': rating,
    'multiplayer': multiplayer,
    'link': links,
    'price': price
    }
#%%CLEAN

os.chdir(r'C:\Users\boydn\Desktop\Work and Study\Projects\1. Data cleaning')
xbox = pd.DataFrame(game_data)
xbox.to_csv

