# Import basic Python packages
import re
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import json
import time
import mysql.connector

# Import Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Request header
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
}

# Connect to MySQL Database
MYDB = mysql.connector.connect(
    host="mysql",
    user="root",
    password="root",
    database="airflow"
)
CURSOR = MYDB.cursor()

# Selenium Chrome Driver
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
DRIVER = webdriver.Chrome(options=chrome_options)

def get_attraction_links(**kwargs):
    ti = kwargs['ti']
    urls = ['https://www.tripadvisor.com/Attraction_Products-g294265-Singapore.html|']
    url2 = ['https://www.tripadvisor.com/Attraction_Products-g294265-o30-Singapore.html']
    url3 = ['https://www.tripadvisor.com/Attraction_Products-g294265-o60-Singapore.html']
    attrUrlList = []

    for link in urls:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        attrUrlList.extend(['https://www.tripadvisor.com'+a['href'] for a in soup.select('a:has(h3)')])

    for link in url2:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        attrUrlList.extend(['https://www.tripadvisor.com'+a['href'] for a in soup.select('a:has(h3)')])

    for link in url3:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        attrUrlList.extend(['https://www.tripadvisor.com'+a['href'] for a in soup.select('a:has(h3)')])
    ti.xcom_push(key='attraction_link_list', value=attrUrlList)
    # Insert list of attraction links into `AttractionURL`
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS AttractionUrl (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='AttractionUrl' AND 
            index_name='url_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX url_index ON AttractionUrl (url(255));
        """)

    insert_sql = "INSERT IGNORE INTO AttractionUrl (url) VALUES (%s)"
    CURSOR.executemany(insert_sql, [(url,) for url in attrUrlList])
    MYDB.commit()

def get_attraction_information(**kwargs):
    ti = kwargs['ti']
    attraction_links = ti.xcom_pull(key='attraction_link_list', task_ids='get_attraction_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS Attraction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name CHAR(225),
            rating CHAR(225), 
            numReviews CHAR(225),
            description TEXT,
            organization CHAR(225),
            price CHAR(225), 
            itinerary TEXT, 
            sentimentRank CHAR(80)
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='Attraction' AND 
            index_name='attraction_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX attraction_index ON Attraction (name);
        """)

    for link in attraction_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        json_data = json.loads(soup.find_all('script', type='application/ld+json')[2].string)
        name = json_data['name']

        try:
            ratingValue = json_data['aggregateRating']['ratingValue']
        except:
            ratingValue = ""
        
        try:
            reviewCount = json_data['aggregateRating']['reviewCount']
        except:
            reviewCount = ""
        
        try:
            description = json_data['description']
        except:
            description = ""
        
        try:
            organization = json_data['brand']['name']
        except:
            organization = ""

        try:
            price = json_data['offers']['price']
        except:
            price = ""

        try:
            itinerary_text = soup.find('section', id='tab-data-WebPresentation_AttractionProductItineraryPlaceholder').text

            def clean(text):
                # remove irrelevant text
                text = re.sub('Itinerary|You\'ll get picked upSee departure details|You\'ll return to the starting point\+–|See details', '', text)

                # split items
                for match in re.findall(re.compile('[a-z][1-9]'), text):
                    text = text.replace(match, match[0] + '|' + match[1])
                items = text.split('|')

                # format item
                formatted_items = []
                for item in items:
                    for match in re.findall(re.compile('[a-z][A-Z]|[0-9][A-Z]'), item):
                        item = item.replace(match, match[0] + ' ' + match[1])
                    formatted_items.append(item)
                formatted_items_str = ', '.join(formatted_items)
                return formatted_items_str

            itinerary = clean(itinerary_text)
        except:
            itinerary=""

        attraction = {'name': name, 'rating': ratingValue, 'numReviews': reviewCount,
                    'description': description, 'organization': organization, 'price': price,
                    'itinerary': itinerary, 'sentimentRank': 'None'}
        # Insert attraction to `Attraction`
        columns = ', '.join(attraction.keys())
        values = ', '.join(['%s'] * len(attraction))
        insert_sql = f"INSERT IGNORE INTO Attraction ({columns}) VALUES ({values})"
        CURSOR.execute(insert_sql, tuple(attraction.values()))
    MYDB.commit()

def get_attraction_reviews(**kwargs):
    ti = kwargs['ti']
    attraction_links = ti.xcom_pull(key='attraction_link_list', task_ids='get_attraction_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS AttractionReview (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username CHAR(225),
            attractionName CHAR(225),
            location CHAR(225),
            title TEXT, 
            review TEXT,
            reviewDate CHAR(225),
            rating CHAR(225),
            numReviewUser CHAR(225),
            numVotesReview CHAR(225)
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='AttractionReview' AND 
            index_name='attraction_review_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX attraction_review_index ON AttractionReview (username, attractionName, reviewDate);
        """)

    for link in attraction_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        json_data = json.loads(soup.find_all('script', type='application/ld+json')[2].string)
        name = json_data['name']

        for rev in soup.select('[data-automation="reviewCard"]'):
            username = rev.find('a', class_='BMQDV _F G- wSSLS SwZTJ FGwzt ukgoS').text

            try:
                n_review_user = rev.find('div', class_='biGQs _P pZUbB osNWb').find('span', class_='IugUm').text
                location = re.sub(n_review_user, "", rev.find('div', class_='biGQs _P pZUbB osNWb').text)
            except:
                n_review_user = rev.find('div', class_='biGQs _P pZUbB osNWb').text
                location = None
            n_review_user = re.sub("contributions|contribution", "", n_review_user)

            n_votes_review = rev.find('span', class_='biGQs _P FwFXZ').text

            text = rev.find_all('span', class_='yCeTE')
            title = text[0].text
            comments = text[1].text

            date = re.sub("Written ", "", rev.find('div', class_='biGQs _P pZUbB ncFvv osNWb').text)

            rating = float(rev.find('svg', class_='UctUV d H0')['aria-label'].split(" ")[0])

            review = {
                'attractionName': name, 'username': username, 'numReviewUser': n_review_user, 'location': location, 
                'rating': rating, 'title': title, 'review': comments, 'reviewDate': date, 'numVotesReview': n_votes_review
            }
             # Insert attraction review to `AttractionReview`
            columns = ', '.join(review.keys())
            values = ', '.join(['%s'] * len(review))
            insert_sql = f"INSERT IGNORE INTO AttractionReview ({columns}) VALUES ({values})"
            CURSOR.execute(insert_sql, tuple(review.values()))
    MYDB.commit()