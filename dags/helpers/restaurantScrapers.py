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
DRIVER.set_page_load_timeout(30)
    
def get_restaurant_links(**kwargs):
    ti = kwargs['ti']
    urls = 'https://www.tripadvisor.com.sg/Restaurants-g294265-Singapore.html'
    url2 = 'https://www.tripadvisor.com/Restaurants-g294265-oa30-Singapore.html#EATERY_LIST_CONTENTS'
    url3 = 'https://www.tripadvisor.com/Restaurants-g294265-oa60-Singapore.html#EATERY_LIST_CONTENTS'
    restUrlList = []

    response = requests.get(urls, headers=HEADERS)
    response2 = requests.get(url2, headers = HEADERS)
    response3 = requests.get(url3, headers = HEADERS)
    soup = bs(response.text, 'html.parser')
    soup2 = bs(response2.text, 'html.parser')
    soup3 = bs(response3.text, 'html.parser')


    for link in soup.find_all('a'):
        if "Restaurant_Review" in link.get('href'):
            unmodifiedUrl = link.get('href')
            restUrlList.append('https://www.tripadvisor.com' + unmodifiedUrl)

    for link in soup2.find_all('a'):
        if "Restaurant_Review" in link.get('href'):
            unmodifiedUrl = link.get('href')
            restUrlList.append('https://www.tripadvisor.com' + unmodifiedUrl)

    for link in soup3.find_all('a'):
        if "Restaurant_Review" in link.get('href'):
            unmodifiedUrl = link.get('href')
            restUrlList.append('https://www.tripadvisor.com' + unmodifiedUrl) 

    restUrlList = [link for link in restUrlList if 'REVIEW' not in link]
    restUrlList = list(set(restUrlList))
    ti.xcom_push(key='restaurant_link_list', value=restUrlList)

    # Insert list of restaurant links into `RestaurantURL`
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS RestaurantURL (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='RestaurantURL' AND 
            index_name='url_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX url_index ON RestaurantURL (url(255));
        """)

    insert_sql = "INSERT IGNORE INTO RestaurantURL (url) VALUES (%s)"
    CURSOR.executemany(insert_sql, [(url,) for url in restUrlList])
    MYDB.commit()

def get_restaurant_information(**kwargs):
    ti = kwargs['ti']
    restaurant_links = ti.xcom_pull(key='restaurant_link_list', task_ids='get_restaurant_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS Restaurant (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name CHAR(225),
            longitude CHAR(225), 
            latitude CHAR(225),
            landmark CHAR(225),
            neighborhood CHAR(225),
            address TEXT,
            email CHAR(225),
            phone CHAR(225),
            website TEXT,
            ranking CHAR(225),
            rating CHAR(225),
            numReviews CHAR(225),
            foodRating CHAR(225),
            serviceRating CHAR(225),
            valueRating CHAR(225),
            atmosphereRating CHAR(225),
            priceRange CHAR(225),
            cuisines TEXT,
            dietaryRestrictions TEXT,
            meals TEXT,
            features TEXT, 
            sentimentRank CHAR(80)
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='Restaurant' AND 
            index_name='restaurant_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX restaurant_index ON Restaurant (name);
        """)

    for link in restaurant_links:
        DRIVER.get(link)
        time.sleep(2)
        id = re.search('d[0-9]+', link).group(0)[1:]
        data = re.search(r'window\.__WEB_CONTEXT__=(.*?});', DRIVER.page_source).group(1)
        data = data.replace('pageManifest', '"pageManifest"')
        data = json.loads(data)
        restaurant = data['pageManifest']['redux']['api']['responses']['/data/1.0/restaurant/'+id+'/overview']['data']

        name = restaurant['name']

        location = restaurant['location']
        longitude = location['longitude']
        latitude = location['latitude']
        if (location['landmark'] is not None):
            landmark = re.sub('\|', '', location['landmark'])
        else:
            landmark = ''
        neighborhood = location['neighborhood']

        contact = restaurant['contact']
        address = contact['address']
        email = contact['email']
        phone = contact['phone']
        website = contact['website']

        rating = restaurant['rating']
        if (rating['primaryRanking'] != None):
            rank = rating['primaryRanking']['rank']
        else:
            rank = None
        primaryRating = rating['primaryRating']
        numReviews = rating['reviewCount']
        foodRating, serviceRating, valueRating, atmosphereRating = None, None, None, None
        for r in rating['ratingQuestions']:
            if r['name'] == 'Food':
                foodRating = r['rating']
            elif r['name'] == 'Service':
                serviceRating = r['rating']
            elif r['name'] == 'Value':
                valueRating = r['rating']
            elif r['name'] == 'Atmosphere':
                atmosphereRating = r['rating']
        if (restaurant['detailCard'] != None):
            details = restaurant['detailCard']['tagTexts']
        else: 
            details = None
        separator = ', '
        priceRange = separator.join([t['tagValue'] for t in details['priceRange']['tags']])
        cuisines = separator.join([t['tagValue'] for t in details['cuisines']['tags']])
        diet = separator.join([t['tagValue'] for t in details['dietaryRestrictions']['tags']])
        meals = separator.join([t['tagValue'] for t in details['meals']['tags']])
        features = separator.join([t['tagValue'] for t in details['features']['tags']])

        result = {
            'name': name,
            'longitude': longitude, 'latitude': latitude, 'landmark': landmark, 'neighborhood': neighborhood,
            'address': address, 'email': email, 'phone': phone, 'website': website,
            'ranking': rank, 'rating': primaryRating, 'numReviews': numReviews, 
            'foodRating': foodRating, 'serviceRating': serviceRating, 'valueRating': valueRating, 'atmosphereRating': atmosphereRating,
            'priceRange': priceRange, 'cuisines': cuisines, 'dietaryRestrictions': diet, 'meals': meals, 'features': features, 
            'sentimentRank': 'None'
        }

        # Insert restaurant to `Restaurant`
        columns = ', '.join(result.keys())
        values = ', '.join(['%s'] * len(result))
        insert_sql = f"INSERT IGNORE INTO Restaurant ({columns}) VALUES ({values})"
        CURSOR.execute(insert_sql, tuple(result.values()))
    MYDB.commit()


def get_restaurant_reviews(**kwargs):
    ti = kwargs['ti']
    restaurant_links = ti.xcom_pull(key='restaurant_link_list', task_ids='get_restaurant_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS RestaurantReview (
            id INT AUTO_INCREMENT PRIMARY KEY,
            restaurantName CHAR(225),
            title TEXT, 
            review TEXT,
            reviewDate CHAR(225),
            rating CHAR(225),
            dateOfVisit CHAR(225),
            username CHAR(225),
            numReviewUser CHAR(225)
        )
    """)
    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='RestaurantReview' AND 
            index_name='restaurant_review_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX restaurant_review_index ON RestaurantReview (username, restaurantName);
        """)

    for link in restaurant_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        
        id = re.search('d[0-9]+', link).group(0)[1:]
        data = re.search(r'window\.__WEB_CONTEXT__=(.*?});', DRIVER.page_source).group(1)
        data = data.replace('pageManifest', '"pageManifest"')
        data = json.loads(data)
        restaurant = data['pageManifest']['redux']['api']['responses']['/data/1.0/restaurant/'+id+'/overview']['data']

        name = restaurant['name']

        for rev in soup.find_all('div', class_='prw_rup prw_reviews_review_resp'):
            username = rev.find('div', class_='info_text pointer_cursor').text
            try:
                n_review_user = rev.find('div', class_='reviewerBadge badge').text.split(" ")[0]
            except:
                n_review_user = ""
            review_date = rev.find('span', class_='ratingDate')['title']
            rating = rev.select('span[class*="ui_bubble_rating"]')[0]['class'][1][-2:]
            title = rev.find('span', class_='noQuotes').text
            comments = rev.find('div', class_='entry').text
            try:
                date_of_visit = rev.find('div', class_='prw_rup prw_reviews_stay_date_hsx').text.split(": ")[1]
            except:
                date_of_visit = ""
            review = {
                'restaurantName': name, 'username': username, 'numReviewUser': n_review_user,
                'reviewDate': review_date, 'rating': rating, 'title': title, 'review': comments,
                'dateOfVisit': date_of_visit
            }
            # Insert restaurant review to `RestaurantReview`
            columns = ', '.join(review.keys())
            values = ', '.join(['%s'] * len(review))
            insert_sql = f"INSERT IGNORE INTO RestaurantReview ({columns}) VALUES ({values})"
            CURSOR.execute(insert_sql, tuple(review.values()))
    MYDB.commit()