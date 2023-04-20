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

def get_hotel_links(**kwargs):
    ti = kwargs['ti']
    urls = ['https://www.tripadvisor.com/Hotels-g294265-Singapore.html|']
    url2 = ['https://www.tripadvisor.com/Hotels-g294265-o30-Singapore.html']
    url3 = ['https://www.tripadvisor.com/Hotels-g294265-o60-Singapore.html']
    hotelUrlList = []

    for link in urls:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            hotelUrlList.extend(['https://www.tripadvisor.com'+a])

    for link in url2:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            hotelUrlList.extend(['https://www.tripadvisor.com'+a])

    for link in url3:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            hotelUrlList.extend(['https://www.tripadvisor.com'+a])

    hotelUrlList = list(set([re.sub("#REVIEWS", "", link) for link in hotelUrlList]))
    print(f'First 3 links: {hotelUrlList[:3]}')
    ti.xcom_push(key='hotel_link_list', value=hotelUrlList)

    # Insert list of hotel links into `HotelURL`
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS HotelURL (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT
        )
    """)
    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='HotelURL' AND 
            index_name='url_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX url_index ON HotelURL (url(255));
        """)
    insert_sql = "INSERT IGNORE INTO HotelURL (url) VALUES (%s)"
    CURSOR.executemany(insert_sql, [(url,) for url in hotelUrlList])
    MYDB.commit()

# Get Hotel information based on the list of Hotel URL earlier
def get_hotel_information(**kwargs):
    ti = kwargs['ti']
    hotel_links = ti.xcom_pull(key='hotel_link_list', task_ids='get_hotel_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS Hotel (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name CHAR(225),
            address TEXT, 
            contactNumber CHAR(225),
            ranking CHAR(225),
            rating CHAR(225),
            numReviews CHAR(225),
            gradeWalkers CHAR(225),
            description TEXT,
            price CHAR(225),
            amenities TEXT,
            numNearbyRestaurants CHAR(225),
            numNearbyAttractions CHAR(225), 
            sentimentRank CHAR(80)
        )
    """)

    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='Hotel' AND 
            index_name='hotel_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX hotel_index ON Hotel (name);
        """)

    for link in hotel_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        json_data = json.loads(soup.find('script', type="application/ld+json").string)
        name = json_data['name']

        try:
            address = soup.find('span', {"class": "fHvkI PTrfg"}).text
        except:
            address = None
        try:
            num = DRIVER.find_element(by='xpath',
                                    value="//a[@class='NFFeO _S ITocq NjUDn']").get_attribute('href').split(':')[1]
        except:
            num = None
        try:
            price = soup.find('div', {"class": "WXMFC b"}).get_text().replace('SGD', '').strip()
        except:
            price = None
        try:
            review = json_data['aggregateRating']['reviewCount']
        except:
            review = 'None'

        try:
            rating = json_data['aggregateRating']['ratingValue']
        except:
            rating = 'None'
        amentities = ''
        for a in soup.findAll('div', {"class": "yplav f ME H3 _c"}):
            amentities += a.text.strip() + ', '
            
        try:
            ranking = DRIVER.find_element(by='xpath', value="//span[@class='Ci _R S4 H3 MD']").text
        except:
            ranking = None
        try:
            description = DRIVER.find_element(by='xpath', value="//div[@class='fIrGe _T']").text
        except:
            description = None
        try:
            grade_walkers = DRIVER.find_element(by='xpath', value="//div[@class='oOsXK WtgYg _S H3 q']").text.split(':')[1]
        except:
            grade_walkers = None
        try:
            n_restaurants = DRIVER.find_element(by='xpath', value="//span[@class='iVKnd Bznmz']").text
        except:
            n_restaurants = None
        try:
            n_attractions = DRIVER.find_element(by='xpath', value="//span[@class='iVKnd rYxbA']").text
        except:
            n_attractions = None
        # Hotel info
        hotel = {
            'name': name, 'address': address, 'contactNumber': num, 
            'ranking': ranking, 'rating': rating, 'numReviews': review, 'gradeWalkers': grade_walkers,
            'description': description, 'price': price, 'amenities': amentities,
            'numNearbyRestaurants': n_restaurants, 'numNearbyAttractions': n_attractions, 
            'sentimentRank': 'None'
        }
        # Insert hotel to `hotel`
        columns = ', '.join(hotel.keys())
        values = ', '.join(['%s'] * len(hotel))
        insert_sql = f"INSERT IGNORE INTO Hotel ({columns}) VALUES ({values})"
        CURSOR.execute(insert_sql, tuple(hotel.values()))
    MYDB.commit()

def get_hotel_reviews(**kwargs):
    ti = kwargs['ti']
    hotel_links = ti.xcom_pull(key='hotel_link_list', task_ids='get_hotel_links')
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS HotelReview (
            id INT AUTO_INCREMENT PRIMARY KEY,
            reviewId CHAR(225),
            hotelName CHAR(225),
            title TEXT, 
            review TEXT,
            reviewDate CHAR(225),
            numVotesReview CHAR(225),
            rating CHAR(225),
            dateOfStay CHAR(225),
            username CHAR(225),
            location CHAR(225),
            numReviewUser CHAR(225)
        )
    """)
    # Check if the index exists
    CURSOR.execute("""
            SELECT COUNT(1) 
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema=DATABASE() AND 
            table_name='HotelReview' AND 
            index_name='hotel_review_index';
        """)
    result = CURSOR.fetchone()
    index_not_exists = result[0] == 0

    # Drop the index if it exists
    if index_not_exists:
        CURSOR.execute("""
            CREATE UNIQUE INDEX hotel_review_index ON HotelReview (reviewId);
        """)

    for link in hotel_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        json_data = json.loads(soup.find('script', type="application/ld+json").string)
        name = json_data['name']
        for rev in soup.select('[data-test-target="HR_CC_CARD"]'):
            review_id = rev.find('div', class_='WAllg _T')['data-reviewid']
            title = rev.find('a', class_='Qwuub').text
            review = rev.find('div', class_='fIrGe _T').text
            try:
                username = rev.find('a', class_='ui_header_link uyyBf').text
            except:
                username=""
            date_string = rev.find('div', class_='cRVSd').text
            date = re.sub(username + '| wrote a review ', '', date_string)
            try:
                location = rev.find('span', class_='default LXUOn small').text
            except:
                location = ""
            try:
                n_review_user = re.sub('contributions|contribution', '', rev.find_all('span', class_='phMBo')[0].text)
            except:
                n_review_user = ""
            try:
                n_votes_review = re.sub(' helpful votes', '', rev.find_all('span', class_='phMBo')[1].text)
            except:
                n_votes_review = ""
            date_of_stay = re.sub('Date of stay: ', '', rev.find('span', class_='teHYY _R Me S4 H3').text)
            rating = rev.find('div', class_='Hlmiy F1').find('span')['class'][1][-2:]

            review = {
                'hotelName': name, 'reviewId': review_id, 'title': title, 
                'review': review,'reviewDate': date, 'numVotesReview': n_votes_review,
                'rating': rating, 'dateOfStay': date_of_stay, 'username': username,  
                'location': location,'numReviewUser': n_review_user, 
            }
            # Insert hotel review to `HotelReview`
            columns = ', '.join(review.keys())
            values = ', '.join(['%s'] * len(review))
            insert_sql = f"INSERT IGNORE INTO HotelReview ({columns}) VALUES ({values})"
            CURSOR.execute(insert_sql, tuple(review.values()))
    MYDB.commit()