from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import mysql.connector

# Import basic Python packages
import re
import pandas as pd
from bs4 import BeautifulSoup as bs
import requests
import time

# Import Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

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

dag = DAG(
    dag_id='is3107',
    default_args=default_args,
    description='Load data from API into MySQL, transform it and read it back',
    schedule_interval=timedelta(days=1),
)

# Get the list of all Hotel Links in Singapore
def get_hotel_links(**kwargs):
    ti = kwargs['ti']
    urls = ['https://www.tripadvisor.com/Hotels-g294265-Singapore.html|']
    url2 = ['https://www.tripadvisor.com/Hotels-g294265-o30-Singapore.html']
    url3 = ['https://www.tripadvisor.com/Hotels-g294265-o60-Singapore.html']
    restlist = []

    for link in urls:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            restlist.extend(['https://www.tripadvisor.com'+a])

    for link in url2:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            restlist.extend(['https://www.tripadvisor.com'+a])

    for link in url3:
        response=requests.get(link, headers=HEADERS)
        soup = bs(response.text, 'html.parser')
        
        for review in soup.findAll('a',{'class':'review_count'}):
            a = review['href']
            restlist.extend(['https://www.tripadvisor.com'+a])

    restlist = list(set([re.sub("#REVIEWS", "", link) for link in restlist]))
    print(f'First 3 links: {restlist[:3]}')
    ti.xcom_push(key='hotel_link_list', value=restlist)

    # Insert list of hotel links into `url`
    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS Url (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url TEXT
        )
    """)
    insert_sql = "INSERT INTO Url (url) VALUES (%s)"
    CURSOR.executemany(insert_sql, [(url,) for url in restlist])
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
            numNearbyAttractions CHAR(225)
        )
    """)
    for link in hotel_links:
        DRIVER.get(link)
        time.sleep(2)
        soup = bs(DRIVER.page_source, 'html.parser')
        name = DRIVER.find_element(by='xpath', value="//h1[@class='QdLfr b d Pn']").text

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
            review = DRIVER.find_element(by='xpath', value="//span[@class='HWBrU q Wi z Wc']").text
        except:
            review = None
        try:
            rating = DRIVER.find_element(by='xpath', value="//span[@class='IHSLZ P']").text
        except:
            rating = None
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
        }
        print(f'Hotel info is {hotel}')
        
        # Insert hotel to `hotel`
        columns = ', '.join(hotel.keys())
        values = ', '.join(['%s'] * len(hotel))
        insert_sql = f"INSERT INTO Hotel ({columns}) VALUES ({values})"
        CURSOR.execute(insert_sql, tuple(hotel.values()))

    MYDB.commit()

get_hotel_links_task = PythonOperator(
    task_id='get_hotel_links',
    python_callable=get_hotel_links,
    dag=dag,
)

get_hotel_info_task = PythonOperator(
    task_id='get_hotel_information',
    python_callable=get_hotel_information,
    dag=dag,
)

get_hotel_links_task >> get_hotel_info_task
