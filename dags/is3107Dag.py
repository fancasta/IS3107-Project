from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Import helpers function
from helpers.attractionScrapers import get_attraction_links, get_attraction_information, get_attraction_reviews
from helpers.hotelScrapers import get_hotel_links, get_hotel_information, get_hotel_reviews
from helpers.restaurantScrapers import get_restaurant_links, get_restaurant_information, get_restaurant_reviews
from helpers.topicAnalysis import get_topics
from helpers.sentimentAnalysis import sentiment_analysis

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 18),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='is3107',
    default_args=default_args,
    description='Load data from API into MySQL, transform it and read it back',
    schedule_interval=timedelta(days=30)
)

# Hotel Task
get_hotel_link_task = PythonOperator(
    task_id='get_hotel_links',
    python_callable=get_hotel_links,
    dag=dag,
)

get_hotel_info_task = PythonOperator(
    task_id='get_hotel_information',
    python_callable=get_hotel_information,
    dag=dag,
)

get_hotel_review_task = PythonOperator(
    task_id='get_hotel_reviews',
    python_callable=get_hotel_reviews,
    dag=dag,
)

# Restaurant Task
get_restaurant_link_task = PythonOperator(
    task_id='get_restaurant_links',
    python_callable=get_restaurant_links,
    dag=dag,
)

get_restaurant_info_task = PythonOperator(
    task_id='get_restaurant_information',
    python_callable=get_restaurant_information,
    dag=dag,
)

get_restaurant_review_task = PythonOperator(
    task_id='get_restaurant_reviews',
    python_callable=get_restaurant_reviews,
    dag=dag,
    retries=5
)

# Attraction Task
get_attraction_link_task = PythonOperator(
    task_id='get_attraction_links',
    python_callable=get_attraction_links,
    dag=dag,
)

get_attraction_info_task = PythonOperator(
    task_id='get_attraction_information',
    python_callable=get_attraction_information,
    dag=dag,
)

get_attraction_review_task = PythonOperator(
    task_id='get_attraction_reviews',
    python_callable=get_attraction_reviews,
    dag=dag,
)

# Topic analysis task
get_topics_task = PythonOperator(
    task_id='get_topics',
    python_callable=get_topics,
    dag=dag,
)

#Sentiment Analysis Task
get_sentiment_analysis_task = PythonOperator(
    task_id='sentiment_analysis',
    python_callable=sentiment_analysis,
    dag=dag,
)

# Arrange the Dag
# Scraper Task
get_hotel_link_task >> get_hotel_info_task
get_hotel_link_task >> get_hotel_review_task
get_restaurant_link_task >> get_restaurant_info_task
get_restaurant_link_task >> get_restaurant_review_task
get_attraction_link_task >> get_attraction_info_task
get_attraction_link_task >> get_attraction_review_task

# Topic Analysis Task
get_attraction_review_task >> get_topics_task
get_hotel_review_task >> get_topics_task
get_restaurant_review_task >> get_topics_task

# Sentiment Analysis Task
get_hotel_info_task >> get_sentiment_analysis_task
get_restaurant_info_task >> get_sentiment_analysis_task
get_attraction_info_task >> get_sentiment_analysis_task
get_attraction_review_task >> get_sentiment_analysis_task
get_hotel_review_task >> get_sentiment_analysis_task
get_restaurant_review_task >> get_sentiment_analysis_task