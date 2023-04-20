import pandas as pd
import spacy
from spacytextblob.spacytextblob import SpacyTextBlob
import mysql.connector

# Load the spaCy model and add the TextBlob extension
nlp = spacy.load("en_core_web_sm")
nlp.add_pipe("spacytextblob")

# Connect to MySQL Database
MYDB = mysql.connector.connect(
    host="mysql",
    user="root",
    password="root",
    database="airflow"
)
CURSOR = MYDB.cursor()

# Define a function to classify the sentiment of a text
def classify_sentiment(text):
    doc = nlp(text)
    return doc._.polarity

def sentiment_analysis():
    # Load the hotel reviews SQL dataset file into a pandas DataFrame
    hotelReviewQuery = "SELECT * FROM HotelReview"
    hotel_reviews_df = pd.read_sql(hotelReviewQuery, MYDB)
    # Compute the average sentiment score for each hotel
    hotel_sentiments = hotel_reviews_df.groupby("hotelName")["review"].apply(lambda x: x.apply(classify_sentiment).mean()).reset_index()
    hotel_sentiments.rename(columns={"review": "sentimentScore"}, inplace=True)

    # Load the hotel information into a pandas DataFrame
    hotelQuery = "SELECT * FROM Hotel"
    hotel_info_df = pd.read_sql(hotelQuery, MYDB)

    # Merge the sentiment scores with the hotel information DataFrame
    merged_hotel_df = pd.merge(hotel_info_df, hotel_sentiments, how="left", left_on="name", right_on="hotelName")

    # Compute the sentiment rank for each hotel based on the average sentiment score
    merged_hotel_df["sentimentRank"] = pd.qcut(merged_hotel_df["sentimentScore"], q=4, labels=["Poor", "Fair", "Good", "Excellent"])

    # Drop the redundant columns
    merged_hotel_df.drop(columns=["hotelName", "sentimentScore"], inplace=True)

    # Load the restaurant reviews into a pandas DataFrame
    restReviewQuery = "SELECT * FROM RestaurantReview"
    restaurant_reviews_df = pd.read_sql(restReviewQuery, MYDB)

    # Compute the average sentiment score for each restaurant
    restaurant_sentiments = restaurant_reviews_df.groupby("restaurantName").apply(lambda x: x.apply(lambda y: classify_sentiment(y["title"] + " " + y["review"]), axis=1).mean()).reset_index()
    restaurant_sentiments.rename(columns={0: "sentimentScore"}, inplace=True)

    # Load the restaurant information into a pandas DataFrame
    restQuery = "SELECT * FROM Restaurant"
    restaurant_info_df = pd.read_sql(restQuery, MYDB)

    # Merge the sentiment scores with the restaurant information DataFrame
    merged_restaurant_df = pd.merge(restaurant_info_df, restaurant_sentiments, how="left", left_on="name", right_on="restaurantName")

    # Compute the sentiment rank for each restaurant based on the average sentiment score
    merged_restaurant_df["sentimentRank"] = pd.qcut(merged_restaurant_df["sentimentScore"], q=4, labels=["Poor", "Fair", "Good", "Excellent"])

    # Drop the redundant columns
    merged_restaurant_df.drop(columns=["restaurantName", "sentimentScore"], inplace=True)

    # Load the attraction reviews SQL dataset file into a pandas DataFrame
    attractionReviewQuery = "SELECT * FROM AttractionReview"
    attraction_reviews_df = pd.read_sql(attractionReviewQuery, MYDB)

    # Compute the average sentiment score for each attraction
    attraction_sentiments = attraction_reviews_df.groupby("attractionName").apply(lambda x: x.apply(lambda y: classify_sentiment(y["title"] + " " + y["review"]), axis=1).mean()).reset_index()
    attraction_sentiments.rename(columns={0: "sentimentScore"}, inplace=True)

    # Load the attraction information into a pandas DataFrame
    attractionQuery = "SELECT * FROM Attraction"
    attraction_info_df = pd.read_sql(attractionQuery, MYDB)

    # Merge the sentiment scores with the attraction information DataFrame
    merged_attraction_df = pd.merge(attraction_info_df, attraction_sentiments, how="left", left_on="name", right_on="attractionName")

    # Compute the sentiment rank for each attraction based on the average sentiment score
    merged_attraction_df["sentimentRank"] = pd.qcut(merged_attraction_df["sentimentScore"], q=4, labels=["Poor", "Fair", "Good", "Excellent"])

    # Drop the redundant columns
    merged_attraction_df.drop(columns=["attractionName", "sentimentScore"], inplace=True)

    merged_hotel_df = merged_hotel_df.dropna(subset=['sentimentRank'])
    merged_restaurant_df = merged_restaurant_df.dropna(subset=['sentimentRank'])
    merged_attraction_df = merged_attraction_df.dropna(subset=['sentimentRank'])

    # Update sentimentRank to Hotel
    hotel_update_query = """
    UPDATE Hotel
    SET sentimentRank = %s
    WHERE id = %s
    """
    for row in merged_hotel_df.itertuples(index=False):
        data = (row.sentimentRank, row.id)
        CURSOR.execute(hotel_update_query, data)

    # Update sentimentRank to Restaurant
    rest_update_query = """
    UPDATE Restaurant
    SET sentimentRank = %s
    WHERE id = %s
    """
    for row in merged_restaurant_df.itertuples(index=False):
        data = (row.sentimentRank, row.id)
        CURSOR.execute(rest_update_query, data)

    # Update sentimentRank to Attraction
    attr_update_query = """
    UPDATE Attraction
    SET sentimentRank = %s
    WHERE id = %s
    """
    for row in merged_attraction_df.itertuples(index=False):
        data = (row.sentimentRank, row.id)
        CURSOR.execute(attr_update_query, data)