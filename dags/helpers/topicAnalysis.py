import pandas as pd
import spacy
from bertopic import BERTopic
from nltk.corpus import stopwords
import mysql.connector
import nltk
nltk.download('stopwords')
nlp = spacy.load("en_core_web_sm")
nltk_stopwords = stopwords.words('english')

# Connect to MySQL Database
MYDB = mysql.connector.connect(
    host="mysql",
    user="root",
    password="root",
    database="airflow"
)
CURSOR = MYDB.cursor()


def clean(noun_phrase):
    cleaned = []
    for tok in nlp(noun_phrase):
        # remove proper nouns, prepositions and stopwords
        if tok.pos_ not in ['PROPN', 'ADP'] and tok.text not in nltk_stopwords:
            cleaned.append(tok.text)
    return " ".join(cleaned)

def get_noun_phrases(text):
    noun_phrases = []
    for chunk in nlp(text).noun_chunks:
        noun_phrase = clean(chunk.text)
        if noun_phrase != "":
            noun_phrases.append(noun_phrase)
    return noun_phrases

def analyse(df, n_topics, category):
    df['text'] = df['title'] + ' ' + df['review']

    # extract noun phrases from review text
    df['noun_phrases'] = df['text'].apply(get_noun_phrases)
    unique_noun_phrases = list(set([val for ls in df['noun_phrases'].values.tolist() for val in ls]))

    # clustering to identify topics
    topic_model = BERTopic(nr_topics=n_topics)
    topics, probs = topic_model.fit_transform(unique_noun_phrases)
    topic_info = topic_model.get_topic_info()
    doc_info = topic_model.get_document_info(unique_noun_phrases)
    nSignificant = topic_info[topic_info['Count']>round(len(unique_noun_phrases)*0.01)].shape[0]

    # convert to dictionary
    topic_dict = dict()
    for i in range(doc_info.shape[0]):
        doc = doc_info.loc[i, :]['Document']
        topic_name = doc_info.loc[i, :]['Name']
        topic_dict[doc] = topic_name

    # tag each review with the topics it is relevant to
    df['topics'] = df['noun_phrases'].apply(lambda x: list(set([topic_dict[i] for i in x])))

    # count number of reviews for each topic
    topics_df = df.explode('topics')\
                .groupby(['topics'])\
                .aggregate({'title': 'count'})\
                .reset_index()\
                .rename({'topics': 'topics', 'title': 'numReviews'}, axis=1)
    
    topics_df['index'] = topics_df['topics'].apply(lambda x: int(x.split("_")[0]))
    topics_df = topics_df.sort_values(['index'])[1: nSignificant].drop(['index'], axis=1)
    topics_df['category'] = category

    return topics_df

def get_topics():
    attractionQuery = "SELECT title, review FROM AttractionReview"
    attractions = pd.read_sql(attractionQuery, MYDB)
    a_topics = analyse(attractions, n_topics=15, category='attractions')

    hotelQuery = "SELECT title, review FROM HotelReview"
    hotels = pd.read_sql(hotelQuery, MYDB)
    h_topics = analyse(hotels, n_topics=15, category='hotels')

    restaurantQuery = "SELECT title, review FROM RestaurantReview"
    restaurants = pd.read_sql(restaurantQuery, MYDB)
    r_topics = analyse(restaurants, n_topics=15, category='restaurants')

    final_df = pd.concat([a_topics, h_topics, r_topics])

    CURSOR.execute("""
        CREATE TABLE IF NOT EXISTS ReviewTopic (
            id INT AUTO_INCREMENT PRIMARY KEY,
            topics CHAR(225), 
            numReviews CHAR(225), 
            category CHAR(225)
        )
    """)

    insertion_query = """
    INSERT INTO ReviewTopic (topics, numReviews, category)
    VALUES (%s, %s, %s)
    """
    for row in final_df.itertuples(index=False):
        CURSOR.execute(insertion_query, row)
    MYDB.commit()