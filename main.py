import base64
import sqlalchemy
import pandas as pd
import requests
from datetime import datetime as dt
import sqlite3
import re
from textblob import TextBlob
from sqlalchemy.types import Text, String, DateTime,Float

# Assign a client and secret keys
CLIENT_KEY = "Your clien key here"
CLIENT_SECRET = "Your client secret here"
DATABASE_LOCATION = "Your database name here"

# Encode a base64 key from the keys above
key_secret = "{}:{}".format(CLIENT_KEY, CLIENT_SECRET).encode("ascii")
b64_encoded_key = base64.b64encode(key_secret)
b64_encoded_key = b64_encoded_key.decode("ascii")

# Obtain a bearer token
base_url = "https://api.twitter.com/"
auth_url = "{}oauth2/token".format(base_url)

auth_headers = {
    "Authorization": "Basic {}".format(b64_encoded_key),
    "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8"
}

auth_data = {
    "grant_type": "client_credentials"
}

auth_resp = requests.post(auth_url, headers=auth_headers, data=auth_data)

print("Auth status: " + str(auth_resp.status_code))

access_token = auth_resp.json()['access_token']


# Validate Data Frame
def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No tweets were retrieved")
        return False
    # Primary Key check
    if pd.Series(df['tweet_id']).is_unique:
        pass
    else:
        raise Exception("Primary Key is duplicated")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    return True


# Making queries to the Twitter API

search_headers = {
    'Authorization': "Bearer {}".format(access_token)
}

search_params = {
    "q": "Donald Glover",
    "result_type": "recent",
    "tweet_mode": "extended",
    "count": 20
}

search_url = '{}1.1/search/tweets.json'.format(base_url)

search_response = requests.get(search_url, headers=search_headers, params=search_params)

# Check if response OK
print("Search status: " + str(search_response.status_code))

tweet_data = search_response.json()


# Praparing data for a Pandas DataFrame

tweet_ids = []
tweet_authors = []
tweet_fulltext = []
polarity = []
subjectivity = []
posted_at = []

for tweet in tweet_data["statuses"]:
    tweet_ids.append(tweet["id"])
    tweet_authors.append(tweet["user"]["screen_name"])
    full_text = tweet["full_text"]
    full_text_no_url = re.sub(r'http\S+', '', full_text) # Removes any links from the tweet text
    tweet_fulltext.append(full_text_no_url)
    tweet_blob = TextBlob(full_text_no_url)
    polarity.append(tweet_blob.sentiment.polarity)
    subjectivity.append(tweet_blob.sentiment.subjectivity)
    dtime = tweet["created_at"]
    dtime_new = dt.strptime(dtime, '%a %b %d %H:%M:%S +0000 %Y')
    posted_at.append(dtime_new)



tweet_dict = {
    "tweet_id": tweet_ids,
    "tweet_author": tweet_authors,
    "tweet_text": tweet_fulltext,
    "polarity": polarity,
    "subjectivity": subjectivity,
    "posted_at": posted_at
}

tweet_df = pd.DataFrame(tweet_dict, columns=["tweet_id", "tweet_author", "tweet_text","polarity","subjectivity", "posted_at"])
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(tweet_df)

# Validate data
if check_if_valid_data(tweet_df):
    print("Data is valid. Proceeding to loading stage.")
else:
    raise Exception("Data is not valid. Finished!")
#Load data in a database

engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect("tweet_sentiments.sqlite")
cursor = conn.cursor()

table_name = "tweet_sentiments"

try:
    tweet_df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        dtype={
            "tweet_id": String(20),
            "tweet_author": String(16),
            "tweet_text": Text,
            "polarity": Float,
            "subjectivity": Float,
            "posted_at": DateTime

        }
    )
except:
    raise Exception("Something went wrong while loading data in the database")

print("Data loaded successfuly")

