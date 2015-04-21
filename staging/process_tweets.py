import json
import pymongo
import datetime

def insert_tweet_into_mongo(tweet, collection):
    date = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y') \
           + datetime.timedelta(hours=1)

    htags = []

    for entry in tweet['entities']['hashtags']:
        tag = entry['text']
        htags.append(tag)

    tweet_doc = {
                  '_id': tweet['id'],
                  'user_id': tweet['user']['id'],
                  'user_name': tweet['user']['screen_name'],
                  'text': tweet['text'],
                  'date': datetime.datetime.strftime(date, '%y-%m-%d %H:00'),
                  'tags': htags,
                  'location': tweet['user']['location'],
                 }

    collection.insert(tweet_doc)


conn=pymongo.MongoClient()
db = conn.search
db.drop_collection('tweets')
tweets_in = db.tweets

dbp = conn.stage
tweets_out = dbp.tweets

for tweet in tweets_in.find():
    tweet = tweet._json

    if not tweets_out.find_one({'_id': tweet['id']}):
        insert_tweet_into_mongo(tweet, tweets_out)
