 #!/usr/bin/python
 # -*- coding: utf-8 -*-

import os
import json
import pymongo
import tweepy

consumer_key = os.environ['TW_CONS_KEY']
consumer_secret = os.environ['TW_CONS_SECRET']

access_key = os.environ['TW_AT_KEY']
access_secret = os.environ['TW_AT_SECRET']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)


class CustomStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        super(tweepy.StreamListener, self).__init__()

        self.db = pymongo.MongoClient().test

    def on_data(self, tweet):
        self.db.tweets.insert(json.loads(tweet))

    def on_error(self, status_code):
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream


sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
sapi.filter(track=[u'الدولة_الإسلامية'])