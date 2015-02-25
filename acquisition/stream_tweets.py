#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
from tweepy_utils import load_credentials, tweepy_auth, tweepy_api
from tweepy import streaming, StreamListener
import json


class CustomStreamListener(StreamListener):
    def __init__(self, api, verbose=False):
        self.api = api
        self.verbose = verbose
        super(StreamListener, self).__init__()
        conn = pymongo.MongoClient()
        self.db = conn.stream

    def on_data(self, tweet):
        self.db.tweets.insert(json.loads(tweet))

    def on_error(self, status_code):
        self.log(("error occurred, status code: ", status_code, ", but twitter streaming is continuing"))
        return True  # Don't kill the stream

    def on_timeout(self):
        self.log("timeout occurred, but twitter streaming is continuing")
        return True  # Don't kill the stream

    def log(self, message):
        if self.verbose:
            print message



if __name__ == '__main__':
    credentials = load_credentials()
    auth = tweepy_auth(credentials)
    api = tweepy_api(auth)

    track = [u'الدولة_الإسلامية', u'الدولة الاسلامية في العراق والشام', u'داعش‎',
             u'حملة_المليار_مسلم_لنصرة_الدولة_الإسلامية‎' ,u'جبهة_النصرة']

    sapi = streaming.Stream(auth, CustomStreamListener(api))
    try:
        sapi.filter(track=track)
    except KeyboardInterrupt:
        print "Twitter streaming interrupted"
