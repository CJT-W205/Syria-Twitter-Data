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
        self.db = conn.test

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

    track = [u'الدولة_الإسلامية#', u'الدولة_الاسلامية_في_العراق_و_الشام#', u'داعش#‎',
             u'جبهة_النصرة#' , '#ISIS', '#ISIL', '#Islamic State', '#Daaesh']

    sapi = streaming.Stream(auth, CustomStreamListener(api))
    try:
        sapi.filter(track=track)
    except KeyboardInterrupt:
        print "Twitter streaming interrupted"
