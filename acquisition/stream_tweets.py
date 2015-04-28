#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
from tweepy_utils import load_credentials, tweepy_auth, tweepy_api
from tweepy import streaming, StreamListener
import json
import argparse


class CustomStreamListener(StreamListener):
    def __init__(self, api, verbose=False):
        self.api = api
        self.count = 0
        self.verbose = verbose
        super(StreamListener, self).__init__()
        conn = pymongo.MongoClient()
        self.db = conn.primarytest

    def on_data(self, tweet):
        self.db.tweets.insert(json.loads(tweet))
        self.count += 1
        print self.count

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

    with open('candidates.json') as f:
        candidate_dict = json.load(f)

    track_terms = []
    for candidate in candidate_dict.keys():
        for term in candidate_dict[candidate]:
            track_terms.append(term)
    print track_terms

    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--verbose", required=False, type=bool, default=False, help="Set verbose output")
    args = vars(ap.parse_args())
    verbose = args['verbose']
    print 'Running streamer, verbose = %s' % verbose
    sapi = streaming.Stream(auth, CustomStreamListener(api, verbose=verbose))
    try:
        sapi.filter(track=track_terms)
    except KeyboardInterrupt:
        print "Twitter streaming interrupted"
