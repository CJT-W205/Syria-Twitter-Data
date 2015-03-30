#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import datetime
import sys
import argparse
import pymongo
import random

from tweepy_utils import *

class timelineCrawler(object):
    def __init__(self, api, verbose=False, cursor_count=100):
        self.api = api
        self.verbose = verbose
        self.count = cursor_count
        conn = pymongo.MongoClient()
        db = conn.network
        self.users = db.user_profiles
        self.timelines = db.timelines

    def crawl(self):
        user_list = []

        for user in self.users.find():
            user_list.append(user)

        user_list = sorted(user_list, key=lambda x:x['followers_count'], reverse=True)

        for user in user_list:

            if not self.get_timeline_from_mongo(user['_id']):
                user_timeline = self.get_timeline_from_api(user['_id'])

                if not user_timeline:
                    continue

                self.write_timeline_to_mongo(user_timeline)

        print 'Done'


    def get_timeline_from_mongo(self, user_id):
        user = self.timelines.find_one({'_id': user_id})
        return user

    def write_timeline_to_mongo(self, user):
        self.timelines.insert(user)

    def get_timeline_from_api(self, user_id):
        userTimeline = None
        tweetsTimeline = []

        try:

            for status in tweepy.Cursor(api.user_timeline,
                                        id=user_id,
                                        count=100
                                        ).items(self.count):
                print 'got status'
                tweetsTimeline.append(status._json)

            userTimeline = {'_id': user_id,
                            'timeline': tweetsTimeline,
                            'suspended': False,
                            }

        except tweepy.TweepError, error:

                    if str(error) == 'Not authorized.':
                        self.log('Can''t access user data - not authorized.')

                    if str(error) == 'User has been suspended.':
                        self.log('User suspended.')
                        userTimeline = {'_id': user.id,
                                        'timeline': [],
                                        'suspended': True,
                                        }

                    self.log(error[0][0])

        return userTimeline

    def log(self, message):
        if self.verbose:
            print message

    @staticmethod
    def encode_str(x):
        return x.encode('utf-8', errors='ignore')


if __name__ == '__main__':
    credentials = load_credentials(from_file=True, path='charlie_credentials.json')
    auth = tweepy_auth(credentials)
    api = tweepy_api(auth)
    timelines = timelineCrawler(api)
    timelines.crawl()
