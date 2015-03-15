#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import datetime
import sys
import argparse
import pymongo
import random

from tweepy_utils import *


class FriendCrawler(object):
    def __init__(self, api, max_depth=1, verbose=False, cursor_count=100, followers_of_followers_limit=200):
        self.api = api
        self.verbose = verbose
        self.crawled = []
        self.max_depth = max_depth
        self.cursor_count = cursor_count
        conn = pymongo.MongoClient()
        db = conn.network
        self.users = db.user_profiles
        self.followers_of_followers_limit = followers_of_followers_limit
        self.tocrawl = []

    def crawl(self, seed, max_users=100):
        self.tocrawl = [seed]
        depth = [0]
        graph = {}

        while self.tocrawl and len(self.crawled) < max_users:
            user_id = self.tocrawl.pop(0)
            d = depth.pop(0)

            if user_id not in self.crawled:
                user = self.get_user_from_mongo(user_id)

                if not user:
                    user = self.get_user_from_api(user_id)

                    if not user:
                        self.crawled.append(user_id)
                        continue  #

                    self.write_user_to_mongo(user)

                relationships_ids = self.get_relationships_from_mongo(user_id)
                graph[user_id] = relationships_ids

                if d < self.max_depth:
                    cnt = self.union(relationships_ids)

                    for i in range(cnt):
                        depth.append(d+1)

                self.crawled.append(user_id)

            self.log('crawled %s and crawled list is %d users' % (user['screen_name'],len(self.crawled)))
            self.log('to crawl list has %s users at depth %d' % (len(self.tocrawl), d))

        return graph

    def get_user_from_mongo(self, user_id):
        user = self.users.find_one({'_id': user_id})
        return user

    def write_user_to_mongo(self, user):
        self.users.insert(user)

    def get_user_from_api(self, user_id):
        userDict = None

        try:
            user = self.api.get_user(user_id)

            if not getattr(user, 'screen_name', None):
                return None

            userDict = {'_id': user.id,
                        'name': user.name,
                        'screen_name': user.screen_name,
                        'following_count': user.friends_count,
                        'followers_count': user.followers_count,
                        'following_ids': user._api.friends_ids(user_id=user.id),
                        'location': user.location,
                        'time_zone': user.time_zone,
                        'created_at': datetime.datetime.strftime(user.created_at, '%Y-%h-%m %H:%M')
                        }

            self.log('%s has %s friends' % (userDict['screen_name'], userDict['following_count']))

        except tweepy.TweepError, error:

                    if str(error) == 'Not authorized.':
                        self.log('Can''t access user data - not authorized.')

                    if str(error) == 'User has been suspended.':
                        self.log('User suspended.')

                    self.log(error[0][0])

        return userDict

    def union(self, relationship_ids):
        cnt = 0
        for e in relationship_ids:
            if e not in self.tocrawl:
                self.tocrawl.append(e)
                cnt += 1

        return cnt

    def get_relationships_from_mongo(self, user_id):
        user = self.users.find_one({'_id': user_id})
        n = min(400, int(user['following_count']**0.9))
        friends = user.get('following_ids', None)
        relationships = random.sample(friends, n)

        return relationships

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

    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--user_id", required=True, type=int, help="User ID of twitter user")
    ap.add_argument("-d", "--depth", required=True, type=int, help="How far to follow user network")
    ap.add_argument("-m", "--max_users", required=True, type=int, help="Max # users to crawl")

    args = vars(ap.parse_args())

    user_id = args['user_id']
    depth = int(args['depth'])
    max_users = int(args['max_users'])

    if depth < 1 or depth > 3:
        print 'Depth value %d is not valid. Valid range is 1-4.' % depth
        sys.exit('Invalid depth argument.')

    print 'Max Depth: %d' % depth

    crawler = FriendCrawler(api, max_depth=depth, verbose=True)
    graph = crawler.crawl(user_id, max_users)

    with open('twitter_user_network.txt', 'w') as outfile:
        json.dump(graph, outfile)
