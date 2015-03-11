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
    def __init__(self, api, centre, max_depth=1, crawled_list=None,
                 verbose=False, cursor_count=100,
                 followers_of_followers_limit=500):

        self.api = api
        self.verbose = verbose
        self.crawled_list = crawled_list if crawled_list else []
        self.to_crawl_list = [centre]
        self.max_depth = max_depth

        self.cursor_count = cursor_count
        conn = pymongo.MongoClient()
        db = conn.network

        self.users = db.user_profiles
        self.followers_of_followers_limit = followers_of_followers_limit

    def crawl(self, parent=None, current_depth=0):
        self.log('current depth: %d, max depth: %d' % (current_depth, self.max_depth))
        self.log(('crawled list: ', ','.join([str(i) for i in self.crawled_list])))

        if current_depth == self.max_depth:
            self.log('out of depth')
            return self.crawled_list  # reached depth limit - exit recursive call

        user_id = self.to_crawl_list.pop(0)

        if user_id in self.crawled_list:
            self.log('Already been here.')
            return self.crawled_list
        else:
            self.crawled_list.append(user_id)

        user = self.get_user_from_mongo(user_id)

        if not user:
            user = self.get_user_from_api(user_id)

            if not user:
                return self.crawled_list  # Couldn't get user so exit recursive call

            user._parents.extend(parent)
            self.write_user_to_mongo(user)

        screen_name = self.encode_str(user['screen_name'])
        relationships_ids = self.get_relationships_from_mongo(user_id)

        self.to_crawl_list.extend(relationships_ids)

        cd = current_depth

    #    if len(user._parent) < self.max_depth:
        while self.to_crawl_list and len(user._parent) < self.max_depth:
        #    for fid in relationships_ids[:self.followers_of_followers_limit]:
            self.crawled_list = self.crawl(fid, current_depth=cd+1, parent=user.id)  # RECURSIVE CALL

        if cd+1 < self.max_depth and len(relationships_ids) > self.followers_of_followers_limit:
            self.log('Not all followers retrieved for %s - limit reached.' % screen_name)

        return self.crawled_list

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
                        'followers_ids': user.followers_ids(),
                        'friends_ids': user._api.friends_ids(user_id=user.id),
                        'location': user.location,
                        'time_zone': user.time_zone,
                        'created_at': datetime.datetime.strftime(user.created_at, '%Y-%h-%m %H:%M')
                        'parent': user._parents}

            self.log('%s has %s followers' % (userDict['screen_name'], userDict['followers_count']))

        except tweepy.TweepError, error:

                    if str(error) == 'Not authorized.':
                        self.log('Can''t access user data - not authorized.')

                    if str(error) == 'User has been suspended.':
                        self.log('User suspended.')

                    self.log(error[0][0])

        return userDict


    def get_relationships_from_mongo(self, user_id):
        user = self.users.find_one({'_id': user_id})

        ratio = 0
        if user['following_count']>0:
            ratio = user['followers_count']/user['following_count']

        self.log('follower to friend ratio is %d' % (ratio))
        n = min(500 , max(10, ratio))

        followers = user.get('followers_ids', None)
        friends = user.get('friends_ids', None)
        relationships = random.sample(friends, min(n, user['following_count']))
        return relationships


    def log(self, message):
        if self.verbose:
            print message

    @staticmethod
    def encode_str(x):
        return x.encode('utf-8', errors='ignore')


if __name__ == '__main__':
    credentials = load_credentials(from_file=True)
    auth = tweepy_auth(credentials)
    api = tweepy_api(auth)

    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--user_id", required=True, type=int, help="User ID of twitter user")
    ap.add_argument("-d", "--depth", required=True, type=int, help="How far to follow user network")
    args = vars(ap.parse_args())

    user_id = args['user_id']
    depth = int(args['depth'])

    if depth < 1 or depth > 3:
        print 'Depth value %d is not valid. Valid range is 1-4.' % depth
        sys.exit('Invalid depth argument.')

    print 'Max Depth: %d' % depth

    crawler = FriendCrawler(api, max_depth=depth, verbose=True)
    print crawler.crawl(user_id)
