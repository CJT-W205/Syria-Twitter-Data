#!/usr/bin/python
# -*- coding: utf-8 -*-

import tweepy
import datetime
import sys
import argparse
import pymongo

from tweepy_utils import *


FOLLOWING_DIR = 'following'
MAX_FRIENDS = 10000
FRIENDS_OF_FRIENDS_LIMIT = 10000


class User:
    def __init__(self, in_dict):
        if in_dict:
            self.__dict__.update(in_dict)


class FriendCrawler(object):
    def __init__(self, api, max_depth=1, crawled_list=None, verbose=False, cursor_count=100):
        self.api = api
        self.verbose = verbose
        self.crawled_list = crawled_list if crawled_list else []
        self.max_depth = max_depth
        self.cursor_count = cursor_count
        conn = pymongo.MongoClient()
        db = conn.network
        self.users = db.user_profiles
        self.followers = db.user_followers

    def crawl(self, center, current_depth=0):
        self.log('current depth: %d, max depth: %d' % (current_depth, self.max_depth))
        self.log(('crawled list: ', ','.join([str(i) for i in self.crawled_list])))

        if current_depth == self.max_depth:
            self.log('out of depth')
            return self.crawled_list
        if center in self.crawled_list:
            self.log('Already been here.')
        else:
            self.crawled_list.append(center)

        user = self.get_user_from_mongo(center)
        if not user:
            user = self.get_user_from_api(center)
            if not user:
                return self.crawled_list
            self.write_user_to_mongo(user)

        screen_name = self.encode_str(user.screen_name)
        friends = self.get_friends_from_mongo(center)
        if not friends:
            self.log('No data for screen name "%s"' % screen_name)
            friend_ids = self.get_friends_from_api(center, user)
        else:
            friends = friends.get('friends', None)
            if not friends:
                self.log('Error retrieving friends for screen name "%s"' % screen_name)
                return self.crawled_list  # @TODO SOMETHING NOT WORKING HERE
            friend_ids = [friend['friend_id'] for friend in friends]
        self.log('Found %d friends for %s' % (len(friend_ids), screen_name))
        cd = current_depth
        if cd+1 < self.max_depth:
            for fid in friend_ids[:FRIENDS_OF_FRIENDS_LIMIT]:
                self.crawled_list = self.crawl(fid, current_depth=cd+1)  # recursive call

        if cd+1 < self.max_depth and len(friend_ids) > FRIENDS_OF_FRIENDS_LIMIT:
            self.log('Not all friends retrieved for %s.' % screen_name)

        return self.crawled_list

    def get_user_from_mongo(self, center):
        user = self.users.find_one({'_id': center})
        user = User(user)
        if not getattr(user,'screen_name', None):
            return None
        return user

    def write_user_to_mongo(self, user):
        d = {'_id': user.id,
             'name': user.name,
             'screen_name': user.screen_name,
             'friends_count': user.friends_count,
             'followers_count': user.followers_count,
             'followers_ids': user.followers_ids(),
             'location': user.location,
             'time_zone': user.time_zone,
             'created_at': datetime.datetime.strftime(user.created_at, '%Y-%h-%m %H:%M')}
        self.users.insert(d)

    def get_user_from_api(self, center):
        user = None
        try:
            user = self.api.get_user(center)
            if not getattr(user,'screen_name', None):
                return None
        except tweepy.TweepError, error:
                    if str(error) == 'Not authorized.':
                        self.log('Can''t access user data - not authorized.')
                    if str(error) == 'User has been suspended.':
                        self.log('User suspended.')
                    self.log(error[0][0])
        return user

    def get_friends_from_mongo(self, center):
        friends = self.followers.find_one({'_id': center})
        return friends

    def get_friends_from_api(self, center, user):
        friend_ids = []
        self.write_friends_to_mongo(center, user)
        self.log('Retrieving friends for user "%s"' % user.screen_name)
        c = tweepy.Cursor(api.friends, id=user.id, count=self.cursor_count).items()

        friend_count = 0
        while True:
            try:
                friend = c.next()
                friend_ids.append(friend.id)
                self.update_friend_in_mongo(friend, center)
                friend_count += 1
                if friend_count >= MAX_FRIENDS:
                    self.log('Reached max no. of friends for "%s".' % friend.screen_name)
                    break
            except StopIteration:
                break
        return friend_ids

    def write_friends_to_mongo(self, center, user):
        d = {'_id': center,
             'screen_name': user.screen_name}
        self.followers.insert(d)

    def update_friend_in_mongo(self, friend, center):
        f = {'friend_id': friend.id,
             'friend_sname': self.encode_str(friend.screen_name),
             'friend_name': self.encode_str(friend.name)}
        self.followers.update({"_id": center}, {"$push": {'friends': f}})

    def log(self, message):
        if self.verbose:
            print message

    @staticmethod
    def encode_str(x):
        return x.encode('utf-8', errors='ignore')


if __name__ == '__main__':
    credentials = load_credentials()
    auth = tweepy_auth(credentials)
    api = tweepy_api(auth)

    ap = argparse.ArgumentParser()
    ap.add_argument("-u", "--user_id", required=True, type=int, help="User ID of twitter user")
    ap.add_argument("-d", "--depth", required=True, type=int, help="How far to follow user network")
    args = vars(ap.parse_args())

    user_id = args['user_id']
    depth = int(args['depth'])

    if depth < 1 or depth > 4:
        print 'Depth value %d is not valid. Valid range is 1-4.' % depth
        sys.exit('Invalid depth argument.')

    print 'Max Depth: %d' % depth

    crawler = FriendCrawler(api, max_depth=depth, verbose=True)
    print crawler.crawl(user_id)
