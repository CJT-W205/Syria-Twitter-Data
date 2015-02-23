 #!/usr/bin/python
 # -*- coding: utf-8 -*-

import tweepy
import time
import datetime
import os
import sys
import json
import argparse
import pymongo

from tweet_auth import *


FOLLOWING_DIR = 'following'
MAX_FRIENDS = 10000
FRIENDS_OF_FRIENDS_LIMIT = 10000

enc = lambda x: x.encode('utf-8', errors='ignore')

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

conn=pymongo.MongoClient()
db = conn.network
users = db.user_profiles
followers = db.user_followers


def get_follower_ids(centre, max_depth=1, current_depth=0, crawled_list=None):
    if crawled_list is None:
        crawled_list = []

    print 'current depth: %d, max depth: %d' % (current_depth, max_depth)
    print 'taboo list: ', ','.join([ str(i) for i in crawled_list ])

    if current_depth == max_depth:
        print 'out of depth'
        return crawled_list

    if centre in crawled_list:
        print 'Already been here.'
        return crawled_list
    else:
        crawled_list.append(centre)

    try:
        if not users.find_one({'_id': centre}):
            print 'Retrieving user details for twitter id %s' % str(centre)
            while True:
                try:
                    user = api.get_user(centre)

                    d = {'_id': user.id,
                         'name': user.name,
                         'screen_name': user.screen_name,
                         'friends_count': user.friends_count,
                         'followers_count': user.followers_count,
                         'followers_ids': user.followers_ids(),
                         'location': user.location,
                         'time_zone': user.time_zone,
                         'created_at': datetime.datetime.strftime(user.created_at,'%Y-%h-%m %H:%M')}

                    users.insert(d)

                    user = d
                    break
                except tweepy.TweepError, error:
                    print type(error)

                    if str(error) == 'Not authorized.':
                        print 'Can''t access user data - not authorized.'
                        return crawled_list

                    if str(error) == 'User has been suspended.':
                        print 'User suspended.'
                        return crawled_list

                    errorObj = error[0][0]

                    print errorObj
                    return crawled_list
        else:
            user = users.find_one({'_id': centre})

        screen_name = enc(user['screen_name'])
        fname = followers.find_one({'_id': centre})
        friendids  = []

        if not fname:
            print 'No data for screen name "%s"' % screen_name

            d = {'_id': centre,
                 'screen_name': screen_name}

            followers.insert(d)

            params = (enc(user['name']), screen_name)
            print 'Retrieving friends for user "%s" (%s)' % params

            c = tweepy.Cursor(api.friends, id=user['_id'], count=100).items()

            friend_count = 0
            while True:
                try:
                    friend = c.next()
                    friendids.append(friend.id)

                    f = {'friend_id': friend.id,
                              'friend_sname': enc(friend.screen_name),
                              'friend_name': enc(friend.name)}

                    followers.update({"_id":centre}, {"$push":{'friends':f}})

                    friend_count += 1
                    if friend_count >= MAX_FRIENDS:
                        print 'Reached max no. of friends for "%s".' % friend.screen_name
                        break

                except StopIteration:
                    break


        else:
            friendids = [fr['friend_id'] for fr in followers.find_one({'_id':centre})['friends']]

        print 'Found %d friends for %s' % (len(friendids), screen_name)

        cd = current_depth
        if cd+1 < max_depth:
            for fid in friendids[:FRIENDS_OF_FRIENDS_LIMIT]:
                crawled_list = get_follower_ids(fid, max_depth=max_depth,
                    current_depth=cd+1, crawled_list=crawled_list)

        if cd+1 < max_depth and len(friendids) > FRIENDS_OF_FRIENDS_LIMIT:
            print 'Not all friends retrieved for %s.' % screen_name

    except Exception, error:
        print 'Error retrieving followers for user id: ', centre
        print error

        sys.exit(1)

    return crawled_list

if __name__ == '__main__':
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

    print get_follower_ids(user_id, max_depth=depth)
