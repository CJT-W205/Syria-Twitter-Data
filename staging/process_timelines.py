#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
import numpy as np
import datetime
from collections import Counter

conn = pymongo.MongoClient()
db = conn.network
timelines = db.timelines
link = db.link_analysis
entities = db.user_entities
content = db.content_analysis

def insert_attributes_to_mongo(user_id, tags, mentions, retweets, replies):
#    if not entities.find_one({'_id': user_doc['_id']}):
    doc = {

              '_id':user_id,
              'tags': count_list(tags),
              'mentions': count_list(mentions),
              'retweets': count_list(retweets),
              'replies': count_list(replies)

      }


    link.insert(doc)


def insert_user_to_mongo(user_doc):
#    if not entities.find_one({'_id': user_doc['_id']}):
    entities.insert(user_doc)

def insert_text_to_mongo(user_id, text):

    doc = {

              '_id':user_id,
              'text': text

          }

#    if not content.find_one({'_id': doc['_id']}):
    content.insert(doc)


def process_tweet(tweet):
    doc = {

                'text': tweet['text'],
                'place': tweet['place'],
                'lang': tweet['lang'],
                'geo': tweet['geo'],
                'created_at': datetime.datetime.strptime(tweet['created_at'],\
                                               '%a %b %d %H:%M:%S +0000 %Y'),

          }

    return doc

def process_user(user):
    doc = {

                '_id': user['id'],
                'screen_name': user['screen_name'],
                'name': user['name'],
                'following_count': user['friends_count'],
                'followers_count': user['followers_count'],
                'favourites_count': user['favourites_count'],
                'location': user['location'],
                'time_zone': user['time_zone'],
                'created_at': datetime.datetime.strptime(user['created_at'],\
                                              '%a %b %d %H:%M:%S +0000 %Y'),
                'url': user['url'],
                'desc': user['description']


            }

    return doc


def count_list(l1):
    counts = Counter(l1)
    return zip(counts.keys(), counts.values())

n = timelines.count()
c = 0

for tl in timelines.find():
    c+=1
    if c % 100 == 0:
        print 'there are %d processed timelines and %d left' %(c, n-c)

    tweet, user_doc = None, None
    tags, mentions, retweets, text, replies= [], [], [], [], []

    if len(tl['timeline'])>0:
        tweet = tl['timeline'][0]

        if not link.find_one({'_id': tweet['user']['id']}):
            print tweet['user']['screen_name']
            
            for tweet in tl['timeline']:

                doc = process_tweet(tweet)
                text.append(doc)

                for entry in tweet['entities']['hashtags']:
                    hashtag = entry['text']
                    tags.append(hashtag)

                for entry in tweet['entities']['user_mentions']:
                    mentions.append(entry['id'])

                if tweet['in_reply_to_user_id']:
                    replies.append(tweet['in_reply_to_user_id'])

                if tweet.has_key('retweeted_status'):
                    retweets.append(tweet['retweeted_status']['user']['id'])

            user_doc = process_user(tweet['user'])

            insert_attributes_to_mongo(tl['_id'], tags, mentions, retweets, replies)

            insert_user_to_mongo(user_doc)

            insert_text_to_mongo(tl['_id'], text)
