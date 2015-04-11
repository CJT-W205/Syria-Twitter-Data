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

    doc = {

              '_id':user_id,
              'tags': count_list(tags),
              'mentions': count_list(mentions),
              'retweets': count_list(retweets),
              'replies': count_list(replies)

          }

    link.insert(doc)


def insert_user_to_mongo(user_doc):
    entities.insert(user_doc)

def insert_text_to_mongo(user_id, text):
    doc = {

              '_id':user_id,
              'text': text

          }

    content.insert(doc)


def process_tweet(tweet):
    doc = {

                'text': tweet['text'],
                'place': tweet['place'],
                'lang': tweet['lang'],
                'geo': tweet['geo'],
                'created_at': datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y'),

          }

    return doc

def process_user(user):
    doc = {

                '_id': user['id'],
                'screen_name': user['screen_name'],
                'name': user['name'],
                'following_count': user['friends_count'],
                'followers_count': user['followers_count'],
                'location': user['location'],
                'time_zone': user['time_zone'],
                'created_at': datetime.datetime.strptime(user['created_at'], '%a %b %d %H:%M:%S +0000 %Y'),
                'url': user['url'],
                'desc': user['description']

            }

    return doc


def count_list(l1):
    counts = Counter(l1)
    return zip(counts.keys(), counts.values())


for tl in timelines.find():
    tags, mentions, retweets, text, replies= [], [], [], [], []

    for tweet in tl['timeline']:

        doc = process_tweet(tweet)
        text.append(doc)

        for entry in tweet['entities']['hashtags']:
            hashtag = entry['text']
            tags.append(hashtag)

        for entry in tweet['entities']['user_mentions']:
            mentions.append(entry['id'])

        replies.append(tweet['in_reply_to_user_id'])

        if tweet.has_key('retweeted_status'):
            retweets.append(tweet['retweeted_status']['user']['id'])

    if tweet:
        user_doc = process_user(tweet['user'])

        insert_attributes_to_mongo(tl['_id'], tags, mentions, retweets, replies)

        insert_user_to_mongo(user_doc)

        insert_text_to_mongo(tl['_id'], text)




#pro = [u'دولة_الخلافة#',u'الدولة_الإسلامية#',\
#       u'ولاية_الانبار#','الدولة_الاسلامية_في_العراق_و_الشام#']

#ati = [u'دواعش#',u'داعش#‎',u'داعشي#',]
