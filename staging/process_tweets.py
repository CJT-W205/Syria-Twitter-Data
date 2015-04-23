#! /usr/bin/env python
# -*- coding: utf-8 -*-


import itertools
import pymongo
import datetime


def transform(tweet):
    tweet['created_at'] = datetime.datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
    entities = tweet.pop('entities', None)
    tweet['hashtags'] = [hashtag['text'] for hashtag in entities['hashtags']]
    return tweet


def read(collection):
    return collection.find({},
        {
            '_id': 0,
            'id': 1,
            'user.id': 1,
            'user.screen_name': 1,
            'user.location': 1,
            'geo': 1,
            'created_at': 1,
            'text': 1,
            'entities.hashtags.text': 1
        })


client = pymongo.MongoClient()
try:
    client['analysis'].drop_collection('tweets')

    analysis = client['analysis']['tweets']
    search = client['search']['tweets']
    stream = client['stream']['tweets']

    analysis.create_index([("id", pymongo.ASCENDING)], unique=True)

    tweets = itertools.chain(read(search), read(stream))
    try:
        client['analysis']['tweets'].insert(map(transform, tweets), continue_on_error=True)
    except pymongo.errors.DuplicateKeyError:
        pass

finally:
    client.close()



