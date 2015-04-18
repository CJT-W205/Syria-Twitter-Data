#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
import numpy as np
import datetime
import csv

conn = pymongo.MongoClient()
db = conn.stream
tweets = db.tweets

pipeline = [
            {"$project": { "_id":1 , "entities.hashtags": 1, "user.id": 1, "user.favourites_count": 1, "lang": 1} },
            {"$match": {'lang': 'ar'}},
            {"$sort": { "_id": -1}},
            {"$limit": 25000},
            {"$sort": { "user.favourites_count": -1}},
           ]

query_tags = tweets.aggregate(pipeline)

##دولة_الخلاف
#الدولة_الإسلام
##داعش
##جبهة_النصرة
#ولاية_الانبا

hashtags = [u'\u062f\u0648\u0644\u0629_\u0627\u0644\u062e\u0644\u0627\u0641\u0629',
            u'\u0627\u0644\u062f\u0648\u0644\u0629_\u0627\u0644\u0625\u0633\u0644\u0627\u0645\u064a\u0629',
            u'\u062f\u0627\u0639\u0634']

users_to_crawl = [int(u['user']['id']) for u in query_tags['result']
                  if set(hashtags) & set([h['text'] for h in u['entities']['hashtags']])]

users_to_crawl = list(set(users_to_crawl))

myfile = open('tocrawl.txt', 'wb')
wr = csv.writer(myfile)
wr.writerow(users_to_crawl)
