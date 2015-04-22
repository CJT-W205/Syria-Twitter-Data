#!/usr/bin/python
# -*- coding: utf-8 -*-

import pymongo
import numpy as np
import datetime
from collections import Counter
from operator import itemgetter

conn = pymongo.MongoClient()
db = conn.network
link = db.link_analysis
views = db.views

#pro = [u'دولة_الخلافة#',u'الدولة_الإسلامية#',u'ولاية_الانبار#','الدولة_الاسلامية_في_العراق_و_الشام#']
pro = [u'\u062f\u0648\u0644\u0629_\u0627\u0644\u062e\u0644\u0627\u0641\u0629',
       u'\u0627\u0644\u062f\u0648\u0644\u0629_\u0627\u0644\u0625\u0633\u0644\u0627\u0645\u064a\u0629',
       u'\u0648\u0644\u0627\u064a\u0629_\u0627\u0644\u0627\u0646\u0628\u0627\u0631',
       u'\u0627\u0644\u062f\u0648\u0644\u0629_\u0627\u0644\u0627\u0633\u0644\u0627\u0645\u064a\u0629_\u0641\u064a_\u0627\u0644\u0639\u0631\u0627\u0642_\u0648_\u0627\u0644\u0634\u0627\u0645']

#ati = [u'دواعش#',u'داعش#‎',u'داعشي#',]
ati = [u'\u062f\u0648\u0627\u0639\u0634',
       u'\u062f\u0627\u0639\u0634',
       u'\u062f\u0627\u0639\u0634\u200e',
       u'\u062f\u0627\u0639\u0634\u064a']

eng = ['isis', 'isil', 'islamicstate', 'islamic state', 'daesh', 'da3esh', 'daaesh']

vec = pro + ati + eng

def sort_k_tags(user_hashtags, K):
    user_hashtags.sort(key=itemgetter(1),reverse=True)
    return user_hashtags[:K]

def derive_affiliations(user_hashtags):
    count_p, count_n, count_e, count_v = 0, 0, 0, 1
    for tag in user_hashtags:
        if tag[0] in vec:
            count_v += tag[1]
            if tag[0] in pro:
                count_p += tag[1]
            elif tag[0] in ati:
                count_n += tag[1]
            elif tag[0] in eng:
                count_e += tag[1]

    count_v = float(count_v)

    if count_v > 0:
        if count_e/count_v > 0.2:
            return 'eng'
        elif count_p/count_v > 2*count_n/count_v:
            return 'pro'
        elif count_n/count_v > 3*count_p/count_v:
            return 'anti'
        else: return 'neutral'


isis_tweeters, isis_active_tweeters = [], []
user_hashtags = []

for user in link.find():
    hashtags = [tag[0].lower() for tag in user['tags']]
    counts = [tag[1] for tag in user['tags']]
    index =  [i for (i, x) in enumerate(vec) if x in set(hashtags)]

    if len(index) > 0:
        isis_tweeters.append(user['_id'])
        user_hashtags.append(zip(hashtags, counts))

        if sum(counts) > 500:
            isis_active_tweeters.append(user['_id'])

users = []

for i, user in enumerate(isis_tweeters):
    user_entities = db.user_entities.find_one({'_id': user})
    user_entities['k_tags'] =  sort_k_tags(user_hashtags[i] ,5)
    user_entities['isis'] = derive_affiliations(user_hashtags[i])
    user_entities['active'] = user in isis_active_tweeters
    users.append(user_entities)

import json
fname = 'sample_users.json'
out =  open(fname,"w")
out.write('[\n')

for user in users_sample_janak:
    out.write(json.dumps(user).encode('utf8'))
    out.write(',\n')


#    views.insert(user_entities)
