from itertools import combinations
from operator import itemgetter
from pyspark import SparkContext, SparkConf
import numpy as np
import pymongo
import sys

#conf = SparkConf().setAppName('jaccard.py').setMaster('local')

#sc = SparkContext(conf=conf)

conn = pymongo.MongoClient()#(host='169.53.140.164', port='27017;)
link = conn.network.link_analysis

results =[]
import json
fname =open('sample_users_hashtags.json')
user_sample = json.load(fname)
#for user_id in user_sample:
#    user = link.find({"_id": user_id}):
#    results.append({"_id": user['_id'], "tags": user['tags']})

results = user_sample['sample'][:1000]
results = sc.parallelize(results)

def normalizeTags(user):
    count = 0.0
    for tag in user['tags']:
        count += tag[1]

    for tag in user['tags']:
        tag[1] /= count
    return user

def reformat(user):
    return [(c[0], (user['_id'], c[1])) for c in user['tags']]

def samplePairs(hashtag, user_counts):
    return hashtag, user_counts

def findUserPairs(hashtag, user_list_with_count):
    return [((user1[0],user2[0]),(user1[1],user2[1])) \
                    for user1,user2 in combinations(user_list_with_count,2)]

def jaccardSim(user_pair, tag_pairs):
    jacc = 0.0
    for tp in tag_pairs:
        jacc += min(tp[0], tp[1])
    return user_pair, jacc*0.5

def keyOnFirstUser(user_pair, jaccard_sim_data):
    (user1_id, user2_id) = user_pair
    return user1_id, (user2_id, jaccard_sim_data)

def sortDistances(user, users_and_sims, k):
    users_and_sims.sort(key=itemgetter(1),reverse=True)
    return user, users_and_sims[:k]

def setNodeEdge(user):
    return [{'src': user[0], 'dest': v[0]} for v in user[1]]



results = results.map(lambda x: normalizeTags(x))

lines = results.map(lambda x: reformat(x)).flatMap(lambda x: x)

hashtag_user_pairs = lines.groupByKey().map(
                        lambda x: samplePairs(x[0],x[1])).cache()

pairwise_users = hashtag_user_pairs.filter(
                    lambda x: len(x[1]) > 1).map(
                    lambda x: findUserPairs(x[0],x[1])).flatMap(
                    lambda x: x).groupByKey()

jaccard_sim = pairwise_users.map(lambda x: jaccardSim(x[0],x[1])).map(
                                 lambda x: keyOnFirstUser(x[0],x[1])).groupByKey().map(
                                 lambda x: sortDistances(x[0], list(x[1]), 10 ) )
#                                 lambda x: {"src": x[0], "dest": x[1][0], "sim": x[1][1]})

jaccard_sim_knn = jaccard_sim.map(setNodeEdge).flatMap(lambda x: x)

jaccard_sim_knn.saveAsTextFile('jaccard_sim_knn.json')
