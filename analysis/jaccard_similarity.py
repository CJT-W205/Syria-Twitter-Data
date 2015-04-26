from itertools import combinations
from operator import itemgetter
from pyspark import SparkContext, SparkConf
import json
import sys

conf = SparkConf().setAppName('jaccard')
sc = SparkContext(conf=conf)

file = open('jaccard_sim_analysis.json')
results = json.load(file)
rdd = sc.parallelize(results)

def normalizeTags(user):
    count = 0.0
    for tag in user['tags']:
        if tag[1]>=3:
            count += tag[1]
    tags = []
    for tag in user['tags']:
        if tag[1]>=3:
            tags.append([tag[0], tag[1]/count])

    user['tags'] = tags
    return user

def reformat(user):
    return [(c[0], (user['_id'], c[1])) for c in user['tags']]

def samplePairs(hashtag, user_counts):
    return hashtag, user_counts

def findUserPairs(hashtag, user_list_with_count):
    return [((user1[0],user2[0]),0.5*min(user1[1],user2[1])) \
                    for user1,user2 in combinations(user_list_with_count,2)]

def keyOnFirstUser(user_pair, jaccard_sim_data):
    (user1_id, user2_id) = user_pair
    return user1_id, (user2_id, jaccard_sim_data)

def sortDistances(user, users_and_sims, k):
    users_and_sims.sort(key=itemgetter(1),reverse=True)
    return user, users_and_sims[:k]

def setNodeEdge(user):
    return {'source': user[0], 'target': user[1][0], 'weight': user[1][1]}


rdd_normalized = rdd.map(lambda x: normalizeTags(x))

lines = rdd_normalized.map(lambda x: reformat(x)).flatMap(lambda x: x)

hashtag_user_pairs = lines.groupByKey().map(
                        lambda x: samplePairs(x[0],x[1])).cache()

jaccard_sim = hashtag_user_pairs.filter(
                    lambda x: len(x[1]) > 1).map(
                    lambda x: findUserPairs(x[0],x[1])).flatMap(
                    lambda x: x).reduceByKey(lambda x, y: x+y).map(
                    lambda x: keyOnFirstUser(x[0],x[1])).filter(
                    lambda x: x[1][1] > 0.15).map(lambda x: setNodeEdge(x))


jaccard_sim.saveAsTextFile('jaccard_sim_knn')
