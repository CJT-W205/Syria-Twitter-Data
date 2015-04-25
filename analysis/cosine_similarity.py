from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import Normalizer
from pyspark import SparkContext, SparkConf
import json
import sys

#conf = SparkConf().setAppName('cosSim')
#sc = SparkContext(conf=conf)

file = open('retweet_sim_analysis.json')
results = json.load(file)
rdd = sc.parallelize(results[:10000])

user_dict = {}

def user_index(user_dict, user):
  	if user not in user_dict:
  	 	 user_index = len(user_dict)
  		 user_dict[user] = user_index
  		 return user_index
  	else:
  		 return user_dict[user]


def mapDocs(user):
    doc = []
    for retweet in user['retweets']:
        doc.extend([retweet[0]]*retweet[1])
    return  doc

normalizer1 = Normalizer()

def cosineSimilarity(tupl):
    x,y=tupl
    return (x[0], y[0], x[1].dot(y[1]))


def setNodeEdge(user):
    return {'source': user[0], 'target': user[1], 'weight': user[2]}

rddDocs = rdd.map(lambda x: mapDocs(x))
rddLabels = rdd.map(lambda x: x['_id'])

hashingTF = HashingTF()
tf = hashingTF.transform(rddDocs)
tf.cache()
idf = IDF().fit(tf)
tfIdf = idf.transform(tf)
tfIdfNormal = normalizer1.transform(tfIdf)
userTfIdf = rddLabels.zip(tfIdfNormal)
userUserVec = userTfIdf.cartesian(userTfIdf).filter(lambda x: x[0][0]>x[1][0])
cosine_sim = userUserVec.map(lambda x: cosineSimilarity(x)).filter(
                      lambda x: x[2] > 0.0).map(
                      lambda x: setNodeEdge(x))


cosine_sim.saveAsTextFile('cosine_sim_knn')
