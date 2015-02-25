import pymongo

db = pymongo.MongoClient().test
results = db.tweets.find()

for tweet in results:
    print tweet