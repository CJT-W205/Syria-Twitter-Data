import pymongo

db = pymongo.MongoClient().stream
results = db.tweets.find()

for tweet in results:
    print tweet