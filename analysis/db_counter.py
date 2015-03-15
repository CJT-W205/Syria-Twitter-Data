import pymongo

stream = pymongo.MongoClient().stream
network = pymongo.MongoClient().network

print '%s tweets stored' % stream.tweets.count()
print '%s user profiles stored' % network.user_profiles.count()